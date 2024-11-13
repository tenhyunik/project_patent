import os
import json
import time
import shutil
import aiohttp
import aiomysql
import asyncio
import logging
import psutil
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Tuple, Dict
from collections import deque
from time import time
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Summary, Gauge
from urllib3.util.ssl_ import create_urllib3_context

class TrademarkMetrics:
    def __init__(self):
        self.api_requests = Counter(
            'trademark_api_requests_total', 
            'Total number of API requests made'
        )
        self.api_errors = Counter(
            'trademark_api_errors_total', 
            'Total number of API request errors'
        )
        self.xml_parse_errors = Counter(
            'trademark_xml_parse_errors_total', 
            'Total number of XML parsing errors'
        )
        self.successful_downloads = Counter(
            'trademark_successful_downloads_total', 
            'Total number of successfully downloaded trademarks'
        )
        self.api_request_duration = Summary(
            'trademark_api_request_duration_seconds', 
            'Time spent in API requests'
        )
        self.api_total_duration = Summary(
            'trademark_api_total_duration_seconds',
            'Total time from start to finish of API processing'
        )
        # API 속도 관련 메트릭
        self.api_request_rate = Gauge(
            'trademark_api_request_rate',
            'Current API request rate per second'
        )
        self.api_request_window_rate = Gauge(
            'trademark_api_request_window_rate',
            'API request rate over sliding window',
            ['window_seconds']
        )
        # 메모리 관련 메트릭
        self.memory_usage = Gauge(
            'trademark_memory_usage_bytes',
            'Current memory usage of the process'
        )
        self.memory_percent = Gauge(
            'trademark_memory_usage_percent',
            'Current memory usage as percentage of system memory'
        )
        self.virtual_memory_usage = Gauge(
            'trademark_virtual_memory_usage_bytes',
            'Current virtual memory usage of the process'
        )
        self.system_memory_usage = Gauge(
            'trademark_system_memory_usage_percent',
            'Current system memory usage percentage'
        )

class MemoryMonitor:
    def __init__(self, metrics: TrademarkMetrics, logger: logging.Logger, 
                 check_interval: int = 60, warning_threshold: float = 80.0):
        self.metrics = metrics
        self.logger = logger
        self.check_interval = check_interval
        self.warning_threshold = warning_threshold
        self.process = psutil.Process(os.getpid())
        self._running = False
        self._task = None

    def _get_memory_info(self):
        try:
            process_memory = self.process.memory_info()
            memory_percent = self.process.memory_percent()
            system_memory = psutil.virtual_memory()
            
            return {
                'rss': process_memory.rss,
                'vms': process_memory.vms,
                'process_percent': memory_percent,
                'system_percent': system_memory.percent,
                'system_available': system_memory.available
            }
        except Exception as e:
            self.logger.error(f"메모리 정보 수집 중 오류 발생: {str(e)}")
            return None

    def _update_metrics(self, memory_info: dict):
        if memory_info:
            self.metrics.memory_usage.set(memory_info['rss'])
            self.metrics.virtual_memory_usage.set(memory_info['vms'])
            self.metrics.memory_percent.set(memory_info['process_percent'])
            self.metrics.system_memory_usage.set(memory_info['system_percent'])

    async def _monitor(self):
        last_warning_time = 0
        warning_interval = 300  # 5분

        while self._running:
            try:
                memory_info = self._get_memory_info()
                if memory_info:
                    self._update_metrics(memory_info)
                    
                    current_time = time()
                    memory_usage_gb = memory_info['rss'] / (1024 * 1024 * 1024)
                    system_memory_gb = memory_info['system_available'] / (1024 * 1024 * 1024)

                    self.logger.info(
                        f"메모리 사용량 - "
                        f"프로세스: {memory_usage_gb:.2f}GB ({memory_info['process_percent']:.1f}%), "
                        f"시스템: {memory_info['system_percent']:.1f}% "
                        f"(가용: {system_memory_gb:.2f}GB)"
                    )

                    if (memory_info['process_percent'] > self.warning_threshold and 
                        current_time - last_warning_time > warning_interval):
                        self.logger.warning(
                            f"높은 메모리 사용량 감지! "
                            f"현재 사용량: {memory_usage_gb:.2f}GB "
                            f"({memory_info['process_percent']:.1f}%)"
                        )
                        last_warning_time = current_time

                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"메모리 모니터링 중 오류 발생: {str(e)}")
                await asyncio.sleep(self.check_interval)

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._monitor())
            self.logger.info(
                f"메모리 모니터링 시작 (확인 주기: {self.check_interval}초, "
                f"경고 임계값: {self.warning_threshold}%)"
            )

    async def stop(self):
        if self._running:
            self._running = False
            if self._task:
                await self._task
            self.logger.info("메모리 모니터링이 중지되었습니다.")

class APIRateMonitor:
    def __init__(self, metrics: TrademarkMetrics, window_size: int = 5):
        self.window_size = window_size
        self.requests = deque()
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger(__name__)
        self.last_log_time = time()
        self.log_interval = 5
        self.metrics = metrics
        self.last_rate_update = time()
        self.rate_update_interval = 1.0

    async def record_request(self):
        async with self._lock:
            current_time = time()
            self.requests.append(current_time)
            
            while self.requests and current_time - self.requests[0] > self.window_size:
                self.requests.popleft()
            
            if current_time - self.last_rate_update >= self.rate_update_interval:
                window_rate = len(self.requests) / self.window_size
                self.metrics.api_request_window_rate.labels(
                    window_seconds=str(self.window_size)
                ).set(window_rate)
                
                recent_requests = sum(1 for req_time in self.requests 
                                   if current_time - req_time <= 1.0)
                self.metrics.api_request_rate.set(recent_requests)
                
                self.last_rate_update = current_time
            
            if current_time - self.last_log_time >= self.log_interval:
                window_rate = len(self.requests) / self.window_size
                recent_rate = sum(1 for req_time in self.requests 
                                if current_time - req_time <= 1.0)
                
                self.logger.info(
                    f"API 호출 속도 - 순간: {recent_rate:.2f} req/s, "
                    f"평균: {window_rate:.2f} req/s (최근 {self.window_size}초)"
                )
                self.last_log_time = current_time

class TokenBucketRateLimiter:
    def __init__(self, metrics: TrademarkMetrics, rate_limit: int = 45, 
                 time_window: float = 1.0, burst_limit: int = 45):
        self.rate_limit = rate_limit
        self.time_window = time_window
        self.burst_limit = burst_limit
        self.tokens = burst_limit
        self.last_update = time()
        self.requests = deque()
        self._lock = asyncio.Lock()
        self.api_monitor = APIRateMonitor(metrics)

    async def acquire(self):
        async with self._lock:
            current_time = time()
            
            time_passed = current_time - self.last_update
            new_tokens = time_passed * self.rate_limit
            self.tokens = min(self.burst_limit, self.tokens + new_tokens)
            self.last_update = current_time

            while self.requests and current_time - self.requests[0] > self.time_window:
                self.requests.popleft()

            if len(self.requests) >= self.rate_limit:
                wait_time = self.requests[0] + self.time_window - current_time
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    return await self.acquire()

            self.requests.append(current_time)
            await self.api_monitor.record_request()
            await asyncio.sleep(1.0 / self.rate_limit)

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.headers = {'Content-Type': 'application/json'}
        self.session = None

    async def init_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def send_message(self, message: str):
        try:
            await self.init_session()
            data = {'text': message}
            async with self.session.post(
                self.webhook_url,
                headers=self.headers,
                json=data
            ) as response:
                if response.status != 200:
                    logging.error(f"Slack 메시지 전송 실패: {response.status}, {await response.text()}")
                return response.status == 200
        except Exception as e:
            logging.error(f"Slack 메시지 전송 중 오류: {str(e)}")
            return False

    async def cleanup(self):
        if self.session and not self.session.closed:
            await self.session.close()

class XMLBuilder:
    def __init__(self, filename: str):
        self.root = ET.Element("trademarkInfoData")
        self.items = ET.SubElement(self.root, "items")
        self.filename = filename
        self._lock = asyncio.Lock()
        self.trademark_count = 0
        self.last_save_count = 0
        self.save_threshold = 10000
        self.logger = logging.getLogger(__name__)

    def get_summary(self):
        return f"{self.filename}: 총 {self.trademark_count:,}개 상표"
    
    async def add_trademark_data(self, xml_string: str, applicant_no: str) -> bool:
        try:
            async with self._lock:
                source_root = ET.fromstring(xml_string)
                items = source_root.findall('.//item')
                
                if not items:
                    return False
                
                for source_item in items:
                    new_item = ET.SubElement(self.items, "item")
                    
                    applicant_no_elem = ET.SubElement(new_item, "applicantNo")
                    applicant_no_elem.text = applicant_no
                    
                    fields = [
                        'agentName', 'appReferenceNumber', 'applicantName',
                        'applicationDate', 'applicationNumber', 'applicationStatus',
                        'bigDrawing', 'classificationCode', 'drawing', 'fullText',
                        'indexNo', 'priorityDate', 'priorityNumber', 'publicationDate',
                        'publicationNumber', 'regPrivilegeName', 'regReferenceNumber',
                        'registrationDate', 'registrationNumber', 'registrationPublicDate',
                        'registrationPublicNumber', 'title', 'viennaCode'
                    ]
                    
                    for field in fields:
                        elem = source_item.find(field)
                        if elem is not None:
                            field_elem = ET.SubElement(new_item, field)
                            field_elem.text = elem.text.strip() if elem.text else ''
                            
                    self.trademark_count += 1
                    
                    if self.trademark_count - self.last_save_count >= self.save_threshold:
                        self.save()
                        self.last_save_count = self.trademark_count
                        self.logger.info(f"중간 저장 완료: {self.trademark_count:,}개 상표")
                    
                return True
                
        except ET.ParseError:
            return False
            
    def save(self):
        try:
            os.makedirs('data', exist_ok=True)
            backup_file = f"{self.filename}.bak"
            if os.path.exists(os.path.join('data', self.filename)):
                shutil.copy2(
                    os.path.join('data', self.filename),
                    os.path.join('data', backup_file)
                )
            
            filepath = os.path.join('data', self.filename)
            tree = ET.ElementTree(self.root)
            ET.indent(tree, space="  ", level=0)
            tree.write(filepath, encoding='utf-8', xml_declaration=True)
            
            self.logger.info(f"XML 저장 완료: {self.filename} (총 {self.trademark_count:,}개)")
        except Exception as e:
            self.logger.error(f"XML 저장 중 오류: {str(e)}")

class ProgressTracker:
    def __init__(self, table_type: str):
        os.makedirs('data', exist_ok=True)
        self.progress_file = f'data/progress_trademark_{table_type}.json'
        self.current_offset = 0
        self.processed_applicants = set()
        self.load_progress()

    def load_progress(self):
        try:
            if os.path.exists(self.progress_file) and os.path.getsize(self.progress_file) > 0:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.current_offset = data.get('offset', 0)
                    self.processed_applicants = set(data.get('processed_applicants', []))
                    logging.info(f"진행상황 로드 성공: offset {self.current_offset}, "
                               f"처리된 신청인 수 {len(self.processed_applicants)}")
                    return True
            else:
                self.save_progress(0)
                logging.info("새로운 진행상황 파일 생성")
                return False
        except json.JSONDecodeError as e:
            logging.error(f"진행상황 파일 손상: {e}")
            if os.path.exists(self.progress_file):
                backup_file = f"{self.progress_file}.bak"
                os.rename(self.progress_file, backup_file)
                logging.info(f"손상된 파일 백업: {backup_file}")
            self.save_progress(0)
            return False
        except Exception as e:
            logging.error(f"진행상황 로드 중 예외 발생: {str(e)}")
            return False

    def save_progress(self, offset: int, processed_applicant: str = None):
        try:
            if processed_applicant:
                self.processed_applicants.add(processed_applicant)
            self.current_offset = offset
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'offset': self.current_offset,
                    'processed_applicants': list(self.processed_applicants),
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, f, ensure_ascii=False, indent=2)
                
            logging.debug(f"진행상황 저장 완료: offset {offset}")
        except Exception as e:
            logging.error(f"진행상황 저장 실패: {str(e)}")

    def is_applicant_processed(self, applicant_no: str) -> bool:
        return applicant_no in self.processed_applicants
    
class AsyncTrademarkDownloader:
    def __init__(self, table_types: List[str]):
        start_http_server(8003, addr='0.0.0.0')
        self.metrics = TrademarkMetrics()
        self.setup_logging()
        
        # 메모리 모니터 초기화
        self.memory_monitor = MemoryMonitor(
            metrics=self.metrics,
            logger=self.logger,
            check_interval=60,  # 1분마다 체크
            warning_threshold=80.0  # 80% 이상일 때 경고
        )
        
        load_dotenv()
        self.validate_env()
        
        self.service_key = os.getenv('KIPRIS_API_KEY')
        self.batch_size = int(os.getenv('BATCH_SIZE'))
        self.base_url = "http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/getAdvancedSearch"
        self.slack = SlackNotifier(os.getenv('SLACK_WEBHOOK_URL'))
        
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'db': os.getenv('DB_NAME'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        
        self.session = None
        self.pool = None
        date_str = datetime.now().strftime('%Y%m%d')
        
        self.builders: Dict[str, XMLBuilder] = {}
        self.table_names: Dict[str, str] = {}
        self.progress_trackers: Dict[str, ProgressTracker] = {}
        
        for table_type in table_types:
            if table_type == 'TB24_200':
                self.builders[table_type] = XMLBuilder(f'{date_str}_trademark_corp.xml')
                self.table_names[table_type] = 'tb24_200_corp_applicant'
            else:
                self.builders[table_type] = XMLBuilder(f'{date_str}_trademark_univ.xml')
                self.table_names[table_type] = 'tb24_210_univ_applicant'
            self.progress_trackers[table_type] = ProgressTracker(table_type)
            
        self.concurrent_limit = 20
        self.semaphore = asyncio.Semaphore(self.concurrent_limit)
        self.rate_limiter = TokenBucketRateLimiter(
            metrics=self.metrics,
            rate_limit=60,
            time_window=1.0,
            burst_limit=60
        )
        
        self.failed_requests: Dict[str, List[str]] = {table_type: [] for table_type in table_types}

    async def cleanup(self):
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                
            if self.pool:
                self.pool.close()
                await self.pool.wait_closed()
                
            await self.slack.cleanup()
            
            self.logger.info("모든 리소스가 정상적으로 정리되었습니다.")
        except Exception as e:
            self.logger.error(f"리소스 정리 중 오류 발생: {str(e)}")

    def validate_env(self):
        required_vars = [
            'KIPRIS_API_KEY', 'DB_HOST', 'DB_USER', 'DB_PASSWORD', 
            'DB_NAME', 'BATCH_SIZE', 'SLACK_WEBHOOK_URL'
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    def setup_logging(self):
        os.makedirs('logs', exist_ok=True)
        log_file = f'logs/trademark_download_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    async def init_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=20, sock_connect=10)
            connector = aiohttp.TCPConnector(
                ssl=create_urllib3_context(
                    ciphers='ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20'
                ),
                keepalive_timeout=60,
                enable_cleanup_closed=True,
                limit=50,
                force_close=False,
                limit_per_host=25
            )
            
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'User-Agent': 'TrademarkDownloader/1.0'}
            )

    async def init_db_pool(self):
        if self.pool is None:
            self.pool = await aiomysql.create_pool(
                **self.db_config,
                maxsize=20,
                minsize=5,
                pool_recycle=3600,
                autocommit=True
            )
    
    async def get_applicant_numbers(self, table_type: str, offset: int = 0) -> Tuple[List[str], int]:
        if self.pool is None:
            await self.init_db_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                table_name = self.table_names[table_type]
                await cursor.execute(
                    f"SELECT COUNT(DISTINCT applicant_no) FROM {table_name} WHERE applicant_no IS NOT NULL"
                )
                total_count = (await cursor.fetchone())[0]
                
                await cursor.execute(
                    f"SELECT DISTINCT applicant_no FROM {table_name} WHERE applicant_no IS NOT NULL LIMIT %s OFFSET %s",
                    (self.batch_size, offset)
                )
                
                results = await cursor.fetchall()
                return [row[0] for row in results], total_count

    async def fetch_trademark_data(self, applicant_no: str, page: int = 1) -> Optional[str]:
        await self.rate_limiter.acquire()
        start_time = time()
        try:
            self.logger.debug(f"Fetching data for {applicant_no} - page {page}")
            params = {
                'applicantName': applicant_no,
                'ServiceKey': self.service_key,
                'pageNo': str(page),
                'numOfRows': '500',
                'application': 'true',
                'registration': 'true',
                'refused': 'true',
                'expiration': 'true',
                'withdrawal': 'true',
                'publication': 'true',
                'cancel': 'true',
                'abandonment': 'true',
                'trademark': 'true',
                'serviceMark': 'true',
                'businessEmblem': 'true',
                'collectiveMark': 'true',
                'geoOrgMark': 'true',
                'trademarkServiceMark': 'true'
            }
            
            async with self.session.get(self.base_url, params=params) as response:
                response_text = await response.text()
                self.logger.debug(f"Response for {applicant_no}: {response_text[:200]}...")
                
                # API 요청 시간 기록
                request_duration = time() - start_time
                self.metrics.api_request_duration.observe(request_duration)
                
                if '<successYN>N</successYN>' in response_text:
                    self.logger.error(f"API error for {applicant_no}: {response_text}")
                    self.metrics.api_errors.inc()
                    return None
                    
                if response.status != 200:
                    self.logger.error(f"HTTP error {response.status} for {applicant_no}")
                    self.metrics.api_errors.inc()
                    return None
                
                if '<items>' not in response_text or '<item>' not in response_text:
                    self.logger.warning(f"No items found for {applicant_no} in page {page}")
                    return None
                        
                self.metrics.api_requests.inc()
                return response_text
                
        except Exception as e:
            self.logger.error(f"Exception in fetch_trademark_data for {applicant_no}: {str(e)}")
            self.metrics.api_errors.inc()
            return None
        finally:
            # 요청 시간이 긴 경우 로그로 기록
            request_duration = time() - start_time
            if request_duration > 5:  # 5초 이상 걸린 요청 로깅
                self.logger.warning(
                    f"Slow API request for {applicant_no} (page {page}): {request_duration:.2f} seconds"
                )

    async def process_single_applicant(self, table_type: str, applicant_no: str):
        if self.progress_trackers[table_type].is_applicant_processed(applicant_no):
            self.logger.debug(f"[{table_type}] 이미 처리된 신청인 스킵: {applicant_no}")
            return

        async with self.semaphore:
            try:
                page = 1
                total_processed = 0
                
                while True:
                    xml_content = await self.fetch_trademark_data(applicant_no, page)
                    if not xml_content:
                        if page == 1:
                            self.logger.info(f"[{table_type}] 데이터 없음: {applicant_no}")
                            self.failed_requests[table_type].append(applicant_no)
                        break

                    try:
                        root = ET.fromstring(xml_content)
                        count_elem = root.find('.//count/totalCount')
                        total_count = int(count_elem.text) if count_elem is not None else 0
                        
                        success = await self.builders[table_type].add_trademark_data(xml_content, applicant_no)
                        if success:
                            items = root.findall('.//item')
                            total_processed += len(items)
                            self.metrics.successful_downloads.inc(len(items))
                            
                            self.logger.info(
                                f"[{table_type}] 처리 중: {applicant_no} - "
                                f"페이지 {page}, "
                                f"현재 {total_processed}/{total_count} 건"
                            )
                            
                            if total_processed >= total_count or len(items) == 0:
                                self.logger.info(
                                    f"[{table_type}] 처리 완료: {applicant_no} - "
                                    f"총 {total_processed}건"
                                )
                                self.progress_trackers[table_type].save_progress(
                                    self.progress_trackers[table_type].current_offset,
                                    applicant_no
                                )
                                break
                        else:
                            self.logger.warning(f"[{table_type}] 처리 실패: {applicant_no} - 페이지 {page}")
                            break
                            
                        page += 1
                        
                    except ET.ParseError as e:
                        self.logger.error(f"[{table_type}] XML 파싱 오류 {applicant_no}: {e}")
                        self.metrics.xml_parse_errors.inc()
                        break
                        
            except Exception as e:
                self.logger.error(f"[{table_type}] 처리 중 오류 발생 {applicant_no}: {str(e)}")
                self.failed_requests[table_type].append(applicant_no)

    async def process_applicants_batch(self, applicant_numbers: List[str], batch_index: int, 
                                     total_batches: int, table_type: str):
        batch_message = f"\n=== [{table_type}] 배치 {batch_index}/{total_batches} 처리 시작 (대상: {len(applicant_numbers)}개) ==="
        self.logger.info(batch_message)
        
        tasks = []
        for applicant_no in applicant_numbers:
            task = asyncio.create_task(
                self.process_single_applicant(table_type, applicant_no)
            )
            tasks.append(task)
        
        completed = 0
        for task in asyncio.as_completed(tasks):
            try:
                await task
                completed += 1
                if completed % 5 == 0:
                    progress_message = (
                        f"[{table_type}] 배치 {batch_index} 진행상황: "
                        f"{completed}/{len(applicant_numbers)} "
                        f"(전체 상표 수: {self.builders[table_type].trademark_count:,}개)"
                    )
                    self.logger.info(progress_message)
            except Exception as e:
                self.logger.error(f"[{table_type}] 배치 {batch_index}의 작업 실패: {str(e)}")
        
        if self.builders[table_type].trademark_count > 0:
            self.builders[table_type].save()
            
        batch_complete_message = f"\n=== [{table_type}] 배치 {batch_index}/{total_batches} 완료 ===\n"
        self.logger.info(batch_complete_message)

    async def retry_failed_requests(self, table_type: str):
        if not self.failed_requests[table_type]:
            self.logger.info(f"[{table_type}] 재시도할 실패 요청이 없습니다.")
            return

        retry_message = f"[{table_type}] {len(self.failed_requests[table_type])}개의 실패한 요청 재시도 시작..."
        self.logger.info(retry_message)
        
        # 실패한 요청들 비동기 병렬 처리
        failed_requests = self.failed_requests[table_type].copy()
        self.failed_requests[table_type] = []
        
        tasks = []
        for applicant_no in failed_requests:
            task = asyncio.create_task(
                self.process_single_applicant(table_type, applicant_no)
            )
            tasks.append(task)

        # 진행상황 모니터링을 위한 카운터
        completed = 0
        total = len(tasks)
        
        # 비동기로 완료되는 작업 처리
        for task in asyncio.as_completed(tasks):
            try:
                await task
                completed += 1
                if completed % 5 == 0:
                    self.logger.info(f"[{table_type}] 재시도 진행상황: {completed}/{total} 완료")
            except Exception as e:
                self.logger.error(f"[{table_type}] 재시도 작업 실패: {str(e)}")
        
        if self.failed_requests[table_type]:
            final_fail_message = f"재시도 후에도 {len(self.failed_requests[table_type])}개 요청 실패"
            self.logger.error(final_fail_message)
            
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            failed_requests_file = os.path.join('data', f'failed_requests_{table_type}_{date_str}.txt')
            
            with open(failed_requests_file, 'w', encoding='utf-8') as f:
                f.write(f"Total failed requests: {len(self.failed_requests[table_type])}\n")
                f.write("=" * 50 + "\n")
                for applicant_no in self.failed_requests[table_type]:
                    f.write(f"{applicant_no}\n")
                    
            self.logger.warning(f"Failed requests have been saved to '{failed_requests_file}'")
        else:
            success_message = "모든 실패 요청이 재시도에서 성공적으로 처리되었습니다."
            self.logger.info(success_message)

    async def process_type(self, table_type: str):
        try:
            offset = self.progress_trackers[table_type].current_offset
            current_batch = offset // self.batch_size
            
            # 전체 건수 먼저 조회
            _, total_count = await self.get_applicant_numbers(table_type, 0)
            total_batches = -(-total_count // self.batch_size)
            
            self.logger.info(f"""
    === [{table_type}] 전체 진행 상황 ===
    - 총 신청인 수: {total_count:,}명
    - 총 배치 수: {total_batches:,}개
    - 현재 배치: {current_batch + 1:,}번째
    - 현재 오프셋: {offset:,}
    - 처리된 신청인 수: {len(self.progress_trackers[table_type].processed_applicants):,}명
    - 진행률: {(len(self.progress_trackers[table_type].processed_applicants) / total_count * 100):.2f}%
    """)
            
            while True:
                applicant_numbers, _ = await self.get_applicant_numbers(table_type, offset)
                if not applicant_numbers:
                    break
                
                current_batch += 1
                self.progress_trackers[table_type].save_progress(offset)
                
                await self.process_applicants_batch(
                    applicant_numbers,
                    current_batch,
                    total_batches,
                    table_type
                )
                
                processed_count = len(self.progress_trackers[table_type].processed_applicants)
                progress_percentage = (processed_count / total_count * 100)
                
                self.logger.info(f"""
    [{table_type}] 진행상황 업데이트:
    - 처리된 상표 수: {self.builders[table_type].trademark_count:,}개
    - 처리된 신청인 수: {processed_count:,}명 / {total_count:,}명
    - 현재 오프셋: {offset:,}
    - 진행률: {progress_percentage:.2f}%
    - 실패 건수: {len(self.failed_requests[table_type]):,}개
    """)
                
                offset += self.batch_size
                
            # 처리 완료 후 최종 상황 보고
            final_processed_count = len(self.progress_trackers[table_type].processed_applicants)
            final_progress_percentage = (final_processed_count / total_count * 100)
            
            self.logger.info(f"""
    === [{table_type}] 처리 완료 ===
    - 최종 처리된 상표 수: {self.builders[table_type].trademark_count:,}개
    - 최종 처리된 신청인 수: {final_processed_count:,}명 / {total_count:,}명
    - 최종 진행률: {final_progress_percentage:.2f}%
    - 최종 실패 건수: {len(self.failed_requests[table_type]):,}개
    """)
                
        except Exception as e:
            self.logger.error(f"[{table_type}] 처리 중 오류 발생: {str(e)}")
            raise

    async def process_all(self):
        try:
            start_time = time()
            
            await self.init_db_pool()
            await self.init_session()
            
            # 메모리 모니터링 시작
            self.memory_monitor.start()
            
            # 모든 테이블 동시 처리
            tasks = []
            for table_type in self.builders.keys():
                task = asyncio.create_task(self.process_type(table_type))
                tasks.append(task)
            
            await asyncio.gather(*tasks)
            
            # 결과 요약
            duration = time() - start_time
            
            # 현재 메모리 사용량 포함
            memory_info = self.memory_monitor._get_memory_info()
            memory_usage_gb = memory_info['rss'] / (1024 * 1024 * 1024) if memory_info else 0
            
            # API 처리 시간 저장
            self.metrics.api_total_duration.observe(duration)
            
            summary = f"""
=== 처리 요약 ===
총 소요 시간: {duration:.2f}초
평균 API 응답 시간: {(duration / max(1, self.metrics.api_requests._value.get())):.2f}초
총 API 요청 수: {self.metrics.api_requests._value.get():,}개
- 성공: {self.metrics.successful_downloads._value.get():,}개
- 실패: {self.metrics.api_errors._value.get():,}개
XML 파싱 오류: {self.metrics.xml_parse_errors._value.get():,}개
현재 메모리 사용량: {memory_usage_gb:.2f}GB

저장된 파일 정보:"""

            for table_type in self.builders.keys():
                summary += f"\n- {self.builders[table_type].get_summary()}"
                summary += f"\n  실패한 요청 수: {len(self.failed_requests[table_type]):,}개"

            self.logger.info(summary)
            
        except Exception as e:
            error_message = f"처리 실패: {str(e)}"
            self.logger.error(error_message, exc_info=True)
            raise
        finally:
            # 메모리 모니터링 중지
            await self.memory_monitor.stop()
            await self.cleanup()

async def main():
    try:
        downloader = AsyncTrademarkDownloader(['TB24_200', 'TB24_210'])
        # await downloader.slack.send_message("<!here> 상표 수집 시작: 현익")
        await downloader.process_all()
    except Exception as e:
        logging.error(f"애플리케이션 실패: {e}")
        raise
    # finally:
        # await downloader.slack.send_message("<!here> 상표 수집 완료: 현익")

if __name__ == "__main__":
    asyncio.run(main())