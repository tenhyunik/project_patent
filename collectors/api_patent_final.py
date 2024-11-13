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
from typing import Optional, List, Tuple
from collections import deque
from time import time
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Summary, Gauge
from urllib3.util.ssl_ import create_urllib3_context

class PatentMetrics:
    def __init__(self):
        self.api_requests = Counter(
            'patent_api_requests_total', 
            'Total number of API requests made'
        )
        self.api_errors = Counter(
            'patent_api_errors_total', 
            'Total number of API request errors'
        )
        self.xml_parse_errors = Counter(
            'patent_xml_parse_errors_total', 
            'Total number of XML parsing errors'
        )
        self.successful_downloads = Counter(
            'patent_successful_downloads_total', 
            'Total number of successfully downloaded patents'
        )
        self.api_request_duration = Summary(
            'patent_api_request_duration_seconds', 
            'Time spent in API requests'
        )
        self.api_total_duration = Summary(
            'patent_api_total_duration_seconds',
            'Total time from start to finish of API processing'
        )
        self.api_request_rate = Gauge(
            'patent_api_request_rate',
            'Current API request rate per second'
        )
        self.api_request_window_rate = Gauge(
            'patent_api_request_window_rate',
            'API request rate over sliding window',
            ['window_seconds']
        )
        # 메모리 관련 메트릭 추가
        self.memory_usage = Gauge(
            'patent_memory_usage_bytes',
            'Current memory usage of the process'
        )
        self.memory_percent = Gauge(
            'patent_memory_usage_percent',
            'Current memory usage as percentage of system memory'
        )
        self.virtual_memory_usage = Gauge(
            'patent_virtual_memory_usage_bytes',
            'Current virtual memory usage of the process'
        )
        self.system_memory_usage = Gauge(
            'patent_system_memory_usage_percent',
            'Current system memory usage percentage'
        )

class MemoryMonitor:
    def __init__(self, metrics: PatentMetrics, logger: logging.Logger, 
                 check_interval: int = 60, warning_threshold: float = 80.0):
        self.metrics = metrics
        self.logger = logger
        self.check_interval = check_interval  # 초 단위
        self.warning_threshold = warning_threshold  # 퍼센트
        self.process = psutil.Process(os.getpid())
        self._running = False
        self._task = None

    def _get_memory_info(self):
        try:
            # 프로세스 메모리 사용량
            process_memory = self.process.memory_info()
            memory_percent = self.process.memory_percent()
            
            # 시스템 메모리 사용량
            system_memory = psutil.virtual_memory()
            
            return {
                'rss': process_memory.rss,  # 실제 물리 메모리 사용량
                'vms': process_memory.vms,  # 가상 메모리 사용량
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
        warning_interval = 300  # 경고 메시지 최소 간격 (5분)

        while self._running:
            try:
                memory_info = self._get_memory_info()
                if memory_info:
                    self._update_metrics(memory_info)
                    
                    current_time = time()
                    memory_usage_gb = memory_info['rss'] / (1024 * 1024 * 1024)
                    system_memory_gb = memory_info['system_available'] / (1024 * 1024 * 1024)

                    # 상세 로깅
                    self.logger.info(
                        f"메모리 사용량 - "
                        f"프로세스: {memory_usage_gb:.2f}GB ({memory_info['process_percent']:.1f}%), "
                        f"시스템: {memory_info['system_percent']:.1f}% "
                        f"(가용: {system_memory_gb:.2f}GB)"
                    )

                    # 경고 조건 체크
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

class APIRateMonitor:
    def __init__(self, metrics: PatentMetrics, window_size: int = 5):
        self.window_size = window_size
        self.requests = deque()
        self._lock = asyncio.Lock()
        self.logger = logging.getLogger(__name__)
        self.last_log_time = time()
        self.log_interval = 5
        self.metrics = metrics
        self.last_rate_update = time()
        self.rate_update_interval = 1.0  # 1초마다 메트릭 업데이트

    async def record_request(self):
        async with self._lock:
            current_time = time()
            self.requests.append(current_time)
            
            # 윈도우 크기보다 오래된 요청 제거
            while self.requests and current_time - self.requests[0] > self.window_size:
                self.requests.popleft()
            
            # 메트릭 업데이트 (1초마다)
            if current_time - self.last_rate_update >= self.rate_update_interval:
                # 전체 윈도우에 대한 평균 속도
                window_rate = len(self.requests) / self.window_size
                self.metrics.api_request_window_rate.labels(
                    window_seconds=str(self.window_size)
                ).set(window_rate)
                
                # 최근 1초 동안의 요청 수 계산
                recent_requests = sum(1 for req_time in self.requests 
                                   if current_time - req_time <= 1.0)
                self.metrics.api_request_rate.set(recent_requests)
                
                self.last_rate_update = current_time
            
            # 로깅 (5초마다)
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
    def __init__(self, metrics: PatentMetrics, rate_limit: int = 50, 
                 time_window: float = 1.0, burst_limit: int = 50):
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
            
            # 새로운 토큰 생성
            time_passed = current_time - self.last_update
            new_tokens = time_passed * self.rate_limit
            self.tokens = min(self.burst_limit, self.tokens + new_tokens)
            self.last_update = current_time

            # 오래된 요청 제거
            while self.requests and current_time - self.requests[0] > self.time_window:
                self.requests.popleft()

            # 현재 윈도우의 요청 수 확인
            if len(self.requests) >= self.rate_limit:
                wait_time = self.requests[0] + self.time_window - current_time
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    return await self.acquire()

            # 요청 기록
            self.requests.append(current_time)
            await self.api_monitor.record_request()
            await asyncio.sleep(1.0 / self.rate_limit)


class XMLBuilder:
    def __init__(self, filename: str):
        self.root = ET.Element("patentInfoData")
        self.items = ET.SubElement(self.root, "items")
        self.filename = filename
        self._lock = asyncio.Lock()
        self.patent_count = 0
        self.last_save_count = 0
        self.save_threshold = 10000
        self.logger = logging.getLogger(__name__)

    def get_summary(self):
        return f"{self.filename}: {self.patent_count}개"
        
    async def add_patent_data(self, xml_string: str, applicant_no: str) -> bool:
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
                        'registerStatus', 'inventionTitle', 'ipcNumber', 
                        'registerNumber', 'registerDate', 'applicationNumber',
                        'applicationDate', 'openNumber', 'openDate',
                        'publicationNumber', 'publicationDate', 'drawing',
                        'applicantName', 'astrtCont', 'bigDrawing'
                    ]
                    
                    for field in fields:
                        elem = source_item.find(field)
                        if elem is not None:
                            field_elem = ET.SubElement(new_item, field)
                            field_elem.text = elem.text.strip() if elem.text else ''
                            
                    self.patent_count += 1
                    
                    # 특허 추가 후 저장 여부 체크
                    if self.patent_count - self.last_save_count >= self.save_threshold:
                        self.save()
                        self.last_save_count = self.patent_count
                        self.logger.info(f"중간 저장 완료: {self.patent_count}개 특허")
                    
                return True
                
        except ET.ParseError:
            return False

    def save(self):
        try:
            os.makedirs('data', exist_ok=True)
            filepath = os.path.join('data', self.filename)
            tree = ET.ElementTree(self.root)
            ET.indent(tree, space="  ", level=0)
            tree.write(filepath, encoding='utf-8', xml_declaration=True)
            
            self.logger.info(f"XML 저장 완료: {self.filename} (총 {self.patent_count}개)")
        except Exception as e:
            self.logger.error(f"XML 저장 중 오류: {str(e)}")

class ProgressTracker:
    def __init__(self, table_name: str):
        self.progress_file = f'data/progress_{table_name}.json'
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

class AsyncPatentDownloader:
    def __init__(self):
        start_http_server(8000, addr='0.0.0.0')
        self.metrics = PatentMetrics()
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
        self.base_url = "http://plus.kipris.or.kr/kipo-api/kipi/patUtiModInfoSearchSevice/getAdvancedSearch"
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
        
        self.downloaders = {
            'corp': {
                'xml_builder': XMLBuilder(f'{date_str}_patent_utility_corp.xml'),
                'table_name': 'tb24_200_corp_applicant',
                'progress_tracker': ProgressTracker('TB24_200'),
                'failed_requests': []
            },
            'univ': {
                'xml_builder': XMLBuilder(f'{date_str}_patent_utility_univ.xml'),
                'table_name': 'tb24_210_univ_applicant',
                'progress_tracker': ProgressTracker('TB24_210'),
                'failed_requests': []
            }
        }
                
        self.concurrent_limit = 10
        self.semaphore = asyncio.Semaphore(self.concurrent_limit)
        self.rate_limiter = TokenBucketRateLimiter(
            metrics=self.metrics,
            rate_limit=45,
            time_window=1.0,
            burst_limit=45
        )
        
        self.setup_logging()

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
        log_file = f'logs/patent_download_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
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
                headers={'User-Agent': 'PatentDownloader/1.0'}
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

    async def get_applicant_numbers(self, table_name: str, offset: int = 0) -> Tuple[List[str], int]:
        if self.pool is None:
            await self.init_db_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
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

    async def fetch_patent_data(self, applicant_no: str, page: int = 1) -> Optional[str]:
        await self.rate_limiter.acquire()
        start_time = time()
        try:
            self.logger.debug(f"Fetching data for {applicant_no} - page {page}")
            params = {
                'applicant': applicant_no,
                'ServiceKey': self.service_key,
                'pageNo': str(page),
                'numOfRows': '500',
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
            self.logger.error(f"Exception in fetch_patent_data for {applicant_no}: {str(e)}")
            self.metrics.api_errors.inc()
            return None
        finally:
            # 요청 시간이 긴 경우 로그로 기록
            request_duration = time() - start_time
            if request_duration > 5:  # 5초 이상 걸린 요청 로깅
                self.logger.warning(
                    f"Slow API request for {applicant_no} (page {page}): {request_duration:.2f} seconds"
                )

    async def process_single_applicant(self, applicant_no: str, downloader_type: str):
        downloader = self.downloaders[downloader_type]
        
        if downloader['progress_tracker'].is_applicant_processed(applicant_no):
            self.logger.debug(f"[{downloader_type}] 이미 처리된 신청인 스킵: {applicant_no}")
            return

        async with self.semaphore:
            try:
                page = 1
                total_processed = 0
                
                while True:
                    xml_content = await self.fetch_patent_data(applicant_no, page)
                    if not xml_content:
                        if page == 1:
                            self.logger.info(
                                f"[{downloader_type}] 데이터 없음 ({applicant_no}) - "
                                f"현재까지 처리된 특허: {downloader['xml_builder'].patent_count:,}개"
                            )
                            downloader['failed_requests'].append(applicant_no)
                        break

                    try:
                        root = ET.fromstring(xml_content)
                        count_elem = root.find('.//count/totalCount')
                        total_count = int(count_elem.text) if count_elem is not None else 0
                        
                        success = await downloader['xml_builder'].add_patent_data(
                            xml_content, applicant_no
                        )
                        if success:
                            items = root.findall('.//item')
                            total_processed += len(items)
                            self.metrics.successful_downloads.inc(len(items))
                            
                            self.logger.info(
                                f"[{downloader_type}] 처리 중: {applicant_no} - "
                                f"페이지 {page}, 현재 {total_processed}/{total_count} 건 "
                                f"(총 누적 특허 수: {downloader['xml_builder'].patent_count:,}개)"
                            )
                            
                            if total_processed >= total_count or len(items) == 0:
                                self.logger.info(
                                    f"[{downloader_type}] 처리 완료: {applicant_no} - "
                                    f"총 {total_processed}건 처리됨"
                                )
                                downloader['progress_tracker'].save_progress(
                                    downloader['progress_tracker'].current_offset,
                                    applicant_no
                                )
                                break
                        else:
                            break
                            
                        page += 1
                        
                    except ET.ParseError as e:
                        self.logger.error(f"[{downloader_type}] XML 파싱 오류 {applicant_no}: {e}")
                        self.metrics.xml_parse_errors.inc()
                        break
                        
            except Exception as e:
                self.logger.error(f"[{downloader_type}] 처리 중 오류 발생 {applicant_no}: {str(e)}")
                downloader['failed_requests'].append(applicant_no)

    async def retry_failed_requests(self, downloader_type: str):
        downloader = self.downloaders[downloader_type]
        if not downloader['failed_requests']:
            self.logger.info(f"[{downloader_type}] 재시도할 실패 요청이 없습니다.")
            return

        retry_message = f"[{downloader_type}] {len(downloader['failed_requests'])}개의 실패한 요청 재시도 시작..."
        self.logger.info(retry_message)
        
        # 실패한 요청들 비동기 병렬 처리
        failed_requests = downloader['failed_requests'].copy()
        downloader['failed_requests'] = []
        
        tasks = []
        for applicant_no in failed_requests:
            task = asyncio.create_task(
                self.process_single_applicant(applicant_no, downloader_type)
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
                    self.logger.info(f"[{downloader_type}] 재시도 진행상황: {completed}/{total} 완료")
            except Exception as e:
                self.logger.error(f"[{downloader_type}] 재시도 작업 실패: {str(e)}")
        
        if downloader['failed_requests']:
            final_fail_message = f"재시도 후에도 {len(downloader['failed_requests'])}개 요청 실패"
            self.logger.error(final_fail_message)
            
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            failed_requests_file = os.path.join(
                'data', f'failed_requests_{downloader_type}_{date_str}.txt'
            )
            
            with open(failed_requests_file, 'w', encoding='utf-8') as f:
                f.write(f"Total failed requests: {len(downloader['failed_requests'])}\n")
                f.write("=" * 50 + "\n")
                for applicant_no in downloader['failed_requests']:
                    f.write(f"{applicant_no}\n")
                    
            self.logger.warning(f"Failed requests have been saved to '{failed_requests_file}'")
        else:
            success_message = "모든 실패 요청이 재시도에서 성공적으로 처리되었습니다."
            self.logger.info(success_message)

    async def process_applicants_batch(self, applicant_numbers: List[str], batch_index: int, 
                                     total_batches: int, downloader_type: str):
        downloader = self.downloaders[downloader_type]
        batch_message = f"\n=== [{downloader_type}] 배치 {batch_index}/{total_batches} 처리 시작 (대상: {len(applicant_numbers)}개) ==="
        self.logger.info(batch_message)
        
        tasks = []
        for applicant_no in applicant_numbers:
            task = asyncio.create_task(
                self.process_single_applicant(applicant_no, downloader_type)
            )
            tasks.append(task)
        
        completed = 0
        for task in asyncio.as_completed(tasks):
            try:
                await task
                completed += 1
                if completed % 5 == 0:
                    progress_message = (
                        f"[{downloader_type}] 배치 {batch_index} 진행상황: "
                        f"{completed}/{len(applicant_numbers)} "
                        f"(전체 특허 수: {downloader['xml_builder'].patent_count}개)"
                    )
                    self.logger.info(progress_message)
            except Exception as e:
                self.logger.error(f"[{downloader_type}] 배치 {batch_index}의 작업 실패: {str(e)}")
        
        if downloader['xml_builder'].patent_count > 0:
            downloader['xml_builder'].save()
            
        batch_complete_message = f"\n=== [{downloader_type}] 배치 {batch_index}/{total_batches} 완료 ===\n"
        self.logger.info(batch_complete_message)

    async def process_type(self, downloader_type: str):
        try:
            downloader = self.downloaders[downloader_type]
            offset = downloader['progress_tracker'].current_offset
            current_batch = offset // self.batch_size
            
            # 전체 건수 먼저 조회
            _, total_count = await self.get_applicant_numbers(downloader['table_name'], 0)
            total_batches = -(-total_count // self.batch_size)
            
            self.logger.info(f"""
    === [{downloader_type}] 전체 진행 상황 ===
    - 총 신청인 수: {total_count:,}명
    - 총 배치 수: {total_batches:,}개
    - 현재 배치: {current_batch + 1:,}번째
    - 현재 오프셋: {offset:,}
    - 처리된 신청인 수: {len(downloader['progress_tracker'].processed_applicants):,}명
    - 진행률: {(len(downloader['progress_tracker'].processed_applicants) / total_count * 100):.2f}%
    """)
            
            while True:
                applicant_numbers, _ = await self.get_applicant_numbers(
                    downloader['table_name'], offset
                )
                if not applicant_numbers:
                    break
                
                current_batch += 1
                
                downloader['progress_tracker'].save_progress(offset)
                
                await self.process_applicants_batch(
                    applicant_numbers,
                    current_batch,
                    total_batches,
                    downloader_type
                )
                
                processed_count = len(downloader['progress_tracker'].processed_applicants)
                progress_percentage = (processed_count / total_count * 100)
                
                self.logger.info(f"""
    [{downloader_type}] 진행상황 업데이트:
    - 처리된 특허 수: {downloader['xml_builder'].patent_count:,}개
    - 처리된 신청인 수: {processed_count:,}명 / {total_count:,}명
    - 현재 오프셋: {offset:,}
    - 진행률: {progress_percentage:.2f}%
    - 실패 건수: {len(downloader['failed_requests']):,}개
    """)
                
                offset += self.batch_size
                
            # 처리 완료 후 최종 상황 보고
            final_processed_count = len(downloader['progress_tracker'].processed_applicants)
            final_progress_percentage = (final_processed_count / total_count * 100)
            
            self.logger.info(f"""
    === [{downloader_type}] 처리 완료 ===
    - 최종 처리된 특허 수: {downloader['xml_builder'].patent_count:,}개
    - 최종 처리된 신청인 수: {final_processed_count:,}명 / {total_count:,}명
    - 최종 진행률: {final_progress_percentage:.2f}%
    - 최종 실패 건수: {len(downloader['failed_requests']):,}개
    """)
                
        except Exception as e:
            self.logger.error(f"[{downloader_type}] 처리 중 오류 발생: {str(e)}")
            raise

    async def process_all(self):
        try:
            start_time = time()
            
            await self.init_db_pool()
            await self.init_session()
            
            # 메모리 모니터링 시작
            self.memory_monitor.start()
            
            # 기업과 대학 동시 처리
            corp_task = asyncio.create_task(self.process_type('corp'))
            univ_task = asyncio.create_task(self.process_type('univ'))
            
            await asyncio.gather(corp_task, univ_task)
            
            # 실패한 요청 재시도
            for downloader_type in self.downloaders:
                if self.downloaders[downloader_type]['failed_requests']:
                    await self.retry_failed_requests(downloader_type)
            
            # 최종 저장
            for downloader in self.downloaders.values():
                downloader['xml_builder'].save()
            
            # 결과 요약
            duration = time() - start_time
            
            # 현재 메모리 사용량 포함
            memory_info = self.memory_monitor._get_memory_info()
            memory_usage_gb = memory_info['rss'] / (1024 * 1024 * 1024) if memory_info else 0
            
            summary = f"""
=== 처리 요약 ===
총 처리 시간: {duration:.2f}초
메모리 사용량: {memory_usage_gb:.2f}GB
저장된 파일 정보:
- 기업: {self.downloaders['corp']['xml_builder'].get_summary()}
- 대학: {self.downloaders['univ']['xml_builder'].get_summary()}
실패한 요청 수:
- 기업: {len(self.downloaders['corp']['failed_requests'])}개
- 대학: {len(self.downloaders['univ']['failed_requests'])}개
"""
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
        downloader = AsyncPatentDownloader()
        await downloader.slack.send_message("<!here> 사용 시작: 현익")
        await downloader.process_all()
    except Exception as e:
        logging.error(f"애플리케이션 실패: {e}")
        raise
    # finally:
        # await downloader.slack.send_message("<!here> 사용 완료: 현익")

if __name__ == "__main__":
    asyncio.run(main())