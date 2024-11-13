import os
import time
import json
import aiohttp
import aiomysql
import asyncio
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, List, Tuple
from collections import deque
from time import time
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Summary, Gauge
from urllib3.util.ssl_ import create_urllib3_context

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
        if self.session:
            await self.session.close()

class TokenBucketRateLimiter:
    def __init__(self, rate_limit: int = 50, time_window: float = 1.0, burst_limit: int = 50):
        self.rate_limit = rate_limit
        self.time_window = time_window
        self.burst_limit = burst_limit
        self.tokens = burst_limit
        self.last_update = time()
        self.requests = deque()
        self._lock = asyncio.Lock()

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
            await asyncio.sleep(1.0 / self.rate_limit)

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

class XMLBuilder:
    def __init__(self, filename: str):
        self.root = ET.Element("trademarkInfoData")
        self.items = ET.SubElement(self.root, "items")
        self.filename = filename
        self._lock = asyncio.Lock()
        self.trademark_count = 0
        
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
                        'indexNo', 'internationalRegisterDate', 'internationalRegisterNumber',
                        'priorityDate', 'priorityNumber', 'publicationDate',
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
                    
                return True
                
        except ET.ParseError:
            return False
            
    def save(self):
        os.makedirs('data', exist_ok=True)
        filepath = os.path.join('data', self.filename)
        tree = ET.ElementTree(self.root)
        ET.indent(tree, space="  ", level=0)
        tree.write(filepath, encoding='utf-8', xml_declaration=True)

class AsyncTrademarkDownloader:
    def __init__(self, table_type: str):
        start_http_server(8002, addr='0.0.0.0')
        self.metrics = TrademarkMetrics()
        
        load_dotenv()
        self.validate_env()
        
        self.service_key = os.getenv('KIPRIS_API_KEY')
        self.batch_size = int(os.getenv('BATCH_SIZE'))
        self.base_url = "http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/getAdvancedSearch"
        self.slack = SlackNotifier(os.getenv('SLACK_WEBHOOK_URL'))
        self.table_type = table_type
        
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
        
        if table_type == 'TB24_200':
            self.xml_builder = XMLBuilder(f'{date_str}_trademark_corp.xml')
            self.table_name = 'tb24_200_corp_applicant'
        else:
            self.xml_builder = XMLBuilder(f'{date_str}_trademark_univ.xml')
            self.table_name = 'tb24_210_univ_applicant'
            
        self.concurrent_limit = 10  # 동시 처리 제한 축소
        self.semaphore = asyncio.Semaphore(self.concurrent_limit)
        self.rate_limiter = TokenBucketRateLimiter(
            rate_limit=45,  # 여유있게 45로 설정
            time_window=1.0,
            burst_limit=45
        )
        
        self.setup_logging()
        self.failed_requests = []

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

    async def get_applicant_numbers(self, offset: int = 0) -> Tuple[List[str], int]:
        if self.pool is None:
            await self.init_db_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"SELECT COUNT(DISTINCT applicant_no) FROM {self.table_name} WHERE applicant_no IS NOT NULL"
                )
                total_count = (await cursor.fetchone())[0]
                
                await cursor.execute(
                    f"SELECT DISTINCT applicant_no FROM {self.table_name} WHERE applicant_no IS NOT NULL LIMIT %s OFFSET %s",
                    (self.batch_size, offset)
                )
                
                results = await cursor.fetchall()
                return [row[0] for row in results], total_count

    async def fetch_trademark_data(self, applicant_no: str, page: int = 1) -> Optional[str]:
        await self.rate_limiter.acquire()
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

    async def process_single_applicant(self, applicant_no: str):
        async with self.semaphore:
            try:
                page = 1
                total_processed = 0
                
                while True:
                    xml_content = await self.fetch_trademark_data(applicant_no, page)
                    if not xml_content:
                        self.logger.warning(f"No content received for {applicant_no} at page {page}")
                        if page == 1:
                            self.failed_requests.append(applicant_no)
                        break

                    try:
                        root = ET.fromstring(xml_content)
                        count_elem = root.find('.//count/totalCount')
                        total_count = int(count_elem.text) if count_elem is not None else 0
                        
                        success = await self.xml_builder.add_trademark_data(xml_content, applicant_no)
                        if success:
                            items = root.findall('.//item')
                            total_processed += len(items)
                            self.metrics.successful_downloads.inc(len(items))
                            
                            self.logger.info(
                                f"Processed {applicant_no}: {total_processed}/{total_count} "
                                f"(Page {page}, Items: {len(items)})"
                            )
                            
                            if total_processed >= total_count or len(items) == 0:
                                break
                        else:
                            self.logger.error(f"Failed to add data for {applicant_no} at page {page}")
                            break
                        page += 1
                        
                    except ET.ParseError as e:
                        self.logger.error(f"XML parsing error for {applicant_no}: {e}")
                        self.metrics.xml_parse_errors.inc()
                        break
                        
            except Exception as e:
                self.logger.error(f"Error processing {applicant_no}: {str(e)}")
                self.failed_requests.append(applicant_no)

    async def retry_failed_requests(self):
        if not self.failed_requests:
            self.logger.info("No failed requests to retry")
            return

        retry_message = f"{len(self.failed_requests)}개의 실패한 요청 재시도 시작..."
        self.logger.info(retry_message)
        
        # 실패한 요청들 목록 출력
        for idx, applicant_no in enumerate(self.failed_requests, 1):
            self.logger.info(f"Failed request {idx}: {applicant_no}")
        
        # 실패한 요청들을 복사하고 원본 리스트 초기화
        failed_requests = self.failed_requests.copy()
        self.failed_requests = []
        
        # 실패한 요청들 하나씩 재시도
        for idx, applicant_no in enumerate(failed_requests, 1):
            retry_log = f"재시도 {idx}/{len(failed_requests)}: {applicant_no}"
            self.logger.info(retry_log)
            await self.process_single_applicant(applicant_no)
            await asyncio.sleep(1)  # 재시도 간 간격 추가
        
        # 재시도 후에도 실패한 요청들 처리
        if self.failed_requests:
            final_fail_message = f"재시도 후에도 {len(self.failed_requests)}개 요청 실패"
            self.logger.error(final_fail_message)
            
            # 실패한 요청들 파일로 저장
            os.makedirs('data', exist_ok=True)
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            failed_requests_file = os.path.join('data', f'failed_requests_{date_str}.txt')
            
            with open(failed_requests_file, 'w', encoding='utf-8') as f:
                f.write(f"Total failed requests: {len(self.failed_requests)}\n")
                f.write("=" * 50 + "\n")
                for applicant_no in self.failed_requests:
                    f.write(f"{applicant_no}\n")
                    
            self.logger.warning(f"Failed requests have been saved to '{failed_requests_file}'")
            
        else:
            success_message = "모든 실패 요청이 재시도에서 성공적으로 처리되었습니다."
            self.logger.info(success_message)
            

    async def process_applicants_batch(self, applicant_numbers: List[str], batch_index: int, total_batches: int):
        batch_message = f"배치 {batch_index}/{total_batches} 처리 시작 (대상: {len(applicant_numbers)}개)"
        self.logger.info(batch_message)
        
        tasks = []
        for applicant_no in applicant_numbers:
            task = asyncio.create_task(self.process_single_applicant(applicant_no))
            tasks.append(task)
        
        # 태스크 실행 및 결과 수집
        completed = 0
        for task in asyncio.as_completed(tasks):
            try:
                await task
                completed += 1
                if completed % 10 == 0:  # 10개 단위로 진행상황 보고
                    progress_message = f"배치 {batch_index} 진행상황: {completed}/{len(applicant_numbers)}"
                    self.logger.info(progress_message)
            except Exception as e:
                self.logger.error(f"배치 {batch_index}의 작업 실패: {str(e)}")
        
        batch_complete_message = f"배치 {batch_index}/{total_batches} 완료"
        self.logger.info(batch_complete_message)
        
        # 중간 저장
        if self.xml_builder.trademark_count > 0:
            save_message = f"배치 {batch_index}의 중간 결과 저장 중"
            self.logger.info(save_message)
            self.xml_builder.save()

    async def process_all(self):
        try:
            start_message = f"{self.table_type} 상표 데이터 다운로드 프로세스 시작"
            self.logger.info(start_message)
            
            start_time = time()
            
            await self.init_db_pool()
            await self.init_session()
            
            offset = 0
            current_batch = 0
            _, total_count = await self.get_applicant_numbers(0)
            total_batches = -(-total_count // self.batch_size)  # 올림 나눗셈
            
            total_info = f"총 처리할 배치: {total_batches} (전체 대상: {total_count}개)"
            self.logger.info(total_info)
            
            while True:
                applicant_numbers, _ = await self.get_applicant_numbers(offset)
                if not applicant_numbers:
                    break
                
                current_batch += 1
                await self.process_applicants_batch(
                    applicant_numbers,
                    current_batch,
                    total_batches
                )
                
                progress_message = (
                    f"진행상황: {self.xml_builder.trademark_count}개 상표 처리됨, "
                    f"오프셋: {offset}/{total_count}"
                )
                self.logger.info(progress_message)
                
                offset += self.batch_size
            
            # 실패한 요청 재시도
            if self.failed_requests:
                retry_message = f"{len(self.failed_requests)}개의 실패한 요청 재시도 중..."
                self.logger.info(retry_message)
                await self.retry_failed_requests()
            
            # 최종 저장
            final_save_message = "최종 결과 저장 중..."
            self.logger.info(final_save_message)
            self.xml_builder.save()
            
            # 결과 요약
            duration = time() - start_time
            summary = f"""
=== 처리 요약 ===
총 처리 시간: {duration:.2f}초
처리된 상표 수: {self.xml_builder.trademark_count}개
실패한 요청 수: {len(self.failed_requests)}개
"""
            self.logger.info(summary)
            
        except Exception as e:
            error_message = f"처리 실패: {str(e)}"
            self.logger.error(error_message, exc_info=True)
            raise
        finally:
            await self.cleanup()
            
    async def cleanup(self):
        if self.session and not self.session.closed:
            await self.session.close()
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
        await self.slack.cleanup()

async def main(table_type: str = 'TB24_200'):
    try:
        downloader = AsyncTrademarkDownloader(table_type)
        await downloader.slack.send_message("<!here> 사용 시작 : 현익")
        await downloader.process_all()
    except Exception as e:
        logging.error(f"애플리케이션 실패: {e}")
        raise
    finally:
        await downloader.slack.send_message("<!here> 사용 완료 : 현익")

if __name__ == "__main__":
    asyncio.run(main())