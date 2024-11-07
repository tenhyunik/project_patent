import os
import time
import aiomysql
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from typing import Optional, List, Tuple
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Summary, Gauge
from urllib3.util.ssl_ import create_urllib3_context
from asyncio import Semaphore

class RateLimiter:
    def __init__(self, max_concurrent: int = 50):
        self.semaphore = Semaphore(max_concurrent)

    async def __aenter__(self):
        await self.semaphore.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.semaphore.release()
        await asyncio.sleep(0.1)  # 개별 요청 간 간격

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
                    
                    # applicant_no 추가
                    applicant_no_elem = ET.SubElement(new_item, "applicantNo")
                    applicant_no_elem.text = applicant_no
                    
                    # 모든 가능한 필드 목록
                    fields = [
                        'agentName',  # 대리인 이름
                        'appReferenceNumber',  # 출원 참조번호
                        'applicantName',  # 출원인 이름
                        'applicationDate',  # 출원일자
                        'applicationNumber',  # 출원번호
                        'applicationStatus',  # 법적상태
                        'bigDrawing',  # 큰 이미지 URL
                        'classificationCode',  # 상품분류코드
                        'drawing',  # 이미지 URL
                        'fullText',  # 전문 제공 여부
                        'indexNo',  # 일련번호
                        'internationalRegisterDate',  # 국제등록일자
                        'internationalRegisterNumber',  # 국제등록번호
                        'priorityDate',  # 우선권주장일자
                        'priorityNumber',  # 우선권번호
                        'publicationDate',  # 공고일자
                        'publicationNumber',  # 공고번호
                        'regPrivilegeName',  # 권리자 이름
                        'regReferenceNumber',  # 등록 참조번호
                        'registrationDate',  # 등록일자
                        'registrationNumber',  # 등록번호
                        'registrationPublicDate',  # 등록공고일자
                        'registrationPublicNumber',  # 등록공고번호
                        'title',  # 상표명
                        'viennaCode'  # 비엔나분류코드
                    ]
                    
                    # 각 필드에 대해 값이 있는 경우에만 추가
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

class TLSAdapter(aiohttp.TCPConnector):
    def __init__(self):
        ssl_context = create_urllib3_context(
            ciphers='ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20'
        )
        super().__init__(
            ssl=ssl_context,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            limit=100,
            force_close=False,
            limit_per_host=50
        )

class AsyncTrademarkDownloader:
    def __init__(self):
        start_http_server(8002, addr='0.0.0.0')
        self.metrics = TrademarkMetrics()
        
        load_dotenv()
        self.validate_env()
        
        self.service_key = os.getenv('KIPRIS_API_KEY')
        self.batch_size = int(os.getenv('BATCH_SIZE'))
        self.base_url = "http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/getAdvancedSearch"
        
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
        self.corp_xml_builder = XMLBuilder(f'{date_str}_trademark_corp.xml')
        self.univ_xml_builder = XMLBuilder(f'{date_str}_trademark_univ.xml')
        self.rate_limiter = RateLimiter()
        
        self.setup_logging()
        self.failed_requests = []

        self.concurrent_limit = 20  # 동시 처리 제한
        self.semaphore = asyncio.Semaphore(self.concurrent_limit)
        self.rate_limiter = RateLimiter()

    def validate_env(self):
        required_vars = ['KIPRIS_API_KEY', 'DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'BATCH_SIZE']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('trademark_download.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    async def init_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=60,
                connect=10,
                sock_read=20,
                sock_connect=10
            )
            connector = TLSAdapter()
            connector.ttl_dns_cache = 300  # DNS 캐시 시간 설정
            
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

    async def get_applicant_numbers(self, table: str, offset: int = 0) -> Tuple[List[str], int]:
        if self.pool is None:
            await self.init_db_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"SELECT COUNT(DISTINCT applicant_no) FROM {table} WHERE applicant_no IS NOT NULL"
                )
                total_count = (await cursor.fetchone())[0]
                
                await cursor.execute(
                    f"SELECT DISTINCT applicant_no FROM {table} WHERE applicant_no IS NOT NULL LIMIT %s OFFSET %s",
                    (self.batch_size, offset)
                )
                
                results = await cursor.fetchall()
                return [row[0] for row in results], total_count

    async def fetch_trademark_data(self, applicant_no: str, page: int = 1) -> Optional[str]:
        async with self.rate_limiter:
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
                    
                    # 응답 내용 로깅 (디버깅용)
                    self.logger.debug(f"Response for {applicant_no}: {response_text[:200]}...")
                    
                    # API 에러 체크
                    if '<successYN>N</successYN>' in response_text:
                        self.logger.error(f"API error for {applicant_no}: {response_text}")
                        return None
                        
                    if response.status != 200:
                        self.logger.error(f"HTTP error {response.status} for {applicant_no}")
                        return None
                    
                    # 실제 데이터 포함 여부 체크
                    if '<items>' not in response_text or '<item>' not in response_text:
                        self.logger.warning(f"No items found for {applicant_no} in page {page}")
                        return None
                            
                    return response_text
                    
            except Exception as e:
                self.logger.error(f"Exception in fetch_trademark_data for {applicant_no}: {str(e)}")
                return None

    async def process_single_applicant(self, applicant_no: str, xml_builder: XMLBuilder):
        async with self.semaphore:
            try:
                page = 1
                total_processed = 0
                
                while True:
                    xml_content = await self.fetch_trademark_data(applicant_no, page)
                    if not xml_content:
                        self.logger.warning(f"No content received for {applicant_no} at page {page}")
                        if page == 1:  # 첫 페이지에서 실패하면 재시도 목록에 추가
                            self.failed_requests.append((applicant_no, xml_builder))
                        break

                    try:
                        root = ET.fromstring(xml_content)
                        
                        # 전체 개수 확인
                        count_elem = root.find('.//count/totalCount')
                        total_count = int(count_elem.text) if count_elem is not None else 0
                        
                        # items 처리
                        success = await xml_builder.add_trademark_data(xml_content, applicant_no)
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
                self.failed_requests.append((applicant_no, xml_builder))

    async def retry_failed_requests(self):
        if not self.failed_requests:
            self.logger.info("No failed requests to retry")
            return

        self.logger.info(f"Starting retry of {len(self.failed_requests)} failed requests...")
        
        # 실패한 요청들 목록 출력
        for idx, (applicant_no, _) in enumerate(self.failed_requests, 1):
            self.logger.info(f"Failed request {idx}: {applicant_no}")
        
        # 실패한 요청들을 복사하고 원본 리스트 초기화
        failed_requests = self.failed_requests.copy()
        self.failed_requests = []
        
        # 실패한 요청들 하나씩 재시도
        for idx, (applicant_no, xml_builder) in enumerate(failed_requests, 1):
            self.logger.info(f"Retrying request {idx}/{len(failed_requests)}: {applicant_no}")
            await self.process_single_applicant(applicant_no, xml_builder, is_retry=True)
            await asyncio.sleep(1)  # 재시도 간 간격 추가
        
        # 재시도 후에도 실패한 요청들 처리
        if self.failed_requests:
            self.logger.error(f"Still failed after retry: {len(self.failed_requests)} requests")
            
            # 실패한 요청들 파일로 저장
            os.makedirs('data', exist_ok=True)
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')  # 시간까지 포함
            failed_requests_file = os.path.join('data', f'failed_requests_{date_str}.txt')
            
            with open(failed_requests_file, 'w', encoding='utf-8') as f:
                f.write(f"Total failed requests: {len(self.failed_requests)}\n")
                f.write("=" * 50 + "\n")
                for applicant_no, _ in self.failed_requests:
                    f.write(f"{applicant_no}\n")
                    
            self.logger.warning(f"Failed requests have been saved to '{failed_requests_file}'")
        else:
            self.logger.info("All failed requests were successfully processed in retry")

    async def process_applicants_batch(self, applicant_numbers: List[str], xml_builder: XMLBuilder, batch_index: int, total_batches: int):
        chunk_size = 50
        chunks = len(applicant_numbers) // chunk_size + (1 if len(applicant_numbers) % chunk_size else 0)
        
        self.logger.info(
            f"Processing batch {batch_index}/{total_batches} - "
            f"Total applicants: {len(applicant_numbers)}, "
            f"Chunks: {chunks}, Chunk size: {chunk_size}"
        )
        
        for i in range(0, len(applicant_numbers), chunk_size):
            chunk = applicant_numbers[i:i + chunk_size]
            chunk_number = i // chunk_size + 1
            
            self.logger.info(
                f"Processing batch {batch_index}/{total_batches} - "
                f"chunk {chunk_number}/{chunks} "
                f"({len(chunk)} applicants)"
            )
            
            tasks = [
                self.process_single_applicant(applicant_no, xml_builder)
                for applicant_no in chunk
            ]
            await asyncio.gather(*tasks)
            
            # 청크 처리 후 잠시 대기
            await asyncio.sleep(1)
            
            # 중간 저장 (일정 개수 이상 처리됐을 때)
            if xml_builder.trademark_count > 0 and xml_builder.trademark_count % 1000 == 0:
                self.logger.info(f"Intermediate save - {xml_builder.trademark_count} trademarks processed")
                xml_builder.save()
        
        self.logger.info(
            f"Completed batch {batch_index}/{total_batches} - "
            f"Current trademark count: {xml_builder.trademark_count}"
        )

    async def process_all(self):
        try:
            self.logger.info("Starting trademark data download process")
            start_time = time.time()
            
            await self.init_db_pool()
            await self.init_session()
            
            # 전체 배치 수 계산
            total_batches = 0
            batch_sizes = {}
            for table in ['tb24_200_corp_applicant', 'tb24_210_univ_applicant']:
                _, total_count = await self.get_applicant_numbers(table, 0)
                batch_count = -(-total_count // self.batch_size)
                total_batches += batch_count
                batch_sizes[table] = batch_count
            
            self.logger.info(f"Total batches to process: {total_batches}")
            
            # 기업과 대학 데이터 처리
            current_batch = 0
            for table, xml_builder in [
                ('tb24_200_corp_applicant', self.corp_xml_builder),
                ('tb24_210_univ_applicant', self.univ_xml_builder)
            ]:
                offset = 0
                while True:
                    applicant_numbers, total_count = await self.get_applicant_numbers(table, offset)
                    if not applicant_numbers:
                        break
                    
                    current_batch += 1
                    await self.process_applicants_batch(
                        applicant_numbers,
                        xml_builder,
                        current_batch,
                        total_batches
                    )
                    
                    # 진행상황 로깅
                    self.logger.info(
                        f"Progress: {table} - "
                        f"Processed {xml_builder.trademark_count} trademarks, "
                        f"Offset: {offset}/{total_count}"
                    )
                    
                    offset += self.batch_size
            
            # 실패한 요청 재시도
            if self.failed_requests:
                self.logger.info(f"Retrying {len(self.failed_requests)} failed requests...")
                await self.retry_failed_requests()
            
            # 최종 저장
            self.logger.info("Saving final results...")
            self.corp_xml_builder.save()
            self.univ_xml_builder.save()
            
            # 결과 요약
            duration = time.time() - start_time
            self.logger.info("\n=== Processing Summary ===")
            self.logger.info(f"Total processing time: {duration:.2f} seconds")
            self.logger.info(f"Corporate trademarks: {self.corp_xml_builder.trademark_count}")
            self.logger.info(f"University trademarks: {self.univ_xml_builder.trademark_count}")
            self.logger.info(f"Failed requests: {len(self.failed_requests)}")
            
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}", exc_info=True)
            raise
        finally:
            await self.cleanup()
            
    async def cleanup(self):
        if self.session and not self.session.closed:
            await self.session.close()
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

async def main():
    try:
        downloader = AsyncTrademarkDownloader()
        await downloader.process_all()
    except Exception as e:
        logging.error(f"Application failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())