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
    def __init__(self, max_concurrent: int = 10):
        self.semaphore = Semaphore(max_concurrent)

    async def __aenter__(self):
        await self.semaphore.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.semaphore.release()
        await asyncio.sleep(0.1)  # 개별 요청 간 간격

class DesignMetrics:
    def __init__(self):
        self.api_requests = Counter(
            'design_api_requests_total', 
            'Total number of API requests made'
        )
        self.api_errors = Counter(
            'design_api_errors_total', 
            'Total number of API request errors'
        )
        self.xml_parse_errors = Counter(
            'design_xml_parse_errors_total', 
            'Total number of XML parsing errors'
        )
        self.successful_downloads = Counter(
            'design_successful_downloads_total', 
            'Total number of successfully downloaded designs'
        )
        
        self.api_request_duration = Summary(
            'design_api_request_duration_seconds', 
            'Time spent in API requests'
        )
        self.api_total_duration = Summary(
            'design_api_total_duration_seconds',
            'Total time from start to finish of API processing'
        )

class XMLBuilder:
    def __init__(self, filename: str):
        self.root = ET.Element("designInfoData")
        self.items = ET.SubElement(self.root, "items")
        self.filename = filename
        self._lock = asyncio.Lock()
        self.design_count = 0
        
    async def add_design_data(self, xml_string: str, applicant_no: str) -> bool:
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
                        'number',  # 일련번호
                        'articleName',  # 디자인의 대상이 되는 물품
                        'applicantName',  # 출원인 이름
                        'inventorName',  # 창작자 이름
                        'agentName',  # 대리인 이름
                        'applicationNumber',  # 출원번호
                        'applicationDate',  # 출원일자
                        'openNumber',  # 공개번호
                        'openDate',  # 공개일자
                        'registrationNumber',  # 등록번호
                        'registrationDate',  # 등록일자
                        'publicationNumber',  # 공고번호
                        'publicationDate',  # 공고일자
                        'applicationStatus',  # 법적상태
                        'priorityNumber',  # 우선권번호
                        'priorityDate',  # 우선권주장일자
                        'designMainClassification',  # 디자인분류(국제분류)
                        'dsShpClssCd',  # 형태분류코드
                        'fullText'  # 전문 제공 여부
                    ]
                    
                    # 각 필드에 대해 값이 있는 경우에만 추가
                    for field in fields:
                        elem = source_item.find(field)
                        if elem is not None:
                            field_elem = ET.SubElement(new_item, field)
                            field_elem.text = elem.text
                            
                    self.design_count += 1
                    
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

class AsyncDesignDownloader:
    def __init__(self):
        start_http_server(8001, addr='0.0.0.0')
        self.metrics = DesignMetrics()
        
        load_dotenv()
        self.validate_env()
        
        self.service_key = os.getenv('KIPRIS_API_KEY')
        self.batch_size = int(os.getenv('BATCH_SIZE'))
        self.base_url = "http://plus.kipris.or.kr/kipo-api/kipi/designInfoSearchService/getAdvancedSearch"
        
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
        self.corp_xml_builder = XMLBuilder(f'{date_str}_design_corp.xml')
        self.univ_xml_builder = XMLBuilder(f'{date_str}_design_univ.xml')
        self.rate_limiter = RateLimiter()
        
        self.setup_logging()
        self.failed_requests = []

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
                logging.FileHandler('design_download.log'),
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
                headers={'User-Agent': 'DesignDownloader/1.0'}
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

    async def fetch_design_data(self, applicant_no: str, page: int = 1) -> Optional[str]:
        async with self.rate_limiter:
            try:
                async with self.session.get(self.base_url, params={
                    'applicantName': applicant_no,
                    'ServiceKey': self.service_key,
                    'pageNo': str(page),
                    'numOfRows': '500',
                    'etc': 'true',
                    'part': 'true',
                    'simi': 'true',
                    'open': 'true',
                    'rejection': 'true',
                    'destroy': 'true',
                    'cancle': 'true',
                    'notice': 'true',
                    'registration': 'true',
                    'invalid': 'true',
                    'abandonment': 'true'
                }) as response:
                    response_text = await response.text()
                    
                    if "Repetition Request ERROR" in response_text:
                        self.logger.error(f"Rate limit exceeded: {applicant_no}")
                        await asyncio.sleep(2)
                        return None
                        
                    if response.status != 200:
                        self.logger.error(f"API request failed: {applicant_no}")
                        return None
                            
                    return response_text
                    
            except Exception as e:
                self.logger.error(f"API error: {applicant_no} - {str(e)}")
                return None

    async def process_single_applicant(self, applicant_no: str, xml_builder: XMLBuilder, is_retry: bool = False):
        page = 1
        total_processed = 0
        
        while True:
            try:
                xml_content = await self.fetch_design_data(applicant_no, page)
                if not xml_content:
                    if not is_retry:
                        self.failed_requests.append((applicant_no, xml_builder))
                    break

                try:
                    root = ET.fromstring(xml_content)
                    items = root.findall('.//item')
                    
                    if items:
                        if await xml_builder.add_design_data(xml_content, applicant_no):
                            total_processed += len(items)
                            self.logger.info(f"Progress: {applicant_no} - {total_processed} designs saved")
                        else:
                            break
                    else:
                        break
                    
                    count_elem = root.find('.//count/totalCount')
                    if count_elem is not None and total_processed >= int(count_elem.text):
                        break
                        
                    page += 1
                    
                except ET.ParseError as e:
                    self.logger.error(f"XML parsing error: {applicant_no} - {e}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error processing applicant {applicant_no}: {e}")
                break

    async def retry_failed_requests(self):
        if not self.failed_requests:
            return

        self.logger.info(f"Retrying {len(self.failed_requests)} failed requests...")
        
        failed_requests = self.failed_requests.copy()
        self.failed_requests = []
        
        tasks = [
            self.process_single_applicant(applicant_no, xml_builder, is_retry=True)
            for applicant_no, xml_builder in failed_requests
        ]
        await asyncio.gather(*tasks)
        
        if self.failed_requests:
            self.logger.error(f"Still failed after retry: {len(self.failed_requests)} requests")
            os.makedirs('data', exist_ok=True)
            date_str = datetime.now().strftime('%Y%m%d')
            failed_requests_file = os.path.join('data', f'failed_requests_{date_str}.txt')
            with open(failed_requests_file, 'w') as f:
                for applicant_no, _ in self.failed_requests:
                    f.write(f"{applicant_no}\n")
            self.logger.warning(f"Failed requests have been saved to '{failed_requests_file}'")

    async def process_applicants_batch(self, applicant_numbers: List[str], xml_builder: XMLBuilder):
        chunk_size = 20
        for i in range(0, len(applicant_numbers), chunk_size):
            chunk = applicant_numbers[i:i + chunk_size]
            tasks = [
                self.process_single_applicant(applicant_no, xml_builder)
                for applicant_no in chunk
            ]
            await asyncio.gather(*tasks)

    async def process_all(self):
        try:
            start_time = time.time()
            await self.init_db_pool()
            await self.init_session()
            
            # 기업과 대학 동시 처리
            tasks = []
            for table, xml_builder in [
                ('tb24_200_corp_applicant', self.corp_xml_builder),
                ('tb24_210_univ_applicant', self.univ_xml_builder)
            ]:
                offset = 0
                while True:
                    applicant_numbers, total_count = await self.get_applicant_numbers(table, offset)
                    if not applicant_numbers:
                        break
                    
                    tasks.append(self.process_applicants_batch(applicant_numbers, xml_builder))
                    offset += self.batch_size
            
            # 모든 태스크 동시 실행
            await asyncio.gather(*tasks)
            
            # 실패한 요청 재시도
            await self.retry_failed_requests()
            
            # XML 파일 저장
            self.corp_xml_builder.save()
            self.univ_xml_builder.save()
            
            duration = time.time() - start_time
            self.metrics.api_total_duration.observe(duration)
            
            self.logger.info("=== Processing Summary ===")
            self.logger.info(f"Total processing time: {duration:.2f} seconds")
            self.logger.info(f"Total designs - Corporate: {self.corp_xml_builder.design_count}, University: {self.univ_xml_builder.design_count}")
            
            if self.failed_requests:
                self.logger.warning(f"Failed requests count: {len(self.failed_requests)}")
                self.logger.warning("Failed requests have been saved to failed_requests.txt")
            
        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            raise
            
        finally:
            if self.session and not self.session.closed:
                await self.session.close()
            if self.pool:
                self.pool.close()
                await self.pool.wait_closed()

async def main():
    try:
        downloader = AsyncDesignDownloader()
        await downloader.process_all()
    except Exception as e:
        logging.error(f"Application failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())