import os
import time
import aiomysql
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
from typing import Optional, List, Tuple, Dict, Any
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Summary, Gauge, Info
from urllib3.util.ssl_ import create_urllib3_context
from asyncio import Semaphore

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
        self.xml_processing_duration = Summary(
            'patent_xml_processing_duration_seconds', 
            'Time spent processing XML'
        )
        self.db_operation_duration = Summary(
            'patent_db_operation_duration_seconds', 
            'Time spent in database operations'
        )
        
        self.processing_progress = Gauge(
            'patent_processing_progress_percent', 
            'Current progress of patent processing'
        )
        self.active_connections = Gauge(
            'patent_active_connections',
            'Number of active connections'
        )
        self.api_total_duration = Gauge(
            'patent_api_total_duration_seconds',
            'Total time from start to finish of API processing'
        )
        self.memory_usage = Gauge(
            'patent_memory_usage_bytes',
            'Current memory usage'
        )

class TLSAdapter(aiohttp.TCPConnector):
    def __init__(self):
        ssl_context = create_urllib3_context(
            ciphers='ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20'
        )
        super().__init__(
            ssl=ssl_context,
            keepalive_timeout=60,
            limit=100,
            enable_cleanup_closed=True,
            force_close=False,
            limit_per_host=50
        )

class XMLBuilder:
    def __init__(self, filename: str):
        self.root = ET.Element("patentCorpData")
        self.items = ET.SubElement(self.root, "items")
        self.filename = filename
        self._lock = asyncio.Lock()
        
    async def add_patent_data(self, xml_string: str, applicant_no: str) -> bool:
        try:
            async with self._lock:
                source_root = ET.fromstring(xml_string)
                for source_item in source_root.findall('.//item'):
                    new_item = ET.SubElement(self.items, "item")
                    
                    applicant_no_elem = ET.SubElement(new_item, "applicantNo")
                    applicant_no_elem.text = applicant_no
                    
                    fields = [
                        'registerStatus', 'inventionTitle', 'ipcNumber', 
                        'registerNumber', 'registerDate', 'applicationNumber',
                        'applicationDate', 'openNumber', 'openDate',
                        'publicationNumber', 'publicationDate', 'drawing',
                        'applicantName', 'astrtCont'
                    ]
                    
                    for field in fields:
                        elem = source_item.find(field)
                        if elem is not None:
                            field_elem = ET.SubElement(new_item, field)
                            field_elem.text = elem.text
                            
                return True
        except ET.ParseError:
            return False
            
    def save(self):
        os.makedirs('data', exist_ok=True)
        filepath = os.path.join('data', self.filename)
        tree = ET.ElementTree(self.root)
        ET.indent(tree, space="  ", level=0)
        tree.write(filepath, encoding='utf-8', xml_declaration=True)

class RateLimiter:
    def __init__(self, rate_limit: int):
        self.rate_limit = rate_limit
        self.semaphore = Semaphore(rate_limit)

    async def acquire(self):
        await self.semaphore.acquire()
        try:
            await asyncio.sleep(1.0 / self.rate_limit)
        finally:
            self.semaphore.release()
        
    async def add_token(self):
        try:
            self.tokens.put_nowait(1)
        except asyncio.QueueFull:
            pass
            
    async def start(self):
        while True:
            await self.add_token()
            await asyncio.sleep(1.0 / self.rate_limit)

class AsyncPatentDownloader:
    def __init__(self):
        start_http_server(8000, addr='0.0.0.0')
        self.metrics = PatentMetrics()
        
        load_dotenv()
        self.validate_env()
        
        self.service_key = os.getenv('KIPRIS_API_KEY')
        self.batch_size = int(os.getenv('BATCH_SIZE'))
        self.base_url = "http://plus.kipris.or.kr/kipo-api/kipi/patUtiModInfoSearchSevice/getAdvancedSearch"
        
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'db': os.getenv('DB_NAME'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        
        self.session = None
        self.pool = None
        self.start_time = None
        
        date_str = datetime.now().strftime('%Y%m%d')
        self.corp_xml_builder = XMLBuilder(f'{date_str}_patent_utility_corp.xml')
        self.univ_xml_builder = XMLBuilder(f'{date_str}_patent_utility_univ.xml')
        
        self.rate_limiter = RateLimiter(50)
        
        self.setup_logging()

    def validate_env(self):
        required_vars = ['KIPRIS_API_KEY', 'DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'BATCH_SIZE']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('patent_download.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    async def init_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=3.05, sock_read=27)
            self.session = aiohttp.ClientSession(
                connector=TLSAdapter(),
                timeout=timeout,
                headers={
                    'Connection': 'keep-alive',
                    'Keep-Alive': 'timeout=60, max=1000',
                    'User-Agent': 'PatentDownloader/1.0'
                }
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
        with self.metrics.db_operation_duration.time():
            if self.pool is None:
                await self.init_db_pool()

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        SELECT COUNT(DISTINCT applicant_no) 
                        FROM {table}
                        WHERE applicant_no IS NOT NULL
                    """)
                    total_count = (await cursor.fetchone())[0]
                    
                    await cursor.execute(f"""
                        SELECT DISTINCT applicant_no 
                        FROM {table}
                        WHERE applicant_no IS NOT NULL
                        LIMIT %s OFFSET %s
                    """, (self.batch_size, offset))
                    
                    results = await cursor.fetchall()
                    return [row[0] for row in results], total_count

    async def fetch_patent_data(self, applicant_no: str, page: int = 1) -> Optional[str]:
        with self.metrics.api_request_duration.time():
            if self.session is None or self.session.closed:
                await self.init_session()
            
            self.metrics.api_requests.inc()
            self.metrics.active_connections.inc()
            
            try:
                await self.rate_limiter.acquire()
                params = {
                    'applicant': applicant_no,
                    'ServiceKey': self.service_key,
                    'pageNo': page,
                    'numOfRows': 500
                }
                
                async with self.session.get(self.base_url, params=params) as response:
                    if response.status != 200:
                        self.logger.error(f"API request failed for {applicant_no}: {response.status}")
                        self.metrics.api_errors.inc()
                        return None
                            
                    return await response.text()
                    
            except Exception as e:
                self.logger.error(f"API request error for {applicant_no}: {e}")
                self.metrics.api_errors.inc()
                return None
            finally:
                self.metrics.active_connections.dec()

    async def process_single_applicant(self, applicant_no: str, xml_builder: XMLBuilder):
        page = 1
        total_processed = 0
        
        while True:
            try:
                xml_content = await self.fetch_patent_data(applicant_no, page)
                if not xml_content:
                    self.logger.error(f"Failed to fetch data for applicant {applicant_no} at page {page}")
                    break

                try:
                    root = ET.fromstring(xml_content)
                    count_elem = root.find('.//count')
                    
                    if count_elem is not None:
                        total_count = int(count_elem.find('totalCount').text)
                        num_of_rows = int(count_elem.find('numOfRows').text)
                        current_rows = len(root.findall('.//item'))
                        
                        self.logger.info(f"Processing applicant {applicant_no} - Page {page}/{-(-total_count // num_of_rows)} " + 
                                    f"(Total: {total_count}, Current: {total_processed + current_rows})")
                        
                        if await xml_builder.add_patent_data(xml_content, applicant_no):
                            self.metrics.successful_downloads.inc()
                            total_processed += current_rows
                            
                            if current_rows == 0 or total_processed >= total_count:
                                self.logger.info(f"Completed processing applicant {applicant_no} - " +
                                            f"Total processed: {total_processed}/{total_count}")
                                break
                        else:
                            self.metrics.xml_parse_errors.inc()
                            self.logger.error(f"Failed to parse XML for applicant {applicant_no} at page {page}")
                    else:
                        items = root.findall('.//item')
                        if items:
                            self.logger.warning(f"No count information found for applicant {applicant_no}, but found {len(items)} items")
                            if await xml_builder.add_patent_data(xml_content, applicant_no):
                                self.metrics.successful_downloads.inc()
                                break
                        else:
                            self.logger.info(f"No data found for applicant {applicant_no}")
                            break
                            
                    page += 1
                    
                except ET.ParseError as e:
                    self.logger.error(f"XML parsing error for applicant {applicant_no}: {e}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Error processing applicant {applicant_no} at page {page}: {e}")
                break

    async def process_applicants_batch(self, applicant_numbers: List[str], xml_builder: XMLBuilder):
        tasks = [self.process_single_applicant(applicant_no, xml_builder) 
                for applicant_no in applicant_numbers]
        await asyncio.gather(*tasks)

    async def process_all(self):
        try:
            self.start_time = time.time()
            
            asyncio.create_task(self.rate_limiter.start())
            
            await self.init_db_pool()
            await self.init_session()
            
            offset = 0
            while True:
                applicant_numbers, total_count = await self.get_applicant_numbers('tb24_200_corp_applicant', offset)
                if not applicant_numbers:
                    break
                    
                await self.process_applicants_batch(applicant_numbers, self.corp_xml_builder)
                offset += self.batch_size
            
            offset = 0
            while True:
                applicant_numbers, total_count = await self.get_applicant_numbers('tb24_210_univ_applicant', offset)
                if not applicant_numbers:
                    break
                    
                await self.process_applicants_batch(applicant_numbers, self.univ_xml_builder)
                offset += self.batch_size
            
            self.corp_xml_builder.save()
            self.univ_xml_builder.save()
            
            final_duration = time.time() - self.start_time
            self.metrics.api_total_duration.set(final_duration)
            
            self.logger.info(f"All processing completed. Total time: {final_duration:.2f} seconds")
            
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
        downloader = AsyncPatentDownloader()
        await downloader.process_all()
    except Exception as e:
        logging.error(f"Application failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())