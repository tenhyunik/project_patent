import pandas as pd
import requests
import xml.etree.ElementTree as ET
import time
from datetime import datetime
import logging
from typing import Dict, List, Optional
import os
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import backoff

class PatentAPIProcessor:
    def __init__(self, service_key: str, rate_limit: int = 50, max_retries: int = 3):
        """
        특허 API 처리기 초기화
        
        Args:
            service_key (str): API 서비스 키
            rate_limit (int): 초당 최대 요청 수
            max_retries (int): 최대 재시도 횟수
        """
        self.service_key = service_key
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.last_request_time = 0
        self.base_url = "http://plus.kipris.or.kr/kipo-api/kipi/patUtiModInfoSearchSevice/getAdvancedSearch"
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"PatentAPIProcessor 초기화 완료 (rate_limit: {rate_limit})")

    def _rate_limit_wait(self):
        """요청 속도 제한 처리"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_request_time
        if time_since_last_call < (1 / self.rate_limit):
            wait_time = (1 / self.rate_limit) - time_since_last_call
            self.logger.debug(f"Rate limit 대기: {wait_time:.2f}초")
            time.sleep(wait_time)
        self.last_request_time = time.time()

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, ET.ParseError),
        max_tries=3
    )
    def fetch_patent_data(self, application_number: str) -> Dict:
        """
        특허 데이터 조회
        
        Args:
            application_number (str): 출원번호
            
        Returns:
            Dict: 특허 정보
        """
        self._rate_limit_wait()
        
        params = {
            'applicationNumber': application_number,
            'ServiceKey': self.service_key
        }

        try:
            self.logger.debug(f"API 요청 시작: {application_number}")
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            items = root.findall('.//item')
            
            if not items:
                self.logger.warning(f"데이터 없음: {application_number}")
                return {}
            
            item = items[0]
            
            data = {
                'applicationNumber': application_number,
                'applicantName': item.findtext('applicantName', ''),
                'applicationDate': item.findtext('applicationDate', ''),
                'inventionTitle': item.findtext('inventionTitle', ''),
                'astrtCont': item.findtext('astrtCont', ''),
                'ipcNumber': item.findtext('ipcNumber', ''),
                'registerStatus': item.findtext('registerStatus', ''),
                'openDate': item.findtext('openDate', ''),
                'openNumber': item.findtext('openNumber', ''),
                'publicationDate': item.findtext('publicationDate', ''),
                'publicationNumber': item.findtext('publicationNumber', ''),
                'registerDate': item.findtext('registerDate', ''),
                'registerNumber': item.findtext('registerNumber', ''),
                'indexNo': item.findtext('indexNo', ''),
                'drawing': item.findtext('drawing', ''),
                'bigDrawing': item.findtext('bigDrawing', '')
            }
            
            return data
            
        except requests.RequestException as e:
            self.logger.error(f"API 요청 실패 ({application_number}): {str(e)}")
            raise
        except ET.ParseError as e:
            self.logger.error(f"XML 파싱 실패 ({application_number}): {str(e)}")
            raise

class PatentDataProcessor:
    def __init__(self, service_key: str, base_dir: Path, logger: logging.Logger):
        """
        특허 데이터 처리기 초기화
        
        Args:
            service_key (str): API 서비스 키
            base_dir (Path): 기본 디렉토리 경로
            logger (logging.Logger): 로거 인스턴스
        """
        self.api_processor = PatentAPIProcessor(service_key)
        self.base_dir = base_dir
        self.logger = logger

    def process_chunk(self, chunk: pd.DataFrame, output_path: Path) -> List[Dict]:
        """청크 단위 처리"""
        results = []
        for appl_no in chunk['appl_no']:
            data = self.api_processor.fetch_patent_data(str(appl_no))
            if data:
                results.append(data)
        return results

    def process_file(self, input_path: Path, output_path: Path, chunk_size: int = 100) -> None:
        """
        파일 단위 처리
        
        Args:
            input_path (Path): 입력 파일 경로
            output_path (Path): 출력 파일 경로
            chunk_size (int): 청크 크기
        """
        if not input_path.exists():
            self.logger.error(f"입력 파일 없음: {input_path}")
            return

        try:
            df = pd.read_csv(input_path)
            total_records = len(df)
            self.logger.info(f"처리 시작: 총 {total_records}건")

            all_results = []
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

            with ThreadPoolExecutor(max_workers=4) as executor:
                for chunk_results in executor.map(
                    lambda chunk: self.process_chunk(chunk, output_path),
                    chunks
                ):
                    all_results.extend(chunk_results)
                    
                    # 중간 저장
                    temp_df = pd.DataFrame(all_results)
                    temp_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                    self.logger.info(f"중간 저장 완료 ({len(all_results)}건): {output_path}")

            final_df = pd.DataFrame(all_results)
            final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"처리 완료. {len(all_results)}건 저장: {output_path}")

        except Exception as e:
            self.logger.error(f"파일 처리 실패: {str(e)}", exc_info=True)
            raise

def setup_logging(log_dir: Path) -> logging.Logger:
    """로깅 설정"""
    log_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger('patent_update_processing')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 파일 핸들러
    file_handler = logging.FileHandler(
        log_dir / f"patent_update_{datetime.now().strftime('%Y%m%d')}.log"
    )
    file_handler.setFormatter(formatter)
    
    # 스트림 핸들러
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    return logger

def main():
    # 환경 변수 로드
    load_dotenv()
    
    # 기본 설정
    log_dir = Path("/home/ubuntu/airflow/logs/update_processing")
    base_dir = Path("/home/ubuntu/call_api/data")
    today = datetime.now().strftime('%Y%m%d')
    
    # 로깅 설정
    logger = setup_logging(log_dir)
    
    try:
        # 환경 변수 검증
        service_key = os.getenv("KIPRIS_API_KEY")
        if not service_key:
            raise ValueError("KIPRIS_API_KEY 환경 변수가 설정되지 않았습니다.")
        
        # 디렉토리 생성
        base_dir.mkdir(parents=True, exist_ok=True)
        
        # 입출력 경로 설정
        input_paths = {
            "corp": base_dir / f"patent_corp_appl_no_{today}.csv",
            "univ": base_dir / f"patent_univ_appl_no_{today}.csv"
        }

        output_paths = {
            "corp": base_dir / f"patent_corp_update_{today}.csv",
            "univ": base_dir / f"patent_univ_update_{today}.csv"
        }
        
        # 데이터 처리기 생성
        processor = PatentDataProcessor(service_key, base_dir, logger)
        
        # 파일 처리
        for key, input_path in input_paths.items():
            logger.info(f"{key} 처리 시작")
            processor.process_file(input_path, output_paths[key])
            
        logger.info("모든 처리 완료")
        
    except Exception as e:
        logger.error("프로그램 실행 실패", exc_info=True)
        raise

if __name__ == "__main__":
    main()
