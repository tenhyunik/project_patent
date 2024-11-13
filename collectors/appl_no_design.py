import os
import requests
import csv
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
import time
import logging
from pathlib import Path

# 로깅 설정 수정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console handler
        logging.FileHandler(  # File handler
            filename=f"/home/ubuntu/airflow/logs/design_processing_{datetime.now().strftime('%Y%m%d')}.log"
        )
    ]
)
logger = logging.getLogger('design_processing')

class APIRateLimiter:
    def __init__(self, calls_per_second=50):
        self.calls_per_second = calls_per_second
        self.last_call = 0
        
    def wait_if_needed(self):
        """API 호출 간격 제어"""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call
        if time_since_last_call < (1 / self.calls_per_second):
            time.sleep((1 / self.calls_per_second) - time_since_last_call)
        self.last_call = time.time()

def get_db_connection(db_config):
    """데이터베이스 연결 생성"""
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as e:
        logger.error(f"데이터베이스 연결 실패: {e}")
        raise

def fetch_api_data(url, rate_limiter):
    """API 데이터 조회"""
    try:
        rate_limiter.wait_if_needed()
        response = requests.get(url)
        response.raise_for_status()
        return response.text.strip().split("\n")
    except requests.RequestException as e:
        logger.error(f"API 요청 실패: {e}")
        raise

def save_to_csv(data_set, file_path):
    """CSV 파일로 데이터 저장"""
    try:
        with open(file_path, mode="w", newline="", encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["appl_no"])
            for appl_no in data_set:
                writer.writerow([appl_no])
        logger.info(f"CSV 파일 저장 완료: {file_path}")
    except IOError as e:
        logger.error(f"CSV 파일 저장 실패: {e}")
        raise

def get_env_var(var_name):
    """환경 변수 조회 함수"""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value

def main():
    try:
        # 환경 변수 로드
        load_dotenv()

        # 필수 환경 변수 확인
        db_config = {
            'host': get_env_var("DB_HOST"),
            'user': get_env_var("DB_USER"),
            'password': get_env_var("DB_PASSWORD"),
            'database': get_env_var("DB_NAME")
        }
        
        SERVICE_KEY = get_env_var("KIPRIS_API_KEY")
        BASE_URL = "http://plus.kipris.or.kr/kipo-api/kipi/designInfoSearchService/getChangeInfoSearch"
        
        # 속도 제한 설정
        rate_limiter = APIRateLimiter(calls_per_second=50)

        # 오늘 날짜 형식 설정
        today_date = datetime.now().strftime("%Y%m%d")

        # 출력 디렉토리 설정 (절대 경로 사용)
        output_dir = Path("/home/ubuntu/call_api/data")
        output_dir.mkdir(parents=True, exist_ok=True)

        connection = None
        cursor = None

        try:
            # API 요청 URL 구성
            url = f"{BASE_URL}?date={today_date}&ServiceKey={SERVICE_KEY}"
            logger.info(f"API 요청 시작: {url}")
            
            # API 데이터 조회
            data = fetch_api_data(url, rate_limiter)
            
            # transListString 데이터 추출 및 처리
            trans_list_string = data[-1] if data else ""
            appl_no_list = set(trans_list_string.split("|"))
            
            # 빈 문자열 제거
            appl_no_list = {x for x in appl_no_list if x.strip()}
            
            if not appl_no_list:
                logger.warning("API에서 받아온 출원번호가 없습니다.")
                return

            # 데이터베이스 연결
            connection = get_db_connection(db_config)
            cursor = connection.cursor()

            # 기업 출원번호 조회
            cursor.execute("SELECT appl_no FROM tb24_300_corp_ipr_reg")
            corp_appl_no_set = {row[0] for row in cursor.fetchall()}

            # 대학 출원번호 조회
            cursor.execute("SELECT appl_no FROM tb24_400_univ_ipr_reg")
            univ_appl_no_set = {row[0] for row in cursor.fetchall()}

            # 매칭 데이터 찾기
            matching_corp_appl_no = appl_no_list.intersection(corp_appl_no_set)
            matching_univ_appl_no = appl_no_list.intersection(univ_appl_no_set)

            # CSV 파일 경로 설정
            corp_csv_path = output_dir / f"design_corp_appl_no_{today_date}.csv"
            univ_csv_path = output_dir / f"design_univ_appl_no_{today_date}.csv"

            # CSV 파일 저장
            save_to_csv(matching_corp_appl_no, corp_csv_path)
            save_to_csv(matching_univ_appl_no, univ_csv_path)

            logger.info(f"처리된 기업 출원번호 수: {len(matching_corp_appl_no)}")
            logger.info(f"처리된 대학 출원번호 수: {len(matching_univ_appl_no)}")

        except Exception as e:
            logger.error(f"데이터 처리 중 오류 발생: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                logger.info("데이터베이스 연결 종료")

    except Exception as e:
        logger.error(f"프로그램 실행 중 치명적 오류 발생: {e}")
        raise

if __name__ == "__main__":
    main()