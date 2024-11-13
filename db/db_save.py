import pandas as pd
import logging
import os
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv
from typing import Dict
from pathlib import Path
import sys


BASE_DIR = Path("/home/ubuntu/call_api")
DATA_DIR = BASE_DIR / "data"
LOG_DIR = Path("/home/ubuntu/airflow/logs/db_save")

def get_env_var(var_name):
    """환경 변수 조회 함수"""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value

class IPRDataMapper:
    def __init__(self):
        """데이터베이스 연결 및 매핑 정보 초기화"""
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME')
        }
        
        # 필드 매핑 정보 정의
        self.field_mappings = {
            'patent': {
                'applicant': 'applicantName',
                'main_ipc': 'ipcNumber',
                'appl_no': 'applicationNumber',
                'appl_date': 'applicationDate',
                'open_no': 'openNumber',
                'open_date': 'openDate',
                'reg_no': 'registerNumber',
                'reg_date': 'registerDate',
                'pub_no': 'publicationNumber',
                'pub_date': 'publicationDate',
                'legal_status_desc': 'registerStatus',
                'img_url': 'drawing',
                'abstract': 'astrtCont',
                'title': 'inventionTitle'
            },
            'design': {
                'applicant': 'applicantName',
                'inventor': 'inventorName',
                'agent': 'agentName',
                'appl_no': 'applicationNumber',
                'appl_date': 'applicationDate',
                'open_no': 'openNumber',
                'open_date': 'openDate',
                'reg_no': 'registrationNumber',
                'reg_date': 'registrationDate',
                'pub_no': 'publicationNumber',
                'pub_date': 'publicationDate',
                'legal_status_desc': 'applicationStatus',
                'img_url': 'imagePath',
                'title': 'articleName'
            },
            'trademark': {
                'applicant': 'ApplicantName',
                'agent': 'AgentName',
                'appl_no': 'ApplicationNumber',
                'appl_date': 'ApplicationDate',
                'reg_no': 'RegistrationNumber',
                'reg_date': 'RegistrationDate',
                'pub_no': 'PublicNumber',
                'pub_date': 'PublicDate',
                'legal_status_desc': 'ApplicationStatus',
                'img_url': 'ThumbnailPath',
                'title': 'Title'
            }
        }

    def map_and_save_data(self, csv_file: str, table_name: str, ipr_type: str):
        """CSV 데이터를 매핑하여 데이터베이스에 업데이트"""
        conn = None
        cursor = None
        batch_size = 1000
        update_count = 0
        insert_count = 0
        
        try:
            if not os.path.exists(csv_file):
                logger.warning(f"파일이 없음: {csv_file}")
                return
                
            if os.path.getsize(csv_file) == 0:
                logger.warning(f"빈 파일: {csv_file}")
                return
                
            logger.info(f"처리 시작: {csv_file} -> {table_name}")
            
            try:
                df = pd.read_csv(csv_file)
                logger.info(f"CSV 컬럼: {df.columns.tolist()}")
                if df.empty:
                    logger.warning(f"데이터가 없음: {csv_file}")
                    return
            except pd.errors.EmptyDataError:
                logger.warning(f"빈 CSV 파일: {csv_file}")
                return
            except Exception as e:
                logger.error(f"CSV 파일 읽기 오류 ({csv_file}): {str(e)}")
                return
            
            mapping = self.field_mappings[ipr_type]
            logger.info(f"사용할 매핑: {mapping}")
            
            # 컬럼 길이 제한 설정
            column_lengths = {
                'applicant': 500,
                'inventor': 500,
                'agent': 500,
                'main_ipc': 500,
                'appl_no': 20,
                'appl_date': 40,
                'open_no': 20,
                'open_date': 8,
                'reg_no': 20,
                'reg_date': 40,
                'pub_no': 20,
                'pub_date': 8,
                'legal_status_desc': 20,
                'img_url': 500,
                'abstract': 2000,
                'title': 500
            }
            
            # CSV 컬럼 검증
            csv_columns = set(df.columns)
            missing_columns = [file_field for file_field in mapping.values() if file_field not in csv_columns]
            
            if missing_columns:
                logger.error(f"CSV 파일에 필요한 컬럼이 없음: {missing_columns}")
                logger.error(f"사용 가능한 컬럼: {csv_columns}")
                return
            
            conn = self.connect_db()
            cursor = conn.cursor()
            
            # UPDATE와 INSERT 쿼리 준비
            update_fields = []
            update_params_order = []
            
            for key in mapping.keys():
                if key != 'appl_no':
                    update_fields.append(f"{key} = %s")
                    update_params_order.append(key)
            
            update_query = f"""
                UPDATE {table_name} 
                SET {', '.join(update_fields)}, update_date = NOW()
                WHERE appl_no = %s
            """
            
            insert_fields = list(mapping.keys())
            insert_params = ', '.join(['%s'] * (len(insert_fields) + 1))  # +1 for update_date
            
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(insert_fields)}, update_date)
                VALUES ({insert_params})
            """
            
            # 기존 데이터 확인
            appl_nos = tuple(df[mapping['appl_no']].astype(str).tolist())
            if not appl_nos:
                logger.warning("처리할 데이터가 없습니다.")
                return
                
            placeholders = ','.join(['%s'] * len(appl_nos))
            existing_query = f"""
                SELECT appl_no 
                FROM {table_name} 
                WHERE appl_no IN ({placeholders})
            """
            cursor.execute(existing_query, appl_nos)
            existing_appl_nos = {str(row[0]) for row in cursor.fetchall()}
            
            # 데이터 처리
            updates = []
            inserts = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for _, row in df.iterrows():
                record = {}
                for table_field, file_field in mapping.items():
                    value = row.get(file_field, '')
                    
                    # 값 형식 처리
                    if table_field.endswith('_date'):
                        value = self.format_date(value)
                    elif table_field == 'main_ipc':
                        value = self.get_first_ipc(value, column_lengths[table_field])
                    elif table_field in column_lengths:
                        if value is not None:
                            value = self.truncate_value(str(value), column_lengths[table_field])
                    
                    record[table_field] = value
                
                appl_no = str(row[mapping['appl_no']])
                
                if appl_no in existing_appl_nos:
                    update_data = [record[field] for field in update_params_order]
                    update_data.append(appl_no)
                    updates.append(update_data)
                else:
                    insert_data = [record[field] for field in mapping.keys()]
                    insert_data.append(current_time)
                    inserts.append(insert_data)
            
            try:
                # UPDATE 처리
                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i+batch_size]
                    cursor.executemany(update_query, batch)
                    update_count += len(batch)
                    conn.commit()
                    logger.info(f"UPDATE 처리: {i+1}~{min(i+batch_size, len(updates))} / {len(updates)}")
                
                # INSERT 처리
                for i in range(0, len(inserts), batch_size):
                    batch = inserts[i:i+batch_size]
                    cursor.executemany(insert_query, batch)
                    insert_count += len(batch)
                    conn.commit()
                    logger.info(f"INSERT 처리: {i+1}~{min(i+batch_size, len(inserts))} / {len(inserts)}")
                    
            except mysql.connector.Error as e:
                logger.error(f"데이터베이스 작업 중 오류: {str(e)}")
                logger.error(f"UPDATE 쿼리: {update_query}")
                logger.error(f"INSERT 쿼리: {insert_query}")
                if updates:
                    logger.error(f"UPDATE 데이터 샘플: {updates[0]}")
                if inserts:
                    logger.error(f"INSERT 데이터 샘플: {inserts[0]}")
                raise
                
            logger.info(f"처리 완료 - 업데이트: {update_count}건, 신규: {insert_count}건")
            
        except Exception as e:
            logger.error(f"데이터 처리 중 오류 발생: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def format_date(self, value) -> str:
        """날짜 형식을 YYYYMMDD 형태로 변환"""
        if not value:
            return None
        try:
            value = str(value).replace('.', '').replace('-', '').strip()
            if len(value) >= 8:
                return value[:8]
            return None
        except:
            return None

    def get_first_ipc(self, ipc_str: str, max_length: int) -> str:
        """IPC 코드 중 첫 번째 값만 반환"""
        if not ipc_str:
            return None
        try:
            first_ipc = str(ipc_str).split('|')[0].strip()
            return self.truncate_value(first_ipc, max_length)
        except:
            return self.truncate_value(str(ipc_str), max_length)

    def truncate_value(self, value: str, max_length: int) -> str:
        """값을 지정된 길이로 자르기"""
        if value and isinstance(value, str) and len(value) > max_length:
            return value[:max_length]
        return value

    def check_table_structure(self, table_name: str):
        """테이블 구조 확인"""
        conn = None
        cursor = None
        
        try:
            conn = self.connect_db()
            if not conn:
                logger.error("데이터베이스 연결 실패")
                return False
                
            cursor = conn.cursor()
            if not cursor:
                logger.error("커서 생성 실패")
                return False
            
            # INDEX 확인
            cursor.execute(f"""
                SELECT INDEX_NAME 
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_NAME = '{table_name}'
                AND COLUMN_NAME = 'appl_no'
            """)
            indexes = cursor.fetchall()
            
            if not indexes:
                logger.warning(f"{table_name}에 appl_no INDEX가 없습니다.")
                logger.warning(f"INDEX 생성 권장: ALTER TABLE {table_name} ADD INDEX idx_appl_no (appl_no);")
            
            # 중복 데이터 확인
            try:
                cursor.execute(f"""
                    SELECT appl_no, COUNT(*) as cnt
                    FROM {table_name}
                    GROUP BY appl_no
                    HAVING cnt > 1
                    ORDER BY cnt DESC
                    LIMIT 5
                """)
                duplicates = cursor.fetchall()
                
                if duplicates:
                    logger.warning(f"{table_name} 중복 appl_no 상위 5개:")
                    for appl_no, cnt in duplicates:
                        logger.warning(f"appl_no: {appl_no}, 중복 수: {cnt}")
            except mysql.connector.Error as e:
                logger.error(f"중복 데이터 확인 중 오류: {str(e)}")
                    
            # 필요한 컬럼들
            required_fields = set([
                'applicant', 'main_ipc', 'appl_no', 'appl_date', 
                'open_no', 'open_date', 'reg_no', 'reg_date', 
                'pub_no', 'pub_date', 'legal_status_desc', 
                'img_url', 'abstract', 'title', 'update_date'
            ])
            
            # 컬럼 확인
            cursor.execute(f"DESCRIBE {table_name}")
            existing_columns = {row[0] for row in cursor.fetchall()}
            
            missing_columns = required_fields - existing_columns
            if missing_columns:
                logger.warning(f"{table_name}에 없는 필수 컬럼: {missing_columns}")
                
                # 필요한 컬럼 타입 정의
                column_definitions = {
                    'applicant': 'VARCHAR(500)',
                    'main_ipc': 'VARCHAR(500)',
                    'appl_no': 'VARCHAR(20)',
                    'appl_date': 'VARCHAR(40)',
                    'open_no': 'VARCHAR(20)',
                    'open_date': 'VARCHAR(8)',
                    'reg_no': 'VARCHAR(20)',
                    'reg_date': 'VARCHAR(40)',
                    'pub_no': 'VARCHAR(20)',
                    'pub_date': 'VARCHAR(8)',
                    'legal_status_desc': 'VARCHAR(20)',
                    'img_url': 'VARCHAR(500)',
                    'abstract': 'TEXT',
                    'title': 'VARCHAR(500)',
                    'update_date': 'DATETIME'
                }
                
                # 없는 컬럼 추가
                for column in missing_columns:
                    if column in column_definitions:
                        try:
                            alter_query = f"""
                                ALTER TABLE {table_name}
                                ADD COLUMN {column} {column_definitions[column]}
                            """
                            cursor.execute(alter_query)
                            conn.commit()
                            logger.info(f"컬럼 추가됨: {column} ({column_definitions[column]})")
                        except Exception as e:
                            logger.error(f"컬럼 추가 실패 ({column}): {str(e)}")
            
            return True
                            
        except Exception as e:
            logger.error(f"테이블 구조 확인 중 오류: {str(e)}")
            return False
            
        finally:
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            except Exception as e:
                logger.error(f"리소스 정리 중 오류: {str(e)}")

    def connect_db(self):
        """데이터베이스 연결"""
        try:
            return mysql.connector.connect(**self.db_config)
        except Exception as e:
            logger.error(f"데이터베이스 연결 오류: {str(e)}")
            raise

    # ... [기존의 다른 메소드들은 그대로 유지]

def setup_logger():
    """로거 설정"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    log_file = LOG_DIR / f"db_save_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('db_save_processing')

def main():
    """
    Airflow task에서 실행될 메인 함수
    """
    global logger
    logger = setup_logger()
    
    try:
        logger.info("=== 데이터베이스 저장 프로세스 시작 ===")
        start_time = datetime.now()
        
        # 환경변수 로드
        load_dotenv(BASE_DIR / '.env')
        
        if not DATA_DIR.exists():
            raise FileNotFoundError(f"데이터 디렉토리가 존재하지 않습니다: {DATA_DIR}")
        
        mapper = IPRDataMapper()
        current_date = datetime.now().strftime('%Y%m%d')
        
        # 테이블 구조 확인
        mapper.check_table_structure('tb24_300_corp_ipr_reg')
        mapper.check_table_structure('tb24_400_univ_ipr_reg')
        
        file_configs = [
            # 기업 데이터
            {
                'csv_file': DATA_DIR / f'patent_corp_update_{current_date}.csv',
                'table_name': 'tb24_300_corp_ipr_reg',
                'ipr_type': 'patent'
            },
            {
                'csv_file': DATA_DIR / f'design_corp_update_{current_date}.csv',
                'table_name': 'tb24_300_corp_ipr_reg',
                'ipr_type': 'design'
            },
            {
                'csv_file': DATA_DIR / f'trademark_corp_update_{current_date}.csv',
                'table_name': 'tb24_300_corp_ipr_reg',
                'ipr_type': 'trademark'
            },
            # 대학 데이터
            {
                'csv_file': DATA_DIR / f'patent_univ_update_{current_date}.csv',
                'table_name': 'tb24_400_univ_ipr_reg',
                'ipr_type': 'patent'
            },
            {
                'csv_file': DATA_DIR / f'design_univ_update_{current_date}.csv',
                'table_name': 'tb24_400_univ_ipr_reg',
                'ipr_type': 'design'
            },
            {
                'csv_file': DATA_DIR / f'trademark_univ_update_{current_date}.csv',
                'table_name': 'tb24_400_univ_ipr_reg',
                'ipr_type': 'trademark'
            }
        ]
        
        processed_count = 0
        for config in file_configs:
            try:
                if config['csv_file'].exists():
                    logger.info(f"처리 시작: {config['csv_file']}")
                    mapper.map_and_save_data(
                        csv_file=str(config['csv_file']),
                        table_name=config['table_name'],
                        ipr_type=config['ipr_type']
                    )
                    processed_count += 1
                else:
                    logger.warning(f"파일을 찾을 수 없음: {config['csv_file']}")
            except Exception as e:
                logger.error(f"파일 처리 중 오류 발생 ({config['csv_file']}): {str(e)}")
                raise
        
        end_time = datetime.now()
        execution_time = end_time - start_time
        logger.info(f"총 {len(file_configs)}개 중 {processed_count}개 파일 처리 완료")
        logger.info(f"=== 모든 처리 완료 (소요시간: {execution_time}) ===")
        
        return f"DB 저장 완료: {processed_count}개 파일"
        
    except Exception as e:
        logger.error(f"전체 처리 중 오류 발생: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"프로그램 실행 중 오류 발생: {str(e)}")
        sys.exit(1)