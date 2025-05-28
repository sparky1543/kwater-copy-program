import sqlite3
import datetime
import os


class DatabaseManager:
    """SQLite 데이터베이스 연결 및 쿼리 관련 기능을 제공하는 클래스"""
    
    def __init__(self, db_path="data.db"):
        """데이터베이스 매니저 초기화"""
        self.db_path = db_path
        # 데이터베이스 초기화
        self.initialize_database()

    def initialize_database(self):
        """데이터베이스 및 테이블 초기화"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # TABLE_INFO 테이블 생성
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS TABLE_INFO (
                    TABLE_NM TEXT PRIMARY KEY,
                    TABLE_DC TEXT,
                    TABLE_OWNERSHIP TEXT
                )
            ''')
            
            # FILE_INFO 테이블 생성 (INSERT_YN -> COPY_YN으로 변경)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS FILE_INFO (
                    TABLE_NM TEXT NOT NULL,
                    FILE_NM TEXT PRIMARY KEY,
                    COPY_YN TEXT,
                    DELETE_YN TEXT,
                    FOREIGN KEY (TABLE_NM) REFERENCES TABLE_INFO (TABLE_NM)
                )
            ''')
            
            # AUTO_CONFIG 테이블 생성 (DELETE_INTERVAL 제거, TABLE_NM을 PRIMARY KEY로 설정)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS AUTO_CONFIG (
                    TABLE_NM TEXT PRIMARY KEY,
                    SRC_PATH TEXT,
                    DEST_PATH TEXT,
                    AUTO_INTERVAL INTEGER,
                    LAST_TIMESTAMP TEXT,
                    USE_YN TEXT,
                    FOREIGN KEY (TABLE_NM) REFERENCES TABLE_INFO (TABLE_NM)
                )
            ''')
            
            # COL_MAPPING 테이블 생성
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS COL_MAPPING (
                    TABLE_NM TEXT NOT NULL,
                    DB_COL_NM TEXT NOT NULL,
                    XML_COL_NM TEXT NOT NULL,
                    FOREIGN KEY (TABLE_NM) REFERENCES TABLE_INFO (TABLE_NM)
                )
            ''')
            
            # TASK_LOG 테이블 생성 (INSERT_CNT 제거)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS TASK_LOG (
                    TABLE_NM TEXT NOT NULL,
                    FILE_NM TEXT NOT NULL,
                    START_TIME TEXT,
                    END_TIME TEXT,
                    FOREIGN KEY (TABLE_NM) REFERENCES TABLE_INFO (TABLE_NM),
                    FOREIGN KEY (FILE_NM) REFERENCES FILE_INFO (FILE_NM)
                )
            ''')
            
            conn.commit()
            print("SQLite 데이터베이스 초기화 완료")
        except Exception as e:
            print(f"데이터베이스 초기화 오류: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_connection(self):
        """데이터베이스 연결 객체 반환"""
        return sqlite3.connect(self.db_path)
    
    def test_connection(self):
        """데이터베이스 연결 테스트
        
        Returns:
            tuple: (연결 성공 여부, 오류 메시지)
        """
        try:
            conn = self.get_connection()
            conn.close()
            return True, ""
        except Exception as e:
            return False, str(e)
    
    # ============================================================
    # 기본 쿼리 실행 함수들
    # ============================================================
    def execute_query(self, query, params=None, commit=False):
        """쿼리 실행
        
        Args:
            query (str): 실행할 SQL 쿼리
            params (tuple, optional): 쿼리 파라미터. Defaults to None.
            commit (bool, optional): 변경사항 즉시 커밋 여부. Defaults to False.
            
        Returns:
            list: 쿼리 결과 행 목록 (SELECT인 경우) 또는 빈 리스트 (기타)
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # 쿼리 타입에 관계없이 fetchall() 시도
            try:
                result = cursor.fetchall()
                if result is None:
                    result = []
            except:
                # fetchall()이 실패하면 빈 리스트 반환
                result = []
                
            if commit:
                conn.commit()
                
            return result
        except Exception as e:
            if conn and commit:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def execute_non_select_query(self, query, params=None, commit=True):
        """SELECT가 아닌 쿼리 실행 (INSERT, UPDATE, DELETE 등)
        
        Args:
            query (str): 실행할 SQL 쿼리
            params (tuple, optional): 쿼리 파라미터. Defaults to None.
            commit (bool, optional): 변경사항 즉시 커밋 여부. Defaults to True.
            
        Returns:
            int: 영향받은 행 수
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            result = cursor.rowcount
                
            if commit:
                conn.commit()
                
            return result
        except Exception as e:
            if conn and commit:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    # ============================================================
    # 테이블 정보 관련 함수들
    # ============================================================
    def get_table_list(self):
        query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"  # 실제 테이블들
        return [row[0] for row in self.execute_query(query)]
    
    def get_table_info_list(self):
        """TABLE_INFO 테이블에서 테이블 목록 조회"""
        query = "SELECT TABLE_NM FROM TABLE_INFO ORDER BY TABLE_NM"
        return [row[0] for row in self.execute_query(query)]
    
    def get_table_details(self, table_nm):
        """특정 테이블의 상세 정보 조회"""
        query = "SELECT TABLE_NM, TABLE_DC, TABLE_OWNERSHIP FROM TABLE_INFO WHERE TABLE_NM = ?"
        result = self.execute_query(query, (table_nm,))
        return result[0] if result else None
    
    def save_table_info(self, table_nm, table_dc, table_ownership):
        """테이블 정보 저장 (새 항목 생성 또는 기존 항목 업데이트)"""
        # SQLite UPSERT 구문 사용
        query = """
            INSERT INTO TABLE_INFO (TABLE_NM, TABLE_DC, TABLE_OWNERSHIP) 
            VALUES (?, ?, ?)
            ON CONFLICT(TABLE_NM) DO UPDATE SET
                TABLE_DC = excluded.TABLE_DC,
                TABLE_OWNERSHIP = excluded.TABLE_OWNERSHIP
        """
        return self.execute_non_select_query(query, (table_nm, table_dc, table_ownership))
    
    def delete_table_info(self, table_nm):
        """테이블 정보 삭제"""
        query = "DELETE FROM TABLE_INFO WHERE TABLE_NM = ?"
        return self.execute_non_select_query(query, (table_nm,))
    
    def save_auto_config(self, table_nm, src_path, dest_path, auto_interval, use_yn):
        """자동화 설정 정보 저장 (DELETE_INTERVAL 제거)"""
        query = """
            INSERT INTO AUTO_CONFIG (TABLE_NM, SRC_PATH, DEST_PATH, AUTO_INTERVAL, USE_YN)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(TABLE_NM) DO UPDATE SET
                SRC_PATH = excluded.SRC_PATH,
                DEST_PATH = excluded.DEST_PATH,
                AUTO_INTERVAL = excluded.AUTO_INTERVAL,
                USE_YN = excluded.USE_YN
        """
        return self.execute_non_select_query(query, (table_nm, src_path, dest_path, auto_interval, use_yn))
    
    def delete_auto_config(self, table_nm):
        """자동화 설정 정보 삭제"""
        query = "DELETE FROM AUTO_CONFIG WHERE TABLE_NM = ?"
        return self.execute_non_select_query(query, (table_nm,))
    
    def update_auto_config_timestamp(self, table_nm):
        """마지막 실행 시간 업데이트"""
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = "UPDATE AUTO_CONFIG SET LAST_TIMESTAMP = ? WHERE TABLE_NM = ?"
        return self.execute_non_select_query(query, (current_time, table_nm))
    
    def register_file(self, table_nm, file_nm):
        """새 파일 정보 등록"""
        query = """
            INSERT INTO FILE_INFO (table_nm, file_nm, copy_yn, delete_yn)
            VALUES (?, ?, 'N', 'N')
            ON CONFLICT(file_nm) DO UPDATE SET
                table_nm = excluded.table_nm,
                copy_yn = 'N',
                delete_yn = 'N'
        """
        return self.execute_non_select_query(query, (table_nm, file_nm))
    
    def update_file_status(self, file_name, copy_status='Y'):
        """파일 처리 상태 업데이트 (INSERT_YN -> COPY_YN)"""
        query = "UPDATE FILE_INFO SET COPY_YN = ? WHERE FILE_NM = ?"
        return self.execute_non_select_query(query, (copy_status, file_name))
    
    def update_file_delete_status(self, file_name):
        """파일 삭제 상태 업데이트"""
        query = "UPDATE FILE_INFO SET DELETE_YN = 'Y' WHERE FILE_NM = ?"
        return self.execute_non_select_query(query, (file_name,))
    
    def log_task(self, table_nm, file_name, start_time, error_msg=None):
        """작업 로그 저장 (INSERT_CNT 제거)"""
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        query = """
            INSERT INTO TASK_LOG (TABLE_NM, FILE_NM, START_TIME, END_TIME)
            VALUES (?, ?, ?, ?)
        """
        return self.execute_non_select_query(query, (table_nm, file_name, start_time, current_time))
    
    def delete_column_mappings(self, table_nm):
        """특정 테이블의 컬럼 매핑 정보 삭제"""
        query = "DELETE FROM COL_MAPPING WHERE TABLE_NM = ?"
        return self.execute_non_select_query(query, (table_nm,))
    
    # ============================================================
    # 자동화 설정 관련 함수들 (DELETE_INTERVAL 제거)
    # ============================================================
    def get_auto_config_list(self):
        """자동화 설정 테이블에서 테이블 목록 조회"""
        query = "SELECT TABLE_NM FROM AUTO_CONFIG ORDER BY TABLE_NM"
        return [row[0] for row in self.execute_query(query)]
    
    def get_auto_config_details(self, table_nm):
        """특정 테이블의 자동화 설정 정보 조회"""
        query = "SELECT SRC_PATH, DEST_PATH, AUTO_INTERVAL, USE_YN FROM AUTO_CONFIG WHERE TABLE_NM = ?"
        result = self.execute_query(query, (table_nm,))
        return result[0] if result else None
    
    def save_auto_config(self, table_nm, src_path, dest_path, auto_interval, use_yn):
        """자동화 설정 정보 저장 (DELETE_INTERVAL 제거)"""
        query = """
            INSERT INTO AUTO_CONFIG (TABLE_NM, SRC_PATH, DEST_PATH, AUTO_INTERVAL, USE_YN)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(TABLE_NM) DO UPDATE SET
                SRC_PATH = excluded.SRC_PATH,
                DEST_PATH = excluded.DEST_PATH,
                AUTO_INTERVAL = excluded.AUTO_INTERVAL,
                USE_YN = excluded.USE_YN
        """
        return self.execute_query(query, (table_nm, src_path, dest_path, auto_interval, use_yn), commit=True)
    
    def delete_auto_config(self, table_nm):
        """자동화 설정 정보 삭제"""
        query = "DELETE FROM AUTO_CONFIG WHERE TABLE_NM = ?"
        return self.execute_query(query, (table_nm,), commit=True)
    
    def update_auto_config_timestamp(self, table_nm):
        """마지막 실행 시간 업데이트"""
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = "UPDATE AUTO_CONFIG SET LAST_TIMESTAMP = ? WHERE TABLE_NM = ?"
        return self.execute_query(query, (current_time, table_nm), commit=True)
    
    def get_all_auto_configs(self):
        """모든 자동화 설정 정보 조회 (스케줄러용)"""
        query = """
            SELECT table_nm, dest_path, auto_interval, last_timestamp
            FROM AUTO_CONFIG 
            WHERE auto_interval IS NOT NULL AND auto_interval > 0
        """
        return self.execute_query(query)
    
    # ============================================================
    # 파일 정보 관련 함수들 (INSERT_YN -> COPY_YN 변경)
    # ============================================================
    def get_pending_files(self, table_nm):
        """처리 대기 중인 파일 목록 조회"""
        query = """
            SELECT fi.file_nm, ac.dest_path 
            FROM FILE_INFO fi
            JOIN AUTO_CONFIG ac ON fi.table_nm = ac.table_nm
            WHERE ac.table_nm = ?
            AND ac.use_yn = 'Y'
            AND (fi.copy_yn IS NULL OR fi.copy_yn = 'N')
            ORDER BY fi.file_nm ASC
        """
        return self.execute_query(query, (table_nm,))
    
    def get_pending_files_count(self, table_nm):
        """처리 대기 중인 파일 수 조회"""
        query = "SELECT COUNT(*) FROM FILE_INFO WHERE table_nm = ? AND copy_yn = 'N'"
        result = self.execute_query(query, (table_nm,))
        return result[0][0] if result else 0
    
    def register_file(self, table_nm, file_nm):
        """새 파일 정보 등록"""
        query = """
            INSERT INTO FILE_INFO (table_nm, file_nm, copy_yn, delete_yn)
            VALUES (?, ?, 'N', 'N')
            ON CONFLICT(file_nm) DO UPDATE SET
                table_nm = excluded.table_nm,
                copy_yn = 'N',
                delete_yn = 'N'
        """
        return self.execute_query(query, (table_nm, file_nm), commit=True)
    
    def update_file_status(self, file_name, copy_status='Y'):
        """파일 처리 상태 업데이트 (INSERT_YN -> COPY_YN)"""
        query = "UPDATE FILE_INFO SET COPY_YN = ? WHERE FILE_NM = ?"
        return self.execute_query(query, (copy_status, file_name), commit=True)
    
    def update_file_delete_status(self, file_name):
        """파일 삭제 상태 업데이트"""
        query = "UPDATE FILE_INFO SET DELETE_YN = 'Y' WHERE FILE_NM = ?"
        return self.execute_query(query, (file_name,), commit=True)
    
    def get_existing_files(self, table_nm):
        """기존에 등록된 파일 목록 조회"""
        query = "SELECT file_nm FROM FILE_INFO WHERE table_nm = ?"
        result = self.execute_query(query, (table_nm,))
        return {row[0] for row in result}
    
    def get_files_to_delete(self, table_nm):
        """삭제 대상 파일 목록 조회"""
        query = """
            SELECT fi.file_nm, ac.dest_path 
            FROM FILE_INFO fi
            JOIN AUTO_CONFIG ac ON fi.table_nm = ac.table_nm
            WHERE fi.table_nm = ? 
            AND fi.copy_yn = 'Y' 
            AND fi.delete_yn = 'N'
        """
        return self.execute_query(query, (table_nm,))
    
    # ============================================================
    # 로그 관련 함수들 (INSERT_CNT 제거)
    # ============================================================
    def log_task(self, table_nm, file_name, start_time, error_msg=None):
        """작업 로그 저장 (INSERT_CNT 제거)"""
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        query = """
            INSERT INTO TASK_LOG (TABLE_NM, FILE_NM, START_TIME, END_TIME)
            VALUES (?, ?, ?, ?)
        """
        return self.execute_query(query, (table_nm, file_name, start_time, current_time), commit=True)
    
    # ============================================================
    # 컬럼 매핑 관련 함수들 (사용하지 않지만 호환성 유지)
    # ============================================================
    def get_col_mapping_tables(self):
        """컬럼 매핑이 설정된 테이블 목록 조회"""
        query = "SELECT DISTINCT TABLE_NM FROM COL_MAPPING ORDER BY TABLE_NM"
        return [row[0] for row in self.execute_query(query)]
    
    def get_column_mappings(self, table_nm):
        """특정 테이블의 컬럼 매핑 정보 조회"""
        query = "SELECT DB_COL_NM, XML_COL_NM FROM COL_MAPPING WHERE TABLE_NM = ?"
        return self.execute_query(query, (table_nm,))
    
    def save_column_mappings(self, table_nm, mappings):
        """컬럼 매핑 정보 저장"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 기존 매핑 삭제
            cursor.execute("DELETE FROM COL_MAPPING WHERE TABLE_NM = ?", (table_nm,))
            
            # 새 매핑 저장
            if mappings:
                for db_col_nm, xml_col_nm, _ in mappings:  # dup_check_yn 무시
                    cursor.execute(
                        "INSERT INTO COL_MAPPING (TABLE_NM, DB_COL_NM, XML_COL_NM) VALUES (?, ?, ?)",
                        (table_nm, db_col_nm, xml_col_nm)
                    )
            
            conn.commit()
            return True
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def delete_column_mappings(self, table_nm):
        """특정 테이블의 컬럼 매핑 정보 삭제"""
        query = "DELETE FROM COL_MAPPING WHERE TABLE_NM = ?"
        return self.execute_query(query, (table_nm,), commit=True)
    
    # ============================================================
    # 데이터 조회 및 참조 함수들 (호환성 유지)
    # ============================================================
    def get_table_data_sample(self, table_name, limit=10):
        """테이블 데이터 샘플 조회 (SQLite에서는 관리 테이블 데이터 반환)"""
        if table_name in ['TABLE_INFO', 'FILE_INFO', 'AUTO_CONFIG', 'COL_MAPPING', 'TASK_LOG']:
            try:
                query = f"SELECT * FROM {table_name} LIMIT {limit}"
                result = self.execute_query(query)
                # execute_query가 SELECT 쿼리에 대해 list를 반환하는지 확인
                if isinstance(result, list):
                    return result
                else:
                    return []
            except Exception as e:
                print(f"테이블 {table_name} 데이터 조회 오류: {e}")
                return []
        else:
            return []
    
    def get_table_columns(self, table_name):
        """테이블 컬럼 정보 조회 (SQLite PRAGMA 사용)"""
        query = f"PRAGMA table_info({table_name})"
        result = self.execute_query(query)
        return [(row[1], row[2]) for row in result]  # (컬럼명, 타입)
    
    # 추가 메서드

    def get_existing_files(self, table_nm):
        """기존에 등록된 파일 목록 조회 (호환성용)
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            set: 파일명 집합
        """
        query = "SELECT file_nm FROM FILE_INFO WHERE table_nm = ?"
        result = self.execute_query(query, (table_nm,))
        return {row[0] for row in result}

    def get_pending_files_count(self, table_nm):
        """처리 대기 중인 파일 수 조회 (호환성용)
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            int: 대기 중인 파일 수
        """
        query = "SELECT COUNT(*) FROM FILE_INFO WHERE table_nm = ? AND copy_yn = 'N'"
        result = self.execute_query(query, (table_nm,))
        return result[0][0] if result else 0

    def get_all_auto_configs(self):
        """모든 자동화 설정 정보 조회 (스케줄러용) - 호환성 개선
        
        Returns:
            list: 자동화 설정 목록 [(table_nm, dest_path, auto_interval, last_timestamp), ...]
        """
        query = """
            SELECT table_nm, dest_path, auto_interval, last_timestamp
            FROM AUTO_CONFIG 
            WHERE auto_interval IS NOT NULL AND auto_interval > 0
        """
        return self.execute_query(query)

    def update_file_copy_status(self, file_name, copy_status='Y'):
        """파일 복사 상태 업데이트 (호환성용 - update_file_status와 동일)
        
        Args:
            file_name (str): 파일명
            copy_status (str): 복사 상태 ('Y' 또는 'N')
            
        Returns:
            int: 영향받은 행 수
        """
        return self.update_file_status(file_name, copy_status)