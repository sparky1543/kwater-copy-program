import os
import xml.etree.ElementTree as ET
import csv
import datetime
from decimal import Decimal, InvalidOperation


class DataProcessor:
    """XML 파일 처리 및 데이터 가공 기능을 제공하는 클래스"""
    
    def __init__(self, db_manager=None):
        """데이터 프로세서 초기화
        
        Args:
            db_manager: 데이터베이스 매니저 객체 (선택적)
        """
        self.db_manager = db_manager
    
    def parse_xml_file(self, file_path, callback=None, progress_callback=None):
        """XML 파일 파싱 및 데이터 추출
        
        Args:
            file_path (str): XML 파일 경로
            callback (function, optional): 진행 중 호출할 콜백 함수. Defaults to None.
            progress_callback (function, optional): 진행률 업데이트 콜백. Defaults to None.
            
        Returns:
            tuple: (컬럼 목록, 데이터 행 목록, 전체 레코드 수)
            
        Raises:
            Exception: XML 파싱 오류 발생 시 예외 발생
        """
        try:
            # 파일 존재 확인
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
            
            # XML 파일 파싱
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # 테이블 이름 추출 (파일명 기반)
            file_name = os.path.basename(file_path)
            table_name = file_name.split('_')[0] if '_' in file_name else file_name.replace('.xml', '')
            
            # 데이터 레코드 탐색
            data_records = root.findall(".//DATA_RECORD")
            total_records = len(data_records)
            
            if total_records == 0:
                if callback:
                    callback(f"XML 파일에 DATA_RECORD 태그가 없습니다: {file_path}")
                return [], [], 0
            
            if callback:
                callback(f"총 {total_records}개의 DATA_RECORD 태그를 찾았습니다.")
            
            # 컬럼 목록 수집
            all_columns = set()
            for record in data_records:
                for element in record:
                    all_columns.add(element.tag)
            
            # 컬럼 알파벳순 정렬
            sorted_columns = sorted(list(all_columns))
            
            # 데이터 추출
            parsed_data = []
            
            for i, record in enumerate(data_records):
                # 진행률 업데이트
                if progress_callback and i % max(1, total_records // 10) == 0:
                    progress = (i / total_records) * 100
                    progress_callback(i, total_records, progress)
                
                # 레코드 데이터 추출
                record_data = {col: "" for col in sorted_columns}
                
                for element in record:
                    record_data[element.tag] = element.text if element.text else ""
                
                parsed_data.append(record_data)
            
            return sorted_columns, parsed_data, total_records
            
        except Exception as e:
            if callback:
                callback(f"XML 파싱 중 오류 발생: {str(e)}")
            raise
    
    def save_to_csv(self, file_path, columns, data, callback=None):
        """파싱된 데이터를 CSV 파일로 저장
        
        Args:
            file_path (str): 저장할 CSV 파일 경로
            columns (list): 컬럼 목록
            data (list): 데이터 행 목록
            callback (function, optional): 진행 상황 콜백. Defaults to None.
            
        Returns:
            bool: 저장 성공 여부
            
        Raises:
            Exception: 파일 저장 중 오류 발생 시 예외 발생
        """
        try:
            # 디렉토리 확인 및 생성
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            
            if callback:
                callback(f"CSV 파일 생성 중: {file_path}")
            
            # CSV 파일 생성 및 데이터 쓰기
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                
                # 헤더 행 쓰기
                csv_writer.writerow(columns)
                
                # 데이터 행 쓰기
                for row_data in data:
                    # 각 열의 데이터를 순서대로 추출
                    row_values = [row_data.get(column_name, "") for column_name in columns]
                    csv_writer.writerow(row_values)
            
            # 파일 크기 정보
            file_size = os.path.getsize(file_path)
            file_size_kb = file_size / 1024
            file_size_mb = file_size_kb / 1024
            
            size_info = f"{file_size_mb:.2f} MB" if file_size_mb >= 1 else f"{file_size_kb:.2f} KB"
            
            if callback:
                callback(f"CSV 파일 저장 완료: {file_path} (크기: {size_info})")
            
            return True
            
        except Exception as e:
            if callback:
                callback(f"CSV 저장 중 오류 발생: {str(e)}")
            raise
    
    def process_xml_for_insert(self, file_path, table_nm, progress_callback=None):
        """XML 파일을 처리하여 데이터베이스 삽입용 데이터 생성
        
        Args:
            file_path (str): XML 파일 경로
            table_nm (str): 대상 테이블명
            progress_callback (function, optional): 진행률 콜백. Defaults to None.
            
        Returns:
            tuple: (컬럼 매핑 정보, 데이터 레코드 목록, 전체 레코드 수)
            
        Raises:
            Exception: 처리 중 오류 발생 시 예외 발생
        """
        try:
            if not self.db_manager:
                raise ValueError("데이터베이스 매니저가 설정되지 않았습니다.")
            
            # XML 파일 파싱
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # 데이터 레코드 탐색
            data_records = root.findall(".//DATA_RECORD")
            total_records = len(data_records)
            
            # 컬럼 매핑 정보 조회
            mapping_rows = self.db_manager.get_column_mappings(table_nm)
            
            if not mapping_rows:
                raise ValueError(f"테이블 {table_nm}에 대한 컬럼 매핑 정보가 없습니다.")
            
            # 컬럼 매핑 정보 구성
            col_mapping = {}
            dup_check_columns = []
            
            for row in mapping_rows:
                xml_col_nm, db_col_nm, dup_check_yn = row
                col_mapping[xml_col_nm] = db_col_nm
                
                # 중복 체크 대상 컬럼 저장
                if dup_check_yn and dup_check_yn.upper() == 'Y':
                    dup_check_columns.append(db_col_nm)
            
            # 테이블 컬럼 정보 조회
            column_rows = self.db_manager.get_table_columns(table_nm)
            column_types = {row[0]: row[1] for row in column_rows}
            
            # 데이터 레코드 추출
            data_records_list = []
            
            for i, record in enumerate(data_records):
                # 진행률 업데이트
                if progress_callback and i % max(1, total_records // 10) == 0:
                    progress = (i / total_records) * 100
                    progress_callback(i, total_records, progress)
                
                # XML 태그를 DB 컬럼으로 매핑하여 레코드 데이터 생성
                record_data = {}
                for element in record:
                    # XML 태그명에 해당하는 DB 컬럼명 확인
                    db_col = col_mapping.get(element.tag)
                    if db_col:
                        record_data[db_col] = element.text
                
                data_records_list.append(record_data)
                
            return col_mapping, dup_check_columns, column_types, data_records_list, total_records
            
        except Exception as e:
            if progress_callback:
                progress_callback(0, 1, 0, f"오류: {str(e)}")
            raise
    
    def generate_insert_sql(self, table_nm, column_types, data_records, batch_size=1000):
        """INSERT SQL 문 생성
        
        Args:
            table_nm (str): 테이블명
            column_types (dict): 컬럼 타입 정보
            data_records (list): 데이터 레코드 목록
            batch_size (int, optional): 배치 크기. Defaults to 1000.
            
        Returns:
            list: 생성된 SQL 문 목록
            
        Raises:
            Exception: SQL 생성 중 오류 발생 시 예외 발생
        """
        sql_list = []
        
        # 배치별 처리
        for batch_index in range(0, len(data_records), batch_size):
            # 현재 배치 데이터 추출
            batch_records = data_records[batch_index:batch_index + batch_size]
            
            # 배치가 비어있으면 건너뜀
            if not batch_records:
                continue
            
            # 컬럼 목록 수집
            all_columns = set()
            for record in batch_records:
                all_columns.update(record.keys())
            
            # INSERT ALL 문 생성
            insert_all_sql = f"INSERT ALL"
            
            # 레코드별 INSERT 구문 생성
            for record in batch_records:
                columns = []
                values = []
                
                # 컬럼별 값 처리
                for col, val in record.items():
                    # 빈 값 처리
                    if val is None or (isinstance(val, str) and val.strip() == ''):
                        continue
                    
                    columns.append(col)
                    col_type = column_types.get(col, 'VARCHAR2').upper()
                    
                    # 데이터 타입별 처리
                    if col_type in ['VARCHAR2', 'CHAR']:
                        # SQL 인젝션 방지
                        val = val.replace("'", "''")
                        values.append(f"'{val}'")
                    elif col_type == 'NUMBER':
                        try:
                            decimal_value = Decimal(val)
                            values.append(f"{decimal_value}")
                        except (ValueError, InvalidOperation):
                            continue
                    elif col_type in ['DATE', 'TIMESTAMP']:
                        values.append(f"TO_DATE('{val}', 'YYYY-MM-DD HH24:MI:SS')")
                    else:
                        # 기타 타입 문자열로 처리
                        val = val.replace("'", "''")
                        values.append(f"'{val}'")
                
                # 유효한 데이터가 있는 경우만 추가
                if columns:
                    columns_str = ", ".join(columns)
                    values_str = ", ".join(values)
                    insert_all_sql += f"\n  INTO {table_nm} ({columns_str}) VALUES ({values_str})"
            
            # SQL 완성
            insert_all_sql += "\nSELECT 1 FROM DUAL"
            sql_list.append(insert_all_sql)
        
        return sql_list
    
    def generate_merge_sql(self, table_nm, column_types, data_records, dup_check_columns, batch_size=1000):
        """MERGE SQL 문 생성 (중복 체크 포함)
        
        Args:
            table_nm (str): 테이블명
            column_types (dict): 컬럼 타입 정보
            data_records (list): 데이터 레코드 목록
            dup_check_columns (list): 중복 체크 컬럼 목록
            batch_size (int, optional): 배치 크기. Defaults to 1000.
            
        Returns:
            list: 생성된 SQL 문 목록
            
        Raises:
            Exception: SQL 생성 중 오류 발생 시 예외 발생
        """
        sql_list = []
        
        # 배치별 처리
        for batch_index in range(0, len(data_records), batch_size):
            # 현재 배치 데이터 추출
            batch_records = data_records[batch_index:batch_index + batch_size]
            
            # 배치가 비어있으면 건너뜀
            if not batch_records:
                continue
            
            # 컬럼 목록 수집
            all_columns = set()
            for record in batch_records:
                all_columns.update(record.keys())
            
            # 유효한 중복 체크 컬럼 확인
            valid_check_cols = [col for col in dup_check_columns if col in all_columns]
            
            if not valid_check_cols:
                # 중복 체크 컬럼이 없는 경우: INSERT ALL로 처리
                sql_list.extend(self.generate_insert_sql(table_nm, column_types, batch_records))
                continue
            
            # 데이터 행별 SELECT 쿼리 생성
            select_queries = []
            for record in batch_records:
                column_values = []
                
                # 컬럼별 값 처리
                for col, val in record.items():
                    if val is None or (isinstance(val, str) and val.strip() == ''):
                        column_values.append(f"NULL AS {col}")
                    else:
                        col_type = column_types.get(col, 'VARCHAR2').upper()
                        
                        # 데이터 타입별 처리
                        if col_type in ['VARCHAR2', 'CHAR']:
                            val = val.replace("'", "''")
                            column_values.append(f"'{val}' AS {col}")
                        elif col_type == 'NUMBER':
                            try:
                                decimal_value = Decimal(val)
                                column_values.append(f"{decimal_value} AS {col}")
                            except (ValueError, InvalidOperation):
                                column_values.append(f"NULL AS {col}")
                        elif col_type in ['DATE', 'TIMESTAMP']:
                            column_values.append(f"TO_DATE('{val}', 'YYYY-MM-DD HH24:MI:SS') AS {col}")
                        else:
                            val = val.replace("'", "''")
                            column_values.append(f"'{val}' AS {col}")
                
                # 유효한 데이터가 있는 경우만 추가
                if column_values:
                    select_query = f"SELECT {', '.join(column_values)} FROM DUAL"
                    select_queries.append(select_query)
            
            if not select_queries:
                continue
            
            # MERGE 문 구성
            merge_sql = f"""
            MERGE INTO {table_nm} t
            USING (
                {' UNION ALL '.join(select_queries)}
            ) s
            ON (
            """
            
            # 중복 체크 조건
            on_conditions = []
            for col in valid_check_cols:
                on_conditions.append(f"(NVL(t.{col}, '||NULL||') = NVL(s.{col}, '||NULL||'))")
            
            merge_sql += f"{' AND '.join(on_conditions)})\n"
            
            # UPDATE 절
            merge_sql += "WHEN MATCHED THEN\n"
            merge_sql += "  UPDATE SET "
            
            # 중복 체크 컬럼을 제외한 컬럼 업데이트
            update_cols = []
            for col in all_columns:
                if col not in valid_check_cols:
                    update_cols.append(f"t.{col} = s.{col}")
            
            if not update_cols:
                merge_sql += "t.ROWID = t.ROWID\n"
            else:
                merge_sql += ", ".join(update_cols) + "\n"
            
            # INSERT 절
            merge_sql += "WHEN NOT MATCHED THEN\n"
            merge_sql += f"  INSERT ({', '.join(all_columns)})\n"
            merge_sql += f"  VALUES ({', '.join([f's.{col}' for col in all_columns])})"
            
            sql_list.append(merge_sql)
        
        return sql_list
    
    def format_data_for_export(self, rows, columns):
        """데이터베이스 조회 결과를 CSV 내보내기 형식으로 변환
        
        Args:
            rows (list): 데이터베이스 조회 결과 행 목록
            columns (list): 컬럼 목록
            
        Returns:
            list: 형식화된 데이터 행 목록
        """
        formatted_rows = []
        
        for row_data in rows:
            # 데이터 타입별 적절한 변환 처리
            formatted_row = []
            for cell_value in row_data:
                if cell_value is None:
                    formatted_row.append("")
                elif isinstance(cell_value, datetime.datetime):
                    formatted_row.append(cell_value.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(cell_value, (bytearray, bytes)):
                    formatted_row.append(str(cell_value))
                else:
                    formatted_row.append(str(cell_value))
            
            formatted_rows.append(formatted_row)
        
        return formatted_rows