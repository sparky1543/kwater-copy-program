import os
import threading
import datetime
import csv


class ExecutionController:
    """파일 복사 및 실행 컨트롤러
    
    파일 복사, XML 파싱, 스케줄링 등 실행 작업을 관리
    """
    
    def __init__(self, db_manager=None, linux_ssh_client=None, was_ssh_client=None, 
                 data_processor=None, scheduler_manager=None):
        """실행 컨트롤러 초기화
        
        Args:
            db_manager: 데이터베이스 매니저 객체
            linux_ssh_client: Linux SSH 클라이언트 객체
            was_ssh_client: WAS SSH 클라이언트 객체
            data_processor: 데이터 프로세서 객체
            scheduler_manager: 스케줄러 매니저 객체
        """
        self.db_manager = db_manager
        self.linux_ssh_client = linux_ssh_client
        self.was_ssh_client = was_ssh_client
        self.data_processor = data_processor
        self.scheduler_manager = scheduler_manager
        
        # 상태 변수
        self.is_online_mode = False  # 온라인/오프라인 모드
        self.parsing_active = False  # 파싱 작업 활성화 상태
        self.parsing_thread = None   # 파싱 작업 스레드
        
        # 콜백 함수
        self.progress_callback = None
        self.status_callback = None
        self.log_callback = None
        self.mode_change_callback = None
    
    def set_dependencies(self, db_manager=None, linux_ssh_client=None, was_ssh_client=None,
                        data_processor=None, scheduler_manager=None):
        """종속성 설정
        
        Args:
            db_manager: 데이터베이스 매니저 객체
            linux_ssh_client: Linux SSH 클라이언트 객체
            was_ssh_client: WAS SSH 클라이언트 객체
            data_processor: 데이터 프로세서 객체
            scheduler_manager: 스케줄러 매니저 객체
        """
        if db_manager:
            self.db_manager = db_manager
        if linux_ssh_client:
            self.linux_ssh_client = linux_ssh_client
        if was_ssh_client:
            self.was_ssh_client = was_ssh_client
        if data_processor:
            self.data_processor = data_processor
        if scheduler_manager:
            self.scheduler_manager = scheduler_manager
    
    def set_callbacks(self, progress_callback=None, status_callback=None, 
                 log_callback=None, mode_change_callback=None,
                 tree_update_callback=None, tree_item_callback=None):
        """콜백 함수 설정
        
        Args:
            progress_callback: 진행 상태 업데이트 콜백 함수
            status_callback: 상태 메시지 업데이트 콜백 함수
            log_callback: 로그 메시지 콜백 함수
            mode_change_callback: 모드 변경 콜백 함수
            tree_update_callback: 트리뷰 컬럼 업데이트 콜백 함수
            tree_item_callback: 트리뷰 항목 추가 콜백 함수
        """
        self.progress_callback = progress_callback
        self.status_callback = status_callback
        self.log_callback = log_callback
        self.mode_change_callback = mode_change_callback
        self.tree_update_callback = tree_update_callback
        self.tree_item_callback = tree_item_callback
        
        # 스케줄러에 콜백 설정
        if self.scheduler_manager:
            self.scheduler_manager.set_callbacks(
                progress_callback=self.progress_callback,
                status_callback=self.status_callback,
                log_callback=self.log_callback
            )
    
    def set_online_mode(self, is_online):
        """온라인/오프라인 모드 설정
        
        Args:
            is_online (bool): 온라인 모드 여부
        """
        if self.is_online_mode != is_online:
            self.is_online_mode = is_online
            
            # 모드 변경 콜백 호출
            if self.mode_change_callback:
                self.mode_change_callback(is_online)
    
    def is_online(self):
        """온라인 모드 여부 반환
        
        Returns:
            bool: 온라인 모드 여부
        """
        return self.is_online_mode
    
    def log(self, message):
        """로그 메시지 출력
        
        Args:
            message (str): 로그 메시지
        """
        # 콘솔에 출력
        print(message)
        
        # 콜백 함수가 있으면 호출
        if self.log_callback:
            self.log_callback(message)
    
    # ============================================================
    # 온라인 모드 함수들 (파일 복사)
    # ============================================================
    def start_data_insert(self):
        """파일 복사 작업 시작 (온라인 모드)
        
        Returns:
            bool: 작업 시작 성공 여부
        """
        if not self.is_online_mode:
            self.log("온라인 모드가 아닙니다.")
            return False
        
        if not self.scheduler_manager:
            self.log("스케줄러 매니저가 설정되지 않았습니다.")
            return False
        
        # 스케줄러 시작
        return self.scheduler_manager.start_scheduler()
    
    def stop_data_insert(self):
        """파일 복사 작업 중지 (온라인 모드)
        
        Returns:
            bool: 작업 중지 성공 여부
        """
        if not self.scheduler_manager:
            return False
        
        # 스케줄러 중지
        return self.scheduler_manager.stop_scheduler()
    
    def is_scheduler_running(self):
        """스케줄러 실행 상태 확인
        
        Returns:
            bool: 스케줄러 실행 중 여부
        """
        if not self.scheduler_manager:
            return False
        
        return self.scheduler_manager.is_running()
    
    def load_table_list(self):
        """시스템 테이블 목록 조회
        
        Returns:
            list: 테이블명 목록
        """
        if not self.db_manager or not self.is_online_mode:
            return []
        
        try:
            return self.db_manager.get_table_list()
        except Exception as e:
            self.log(f"테이블 목록 불러오기 오류: {e}")
            return []
    
    def load_table_data(self, table_name, limit=10):
        """테이블 데이터 조회
        
        Args:
            table_name (str): 테이블명
            limit (int, optional): 최대 행 수. Defaults to 10.
        
        Returns:
            tuple: (컬럼 목록, 데이터 행 목록)
        """
        if not self.db_manager or not self.is_online_mode:
            return [], []
        
        try:
            # SQLite 관리 테이블의 컬럼 정보 조회
            columns_info = self.db_manager.get_table_columns(table_name)
            columns = [col[0] for col in columns_info]  # 컬럼명만 추출
            
            # 데이터 조회
            rows = self.db_manager.get_table_data_sample(table_name, limit)
            
            return columns, rows
        except Exception as e:
            self.log(f"{table_name} 데이터 불러오기 오류: {e}")
            return [], []
    
    def export_to_csv(self, table_name, file_path):
        """테이블 데이터를 CSV로 내보내기
        
        Args:
            table_name (str): 테이블명
            file_path (str): 저장할 파일 경로
        
        Returns:
            bool: 내보내기 성공 여부
        """
        if not self.db_manager or not self.is_online_mode:
            return False
        
        try:
            # SQLite 관리 테이블의 컬럼 정보 조회
            columns_info = self.db_manager.get_table_columns(table_name)
            columns = [col[0] for col in columns_info]  # 컬럼명만 추출
            
            # 데이터 조회 (전체 데이터)
            rows = self.db_manager.get_table_data_sample(table_name, limit=1000)
            
            # 데이터가 없는 경우
            if not rows:
                self.log(f"{table_name} 테이블에 표시할 데이터가 없습니다.")
                return False
            
            # CSV 파일 생성
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                
                # 헤더 행 추가
                csv_writer.writerow(columns)
                
                # 데이터 행 추가
                for row_data in rows:
                    # 데이터 형식 변환
                    formatted_row = []
                    for cell_value in row_data:
                        if cell_value is None:
                            formatted_row.append("")
                        elif isinstance(cell_value, datetime.datetime):
                            formatted_row.append(cell_value.strftime('%Y-%m-%d %H:%M:%S'))
                        else:
                            formatted_row.append(str(cell_value))
                    csv_writer.writerow(formatted_row)
            
            self.log(f"{table_name} 테이블이 CSV 파일로 저장되었습니다.\n저장 위치: {file_path}")
            return True
            
        except Exception as e:
            self.log(f"CSV 파일 저장 중 오류 발생: {e}")
            return False
    
    def refresh_progress_view(self):
        """진행 상태 표시 업데이트 요청"""
        # 진행 상태 업데이트 콜백 호출
        if self.progress_callback:
            self.progress_callback(None, None, "refresh")
    
    # ============================================================
    # 오프라인 모드 함수들 (XML 파싱)
    # ============================================================
    def start_parse(self, xml_file_path, save_path):
        """XML 파싱 작업 시작 (오프라인 모드)
        
        Args:
            xml_file_path (str): XML 파일 경로
            save_path (str): 결과 저장 경로
            
        Returns:
            bool: 작업 시작 성공 여부
        """
        # 이미 파싱 작업 중인지 확인
        if self.parsing_active:
            self.log("이미 파싱 작업이 진행 중입니다.")
            return False
        
        # 입력값 유효성 검사
        if not xml_file_path or not os.path.exists(xml_file_path):
            self.log("XML 파일이 존재하지 않습니다.")
            return False
        
        if not save_path:
            self.log("저장 경로가 지정되지 않았습니다.")
            return False
        
        # 상태 업데이트
        self.parsing_active = True
        
        # 상태 콜백 호출
        if self.status_callback:
            self.status_callback("파싱중", True)
        
        # 새 스레드에서 파싱 작업 시작
        self.parsing_thread = threading.Thread(
            target=self._parse_xml_file,
            args=(xml_file_path, save_path),
            daemon=True
        )
        self.parsing_thread.start()
        
        return True
    
    def stop_parse(self):
        """XML 파싱 작업 중지 (오프라인 모드)
        
        Returns:
            bool: 작업 중지 성공 여부
        """
        if not self.parsing_active:
            return False
        
        # 파싱 작업 중지 플래그 설정
        self.parsing_active = False
        self.log("파싱 중지 요청...")
        
        # 상태 콜백 호출
        if self.status_callback:
            self.status_callback("종료", False)
        
        return True
    
    def _parse_xml_file(self, xml_file_path, save_path):
        """XML 파일 파싱 작업 실행 (내부 함수)
        
        Args:
            xml_file_path (str): XML 파일 경로
            save_path (str): 결과 저장 경로
        """
        try:
            self.log(f"XML 파일 파싱 시작: {xml_file_path}")
            
            # 파싱 작업 수행
            sorted_columns, parsed_data, total_records = self.data_processor.parse_xml_file(
                xml_file_path,
                callback=self.log,
                progress_callback=self._update_parse_progress
            )
            
            # 중단 요청 확인
            if not self.parsing_active:
                self.log("파싱 강제 종료됨")
                self._finish_parsing("중단")
                return
            
            # 트리뷰 컬럼 업데이트 및 데이터 표시
            if self.tree_update_callback and sorted_columns:
                # 컬럼 업데이트
                self.tree_update_callback(sorted_columns)
                
                # 처음 10개 행만 트리뷰에 표시
                preview_count = min(10, len(parsed_data))
                for i in range(preview_count):
                    row_data = parsed_data[i]
                    row_values = [row_data.get(col, "") for col in sorted_columns]
                    self.tree_item_callback(row_values)
            
            # CSV 파일로 저장
            if parsed_data:
                # 출력 파일명 생성
                xml_file_name = os.path.basename(xml_file_path)
                csv_file_name = os.path.splitext(xml_file_name)[0] + ".csv"
                csv_file_path = os.path.join(save_path, csv_file_name)
                
                # CSV 저장
                self.data_processor.save_to_csv(
                    csv_file_path,
                    sorted_columns,
                    parsed_data,
                    callback=self.log
                )
            
            # 파싱 완료 처리
            self._finish_parsing("완료")
            
        except Exception as e:
            self.log(f"파싱 중 오류 발생: {str(e)}")
            self._finish_parsing("오류")
    
    def _update_parse_progress(self, current, total, progress):
        """파싱 진행 상태 업데이트
        
        Args:
            current (int): 현재 처리 항목 수
            total (int): 전체 처리 항목 수
            progress (float): 진행률 (0-100)
        """
        # 진행 상태 콜백 호출
        if self.progress_callback:
            self.progress_callback(None, None, "파싱중", current, total)
    
    def _finish_parsing(self, status):
        """파싱 작업 종료 처리
        
        Args:
            status (str): 종료 상태 ("완료", "오류", "중단")
        """
        # 파싱 상태 변수 업데이트
        self.parsing_active = False
        
        # 상태 콜백 호출
        if self.status_callback:
            if status == "완료":
                self.status_callback("준비", False)
                self.log("파싱 작업이 완료되었습니다.")
            elif status == "오류":
                self.status_callback("오류", False)
                self.log("파싱 작업이 오류로 중단되었습니다.")
            else:
                self.status_callback("준비", False)
                self.log("파싱 작업이 중단되었습니다.")