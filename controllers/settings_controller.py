class SettingsController:
    """설정 관리 컨트롤러
    
    애플리케이션의 설정(테이블 정보, 자동화 설정) 관리
    """
    
    def __init__(self, db_manager=None, linux_ssh_client=None, was_ssh_client=None):
        """설정 컨트롤러 초기화
        
        Args:
            db_manager: 데이터베이스 매니저 객체
            linux_ssh_client: Linux SSH 클라이언트 객체
            was_ssh_client: WAS SSH 클라이언트 객체
        """
        self.db_manager = db_manager
        self.linux_ssh_client = linux_ssh_client
        self.was_ssh_client = was_ssh_client
        
        # 로그 콜백
        self.log_callback = None
    
    def set_dependencies(self, db_manager=None, linux_ssh_client=None, was_ssh_client=None):
        """종속성 설정
        
        Args:
            db_manager: 데이터베이스 매니저 객체
            linux_ssh_client: Linux SSH 클라이언트 객체
            was_ssh_client: WAS SSH 클라이언트 객체
        """
        if db_manager:
            self.db_manager = db_manager
        if linux_ssh_client:
            self.linux_ssh_client = linux_ssh_client
        if was_ssh_client:
            self.was_ssh_client = was_ssh_client
    
    def set_log_callback(self, log_callback):
        """로그 콜백 함수 설정
        
        Args:
            log_callback: 로그 메시지 콜백 함수
        """
        self.log_callback = log_callback
    
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
    # 접속 정보 관리
    # ============================================================
    def get_connection_info(self):
        """현재 접속 정보 조회
        
        Returns:
            dict: 접속 정보 (Linux SSH, WAS SSH)
        """
        result = {}
        
        # Linux SSH 연결 정보
        if self.linux_ssh_client:
            result['linux_ssh'] = self.linux_ssh_client.get_connection_info()
        
        # WAS SSH 연결 정보
        if self.was_ssh_client:
            result['was_ssh'] = self.was_ssh_client.get_connection_info()
        
        return result
    
    def update_linux_info(self, ip, port, username, password):
        """Linux SSH 접속 정보 업데이트
        
        Args:
            ip (str): 서버 IP 주소
            port (int): 포트 번호
            username (str): 사용자명
            password (str): 비밀번호
            
        Returns:
            bool: 업데이트 성공 여부
        """
        if not self.linux_ssh_client:
            return False
        
        try:
            # 연결 정보 업데이트
            self.linux_ssh_client.set_connection_info(ip, int(port), username, password)
            return True
        except Exception as e:
            self.log(f"Linux SSH 접속 정보 업데이트 오류: {e}")
            return False
    
    def update_was_info(self, ip, port, username, password):
        """WAS SSH 접속 정보 업데이트
        
        Args:
            ip (str): 서버 IP 주소
            port (int): 포트 번호
            username (str): 사용자명
            password (str): 비밀번호
            
        Returns:
            bool: 업데이트 성공 여부
        """
        if not self.was_ssh_client:
            return False
        
        try:
            # 연결 정보 업데이트
            self.was_ssh_client.set_connection_info(ip, int(port), username, password)
            return True
        except Exception as e:
            self.log(f"WAS SSH 접속 정보 업데이트 오류: {e}")
            return False
    
    # ============================================================
    # 테이블 정보 관리
    # ============================================================
    def get_table_info_list(self):
        """테이블 정보 목록 조회
        
        Returns:
            list: 테이블명 목록
        """
        if not self.db_manager:
            return []
        
        try:
            return self.db_manager.get_table_info_list()
        except Exception as e:
            self.log(f"테이블 정보 목록 조회 오류: {e}")
            return []
    
    def get_table_details(self, table_nm):
        """테이블 상세 정보 조회
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            tuple: (테이블명, 설명, 소유자)
        """
        if not self.db_manager:
            return None
        
        try:
            return self.db_manager.get_table_details(table_nm)
        except Exception as e:
            self.log(f"{table_nm} 테이블 상세 정보 조회 오류: {e}")
            return None
    
    def save_table_info(self, table_nm, table_dc, table_ownership):
        """테이블 정보 저장
        
        Args:
            table_nm (str): 테이블명
            table_dc (str): 테이블 설명
            table_ownership (str): 테이블 소유자
            
        Returns:
            bool: 저장 성공 여부
        """
        if not self.db_manager:
            return False
        
        try:
            # 테이블명 필수 검사
            if not table_nm:
                self.log("테이블명은 필수 입력 항목입니다.")
                return False
                
            # 테이블 정보 저장
            self.db_manager.save_table_info(table_nm, table_dc, table_ownership)
            self.log(f"{table_nm} 테이블 정보가 저장되었습니다.")
            return True
        except Exception as e:
            self.log(f"테이블 정보 저장 오류: {e}")
            return False
    
    def delete_table_info(self, table_nm):
        """테이블 정보 삭제
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            bool: 삭제 성공 여부
        """
        if not self.db_manager:
            return False
        
        try:
            # 테이블 정보 삭제
            self.db_manager.delete_table_info(table_nm)
            self.log(f"{table_nm} 테이블 정보가 삭제되었습니다.")
            return True
        except Exception as e:
            self.log(f"테이블 정보 삭제 오류: {e}")
            return False
    
    # ============================================================
    # 자동화 설정 관리 (DELETE_INTERVAL 제거)
    # ============================================================
    def get_auto_config_list(self):
        """자동화 설정 목록 조회
        
        Returns:
            list: 테이블명 목록
        """
        if not self.db_manager:
            return []
        
        try:
            return self.db_manager.get_auto_config_list()
        except Exception as e:
            self.log(f"자동화 설정 목록 조회 오류: {e}")
            return []
    
    def get_auto_config_details(self, table_nm):
        """자동화 설정 상세 정보 조회
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            tuple: (소스 경로, 저장 경로, 자동화 간격, 사용 여부)
        """
        if not self.db_manager:
            return None
        
        try:
            return self.db_manager.get_auto_config_details(table_nm)
        except Exception as e:
            self.log(f"{table_nm} 자동화 설정 상세 정보 조회 오류: {e}")
            return None
    
    def save_auto_config(self, table_nm, src_path, dest_path, auto_interval, use_yn):
        """자동화 설정 저장 (DELETE_INTERVAL 제거)
        
        Args:
            table_nm (str): 테이블명
            src_path (str): 소스 파일 경로
            dest_path (str): 저장 파일 경로
            auto_interval (int): 자동화 간격 (분)
            use_yn (str): 사용 여부 (Y/N)
            
        Returns:
            bool: 저장 성공 여부
        """
        if not self.db_manager:
            return False
        
        try:
            # 테이블명 필수 검사
            if not table_nm:
                self.log("테이블명은 필수 입력 항목입니다.")
                return False
                
            # TABLE_INFO에 테이블 존재 여부 확인
            table_info = self.db_manager.get_table_details(table_nm)
            if not table_info:
                self.log(f"{table_nm} 테이블이 TABLE_INFO에 등록되어 있지 않습니다.")
                return False
            
            # 숫자 항목 변환
            try:
                auto_interval = int(auto_interval) if auto_interval else None
            except ValueError:
                self.log("인터벌 값은 숫자로 입력해야 합니다.")
                return False
            
            # 자동화 설정 저장
            self.db_manager.save_auto_config(
                table_nm, src_path, dest_path, 
                auto_interval, use_yn
            )
            self.log(f"{table_nm} 자동화 설정이 저장되었습니다.")
            return True
        except Exception as e:
            self.log(f"자동화 설정 저장 오류: {e}")
            return False
    
    def delete_auto_config(self, table_nm):
        """자동화 설정 삭제
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            bool: 삭제 성공 여부
        """
        if not self.db_manager:
            return False
        
        try:
            # 자동화 설정 삭제
            self.db_manager.delete_auto_config(table_nm)
            self.log(f"{table_nm} 자동화 설정이 삭제되었습니다.")
            return True
        except Exception as e:
            self.log(f"자동화 설정 삭제 오류: {e}")
            return False
    
    # ============================================================
    # 컬럼 매핑 관리 (사용하지 않지만 호환성 유지)
    # ============================================================
    def get_column_mapping_tables(self):
        """컬럼 매핑 설정된 테이블 목록 조회
        
        Returns:
            list: 테이블명 목록
        """
        if not self.db_manager:
            return []
        
        try:
            return self.db_manager.get_col_mapping_tables()
        except Exception as e:
            self.log(f"컬럼 매핑 테이블 목록 조회 오류: {e}")
            return []
    
    def get_column_mappings(self, table_nm):
        """컬럼 매핑 정보 조회
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            list: [(DB컬럼명, XML컬럼명, 중복체크여부), ...] 목록
        """
        if not self.db_manager:
            return []
        
        try:
            mappings = self.db_manager.get_column_mappings(table_nm)
            # 중복체크 여부를 'N'으로 기본값 설정하여 호환성 유지
            return [(db_col, xml_col, 'N') for db_col, xml_col in mappings]
        except Exception as e:
            self.log(f"{table_nm} 컬럼 매핑 정보 조회 오류: {e}")
            return []
    
    def save_column_mappings(self, table_nm, mappings):
        """컬럼 매핑 정보 저장
        
        Args:
            table_nm (str): 테이블명
            mappings (list): [(DB컬럼명, XML컬럼명, 중복체크여부), ...] 목록
            
        Returns:
            bool: 저장 성공 여부
        """
        if not self.db_manager:
            return False
        
        try:
            # 테이블명 필수 검사
            if not table_nm:
                self.log("테이블명은 필수 입력 항목입니다.")
                return False
            
            # 유효한 매핑 항목만 필터링
            valid_mappings = []
            for db_col, xml_col, dup_check in mappings:
                if db_col and xml_col:
                    valid_mappings.append((db_col, xml_col, dup_check))
            
            # 유효한 항목이 없는 경우
            if not valid_mappings:
                self.log("저장할 유효한 매핑 정보가 없습니다. DB 컬럼과 XML 컬럼이 모두 입력되어야 합니다.")
                return False
            
            # 컬럼 매핑 정보 저장
            self.db_manager.save_column_mappings(table_nm, valid_mappings)
            self.log(f"{table_nm} 컬럼 매핑 정보가 저장되었습니다.")
            return True
        except Exception as e:
            self.log(f"컬럼 매핑 정보 저장 오류: {e}")
            return False
    
    def delete_column_mappings(self, table_nm):
        """컬럼 매핑 정보 삭제
        
        Args:
            table_nm (str): 테이블명
            
        Returns:
            bool: 삭제 성공 여부
        """
        if not self.db_manager:
            return False
        
        try:
            # 컬럼 매핑 정보 삭제
            self.db_manager.delete_column_mappings(table_nm)
            self.log(f"{table_nm} 컬럼 매핑 정보가 삭제되었습니다.")
            return True
        except Exception as e:
            self.log(f"컬럼 매핑 정보 삭제 오류: {e}")
            return False