import threading
import time
import datetime


class ConnectionController:
    """연결 상태 관리 컨트롤러
    
    SQLite 데이터베이스 및 이중 SSH 연결 상태를 관리하고 모니터링하는 클래스
    """
    
    def __init__(self, db_manager=None, linux_ssh_client=None, was_ssh_client=None):
        """연결 컨트롤러 초기화
        
        Args:
            db_manager: 데이터베이스 매니저 객체
            linux_ssh_client: Linux SSH 클라이언트 객체
            was_ssh_client: WAS SSH 클라이언트 객체
        """
        self.db_manager = db_manager
        self.linux_ssh_client = linux_ssh_client
        self.was_ssh_client = was_ssh_client
        
        # 연결 상태 변수
        self.is_online = False
        self.last_connection_check = None
        self.connection_error_msg = ""
        
        # 개별 연결 상태 변수
        self.linux_connected = False
        self.was_connected = False
        self.db_connected = False
        
        # 모니터링 스레드
        self.monitor_thread = None
        self.monitor_running = False
        
        # 상태 변경 콜백
        self.status_change_callback = None
        self.connection_message_callback = None
    
    def set_dependencies(self, db_manager, linux_ssh_client, was_ssh_client):
        """종속성 설정
        
        Args:
            db_manager: 데이터베이스 매니저 객체
            linux_ssh_client: Linux SSH 클라이언트 객체
            was_ssh_client: WAS SSH 클라이언트 객체
        """
        self.db_manager = db_manager
        self.linux_ssh_client = linux_ssh_client
        self.was_ssh_client = was_ssh_client
    
    def set_callbacks(self, status_change_callback=None, connection_message_callback=None):
        """콜백 함수 설정
        
        Args:
            status_change_callback: 연결 상태 변경 콜백 함수 (상태: bool)
            connection_message_callback: 연결 메시지 콜백 함수 (메시지: str, 오류: bool)
        """
        self.status_change_callback = status_change_callback
        self.connection_message_callback = connection_message_callback

    def check_linux_connection(self):
        """Linux 서버 연결 상태만 확인
        
        Returns:
            bool: Linux 연결 성공 여부
        """
        self.last_connection_check = datetime.datetime.now()
        
        # 접속 정보가 비어 있으면 실패 처리
        if not self.linux_ssh_client.server_ip or not self.linux_ssh_client.server_username:
            return False
        
        # SSH 연결 테스트
        linux_success, linux_error = self.linux_ssh_client.test_connection()
        self.linux_connected = linux_success
        
        if not linux_success:
            self.connection_error_msg = f"행안부 서버 연결 실패: {linux_error[:30]}..."
        
        return linux_success
    
    def check_was_connection(self):
        """WAS 서버 연결 상태만 확인
        
        Returns:
            bool: WAS 연결 성공 여부
        """
        self.last_connection_check = datetime.datetime.now()
        
        # 접속 정보가 비어 있으면 실패 처리
        if not self.was_ssh_client.server_ip or not self.was_ssh_client.server_username:
            return False
        
        # SSH 연결 테스트
        was_success, was_error = self.was_ssh_client.test_connection()
        self.was_connected = was_success
        
        if not was_success:
            self.connection_error_msg = f"WAS 서버 연결 실패: {was_error[:30]}..."
        
        return was_success
    
    def check_db_connection(self):
        """SQLite 데이터베이스 연결 상태 확인
        
        Returns:
            bool: DB 연결 성공 여부
        """
        if not self.db_manager:
            return False
        
        # SQLite는 파일 기반이므로 항상 연결 가능
        db_success, db_error = self.db_manager.test_connection()
        self.db_connected = db_success
        
        if not db_success:
            self.connection_error_msg = f"SQLite DB 연결 실패: {db_error[:30]}..."
        
        return db_success

    def check_connection_status(self):
        """전체 연결 상태 확인
        
        Returns:
            bool: 연결 성공 여부
        """
        self.last_connection_check = datetime.datetime.now()
        old_state = self.is_online  # 이전 상태 저장
        
        # 접속 정보가 비어 있으면 오프라인 처리
        if not self.linux_ssh_client.server_ip or not self.linux_ssh_client.server_username \
                or not self.was_ssh_client.server_ip or not self.was_ssh_client.server_username:
            self.is_online = False
            if self.status_change_callback:
                self.status_change_callback(False)
            if self.connection_message_callback:
                self.connection_message_callback("접속 정보가 설정되지 않았습니다. 오프라인 모드로 실행합니다.", True)
            return False
        
        # SQLite, Linux, WAS 연결 테스트를 각각 실행
        db_ok = self.check_db_connection()
        linux_ok = self.check_linux_connection()
        was_ok = self.check_was_connection()
        
        # 모든 연결이 성공해야 온라인 상태로 판정
        self.is_online = db_ok and linux_ok and was_ok
        
        # 상태 변경 시 콜백 호출
        if self.is_online != old_state and self.status_change_callback:
            self.status_change_callback(self.is_online)
        
        # 연결 메시지 업데이트
        self._update_connection_message()
        
        return self.is_online
    
    def _monitor_connection(self, interval):
        """백그라운드에서 연결 상태 모니터링
        
        Args:
            interval (int): 확인 주기 (초)
        """
        while self.monitor_running:
            try:
                self.check_connection_status()
            except Exception as e:
                print(f"연결 상태 확인 중 오류 발생: {e}")
                self.is_online = False
                self.connection_error_msg = f"확인 중 오류: {str(e)[:30]}..."
                
                # 상태 변경 콜백
                if self.status_change_callback:
                    self.status_change_callback(False)
                
                # 연결 메시지 업데이트
                self._update_connection_message()
            
            # 다음 확인까지 대기
            for _ in range(interval):
                if not self.monitor_running:
                    break
                time.sleep(1)
    
    def _update_connection_message(self):
        """연결 상태 메시지 업데이트"""
        # 연결 메시지 콜백이 없으면 종료
        if not self.connection_message_callback:
            return
        
        # 마지막 확인 시간 포맷
        check_time = None
        if self.last_connection_check:
            check_time = self.last_connection_check.strftime('%H:%M:%S')
        
        # 연결 상태에 따른 메시지
        if self.is_online:
            message = f"모든 서버에 정상적으로 연결되었습니다"
            if check_time:
                message += f" - 마지막 확인: {check_time}"
            
            self.connection_message_callback(message, False)
        else:
            # 개별 연결 상태 확인하여 구체적인 오류 메시지 생성
            failed_connections = []
            if not self.db_connected:
                failed_connections.append("SQLite")
            if not self.linux_connected:
                failed_connections.append("행안부 서버")
            if not self.was_connected:
                failed_connections.append("WAS 서버")
            
            if failed_connections:
                error = f"{', '.join(failed_connections)} 연결 실패"
            else:
                error = self.connection_error_msg or "알 수 없는 오류"
            
            message = f"서버에 연결할 수 없습니다 ({error})"
            if check_time:
                message += f" - 마지막 확인: {check_time}"
            
            self.connection_message_callback(message, True)
    
    def get_connection_info(self):
        """현재 연결 정보 반환
        
        Returns:
            dict: SSH 및 데이터베이스 연결 정보
        """
        result = {
            'is_online': self.is_online,
            'linux_connected': self.linux_connected,
            'was_connected': self.was_connected,
            'db_connected': self.db_connected,
            'last_check': self.last_connection_check,
            'error_message': self.connection_error_msg,
        }
        
        # Linux SSH 연결 정보
        if self.linux_ssh_client:
            result['linux_ssh'] = self.linux_ssh_client.get_connection_info()
        
        # WAS SSH 연결 정보
        if self.was_ssh_client:
            result['was_ssh'] = self.was_ssh_client.get_connection_info()
        
        # DB 연결 정보
        if self.db_manager:
            result['db'] = {'path': getattr(self.db_manager, 'db_path', 'data.db')}
        
        return result
    
    def update_linux_connection(self, ip, port, username, password, timeout=3):
        """Linux SSH 연결 정보 업데이트
        
        Args:
            ip (str): 서버 IP 주소
            port (int): 포트 번호
            username (str): 사용자명
            password (str): 비밀번호
            timeout (int, optional): 연결 타임아웃. Defaults to 3.
            
        Returns:
            bool: 연결 테스트 성공 여부
        """
        if not self.linux_ssh_client:
            return False
        
        # 연결 정보 업데이트
        self.linux_ssh_client.set_connection_info(ip, port, username, password, timeout)
        
        # 연결 테스트
        success = self.check_linux_connection()
        
        return success
    
    def update_was_connection(self, ip, port, username, password, timeout=3):
        """WAS SSH 연결 정보 업데이트
        
        Args:
            ip (str): 서버 IP 주소
            port (int): 포트 번호
            username (str): 사용자명
            password (str): 비밀번호
            timeout (int, optional): 연결 타임아웃. Defaults to 3.
            
        Returns:
            bool: 연결 테스트 성공 여부
        """
        if not self.was_ssh_client:
            return False
        
        # 연결 정보 업데이트
        self.was_ssh_client.set_connection_info(ip, port, username, password, timeout)
        
        # 연결 테스트
        success = self.check_was_connection()
        
        return success