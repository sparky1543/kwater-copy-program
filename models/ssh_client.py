import os
import paramiko
import time
import threading
from pathlib import Path


class SSHClient:
    """SSH/SFTP 연결 및 파일 전송 기능을 제공하는 클래스 (지속 연결 지원)"""
    
    def __init__(self):
        """SSH 클라이언트 초기화"""
        # 서버 접속 정보
        self.server_ip = ""
        self.server_port = 22
        self.server_username = ""
        self.server_password = ""
        self.connection_timeout = 3  # 연결 타임아웃 (초)
        
        # 지속 연결 관리
        self._ssh_client = None
        self._sftp_client = None
        self._connection_lock = threading.Lock()
        self._last_activity = None
        self._connection_timeout_seconds = 300  # 5분 후 자동 종료
        
    def set_connection_info(self, ip, port, username, password, timeout=3):
        """서버 연결 정보 설정
        
        Args:
            ip (str): 서버 IP 주소
            port (int): SSH 포트 번호
            username (str): SSH 사용자 계정
            password (str): SSH 계정 암호
            timeout (int, optional): 연결 타임아웃 (초). Defaults to 3.
        """
        # 연결 정보가 변경되면 기존 연결 종료
        if (self.server_ip != ip or self.server_port != port or 
            self.server_username != username or self.server_password != password):
            self.close_connection()
        
        self.server_ip = ip
        self.server_port = port
        self.server_username = username
        self.server_password = password
        self.connection_timeout = timeout
    
    def get_connection_info(self):
        """현재 연결 정보 반환"""
        return {
            'ip': self.server_ip,
            'port': self.server_port,
            'username': self.server_username,
            'password': self.server_password,
            'timeout': self.connection_timeout,
            'is_connected': self._is_connection_alive()
        }
    
    def _is_connection_alive(self):
        """연결 상태 확인"""
        try:
            if self._ssh_client is None:
                return False
            
            # 간단한 명령어로 연결 상태 확인
            transport = self._ssh_client.get_transport()
            return transport is not None and transport.is_active()
        except:
            return False
    
    def _ensure_connection(self):
        """연결 확보 (없으면 새로 생성, 끊어졌으면 재연결)
        
        Returns:
            tuple: (ssh_client, sftp_client) 또는 (None, None)
        """
        with self._connection_lock:
            try:
                # 기존 연결이 살아있는지 확인
                if self._ssh_client and self._sftp_client and self._is_connection_alive():
                    # 타임아웃 확인
                    if (self._last_activity and 
                        time.time() - self._last_activity > self._connection_timeout_seconds):
                        print(f"SSH 연결 타임아웃으로 재연결: {self.server_ip}")
                        self.close_connection()
                    else:
                        self._last_activity = time.time()
                        return self._ssh_client, self._sftp_client
                
                # 새 연결 생성
                print(f"새로운 SSH 연결 생성: {self.server_ip}")
                self._ssh_client = paramiko.SSHClient()
                self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self._ssh_client.connect(
                    self.server_ip, 
                    port=self.server_port, 
                    username=self.server_username, 
                    password=self.server_password, 
                    timeout=self.connection_timeout
                )
                
                self._sftp_client = self._ssh_client.open_sftp()
                self._last_activity = time.time()
                
                print(f"SSH 연결 성공: {self.server_ip}")
                return self._ssh_client, self._sftp_client
                
            except Exception as e:
                print(f"SSH 연결 실패: {self.server_ip}, 오류: {e}")
                self.close_connection()
                return None, None
    
    def close_connection(self):
        """연결 종료"""
        with self._connection_lock:
            try:
                if self._sftp_client:
                    self._sftp_client.close()
                    self._sftp_client = None
                    
                if self._ssh_client:
                    self._ssh_client.close()
                    self._ssh_client = None
                    
                print(f"SSH 연결 종료: {self.server_ip}")
            except Exception as e:
                print(f"SSH 연결 종료 중 오류: {e}")
            finally:
                self._ssh_client = None
                self._sftp_client = None
                self._last_activity = None
    
    def test_connection(self):
        """SSH 연결 테스트
        
        Returns:
            tuple: (연결 성공 여부, 오류 메시지)
        """
        ssh = None
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                self.server_ip, 
                port=self.server_port, 
                username=self.server_username, 
                password=self.server_password, 
                timeout=self.connection_timeout
            )
            return True, ""
        except Exception as e:
            return False, str(e)
        finally:
            if ssh:
                ssh.close()
    
    def get_client(self):
        """SSH 클라이언트 생성 및 연결 (일회용)
        
        Returns:
            paramiko.SSHClient: 연결된 SSH 클라이언트
            
        Raises:
            Exception: 연결 실패 시 예외 발생
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            self.server_ip, 
            port=self.server_port, 
            username=self.server_username, 
            password=self.server_password, 
            timeout=self.connection_timeout
        )
        return ssh
    
    def list_remote_files(self, remote_path, file_pattern=None):
        """원격 디렉토리의 파일 목록 조회
        
        Args:
            remote_path (str): 원격 디렉토리 경로
            file_pattern (str, optional): 파일명 패턴 (예: '.xml'). Defaults to None.
            
        Returns:
            list: 파일명 목록
            
        Raises:
            Exception: 연결 또는 디렉토리 접근 실패 시 예외 발생
        """
        ssh, sftp = self._ensure_connection()
        if not ssh or not sftp:
            raise Exception("SSH 연결을 설정할 수 없습니다")
        
        try:
            # 원격 디렉토리 파일 목록 조회
            all_files = sftp.listdir(remote_path)
            
            # 패턴이 없으면 모든 파일 반환
            if not file_pattern:
                return all_files
            
            # 패턴과 일치하는 파일만 필터링
            filtered_files = [f for f in all_files if file_pattern in f.lower()]
            return filtered_files
        except Exception as e:
            # 오류 발생 시 연결 재설정을 위해 연결 종료
            self.close_connection()
            raise e
    
    def download_file(self, remote_path, local_path, file_name):
        """단일 파일 다운로드
        
        Args:
            remote_path (str): 원격 디렉토리 경로
            local_path (str): 로컬 저장 디렉토리 경로
            file_name (str): 다운로드할 파일명
            
        Returns:
            bool: 다운로드 성공 여부
        """
        ssh, sftp = self._ensure_connection()
        if not ssh or not sftp:
            return False
        
        try:
            # 로컬 디렉토리가 없으면 생성
            os.makedirs(local_path, exist_ok=True)
            
            # 파일 경로 생성
            remote_file_path = os.path.join(remote_path, file_name)
            local_file_path = os.path.join(local_path, file_name)
            
            # 이미 존재하는 파일 확인
            if os.path.exists(local_file_path):
                return False
            
            # 파일 다운로드
            sftp.get(remote_file_path, local_file_path)
            return True
            
        except Exception as e:
            print(f"파일 다운로드 오류: {e}")
            # 오류 발생 시 연결 재설정을 위해 연결 종료
            self.close_connection()
            return False
    
    def upload_file(self, local_path, remote_path, file_name):
        """단일 파일 업로드
        
        Args:
            local_path (str): 로컬 파일 디렉토리 경로
            remote_path (str): 원격 저장 디렉토리 경로
            file_name (str): 업로드할 파일명
            
        Returns:
            bool: 업로드 성공 여부
        """
        ssh, sftp = self._ensure_connection()
        if not ssh or not sftp:
            return False
        
        try:
            # 파일 경로 생성
            local_file_path = os.path.join(local_path, file_name)
            remote_file_path = os.path.join(remote_path, file_name)
            
            # 로컬 파일 존재 확인
            if not os.path.exists(local_file_path):
                return False
            
            # 원격 디렉토리 존재 확인 및 생성
            try:
                sftp.stat(remote_path)
            except IOError:
                # 디렉토리가 없으면 생성 (mkdir -p와 유사한 기능)
                self._mkdir_p(sftp, remote_path)
            
            # 파일 업로드
            sftp.put(local_file_path, remote_file_path)
            return True
            
        except Exception as e:
            print(f"파일 업로드 오류: {e}")
            # 오류 발생 시 연결 재설정을 위해 연결 종료
            self.close_connection()
            return False
    
    def _mkdir_p(self, sftp, remote_path):
        """원격 디렉토리를 재귀적으로 생성 (mkdir -p와 유사)
        
        Args:
            sftp (paramiko.SFTPClient): SFTP 클라이언트
            remote_path (str): 생성할 원격 디렉토리 경로
        """
        if remote_path == '/':
            return
        
        try:
            sftp.stat(remote_path)
        except IOError:
            parent = os.path.dirname(remote_path)
            if parent:
                self._mkdir_p(sftp, parent)
            sftp.mkdir(remote_path)
    
    def execute_command(self, command):
        """원격 서버에서 명령어 실행
        
        Args:
            command (str): 실행할 명령어
            
        Returns:
            tuple: (stdout, stderr) 표준 출력과 오류
        """
        ssh, _ = self._ensure_connection()
        if not ssh:
            raise Exception("SSH 연결을 설정할 수 없습니다")
        
        try:
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout_data = stdout.read().decode('utf-8')
            stderr_data = stderr.read().decode('utf-8')
            return stdout_data, stderr_data
        except Exception as e:
            # 오류 발생 시 연결 재설정을 위해 연결 종료
            self.close_connection()
            raise e

    def list_files_by_pattern(self, remote_path, table_nm):
        """테이블명 패턴에 맞는 XML 파일 목록 조회
        
        Args:
            remote_path (str): 원격 디렉토리 경로
            table_nm (str): 테이블명 (파일명 패턴)
            
        Returns:
            set: 파일명 집합
        """
        try:
            # 원격 파일 목록 조회
            all_files = self.list_remote_files(remote_path)
            
            # XML 파일 중 테이블명으로 시작하는 파일만 필터링
            xml_files = {
                file_name for file_name in all_files 
                if file_name.lower().endswith('.xml') and 
                file_name.lower().startswith(f"{table_nm.lower()}_")
            }
            
            return xml_files
        except Exception as e:
            print(f"파일 목록 조회 오류: {e}")
            return set()
    
    def download_files_batch(self, remote_path, local_path, file_list):
        """여러 파일 배치 다운로드 (단일 세션 사용)
        
        Args:
            remote_path (str): 원격 디렉토리 경로
            local_path (str): 로컬 저장 디렉토리 경로
            file_list (list): 다운로드할 파일명 목록
            
        Returns:
            dict: {파일명: 성공여부} 딕셔너리
        """
        results = {}
        
        ssh, sftp = self._ensure_connection()
        if not ssh or not sftp:
            return {file_name: False for file_name in file_list}
        
        try:
            # 로컬 디렉토리가 없으면 생성
            os.makedirs(local_path, exist_ok=True)
            
            # 파일별 다운로드
            for file_name in file_list:
                try:
                    remote_file_path = os.path.join(remote_path, file_name)
                    local_file_path = os.path.join(local_path, file_name)
                    
                    # 이미 존재하는 파일 확인
                    if os.path.exists(local_file_path):
                        results[file_name] = False
                        continue
                    
                    # 파일 다운로드
                    sftp.get(remote_file_path, local_file_path)
                    results[file_name] = True
                    
                except Exception as e:
                    print(f"파일 다운로드 실패: {file_name}, 오류: {e}")
                    results[file_name] = False
            
            return results
            
        except Exception as e:
            print(f"배치 다운로드 오류: {e}")
            # 오류 발생 시 연결 재설정을 위해 연결 종료
            self.close_connection()
            return {file_name: False for file_name in file_list}
    
    def upload_files_batch(self, local_path, remote_path, file_list):
        """여러 파일 배치 업로드 (단일 세션 사용)
        
        Args:
            local_path (str): 로컬 파일 디렉토리 경로
            remote_path (str): 원격 저장 디렉토리 경로
            file_list (list): 업로드할 파일명 목록
            
        Returns:
            dict: {파일명: 성공여부} 딕셔너리
        """
        results = {}
        
        ssh, sftp = self._ensure_connection()
        if not ssh or not sftp:
            return {file_name: False for file_name in file_list}
        
        try:
            # 원격 디렉토리 존재 확인 및 생성
            try:
                sftp.stat(remote_path)
            except IOError:
                self._mkdir_p(sftp, remote_path)
            
            # 파일별 업로드
            for file_name in file_list:
                try:
                    local_file_path = os.path.join(local_path, file_name)
                    remote_file_path = os.path.join(remote_path, file_name)
                    
                    # 로컬 파일 존재 확인
                    if not os.path.exists(local_file_path):
                        results[file_name] = False
                        continue
                    
                    # 파일 업로드
                    sftp.put(local_file_path, remote_file_path)
                    results[file_name] = True
                    
                except Exception as e:
                    print(f"파일 업로드 실패: {file_name}, 오류: {e}")
                    results[file_name] = False
            
            return results
            
        except Exception as e:
            print(f"배치 업로드 오류: {e}")
            # 오류 발생 시 연결 재설정을 위해 연결 종료
            self.close_connection()
            return {file_name: False for file_name in file_list}
    
    def __del__(self):
        """소멸자: 연결 정리"""
        self.close_connection()