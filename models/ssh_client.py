import os
import paramiko
import time
import threading
from pathlib import Path


class SSHClient:
    """SSH/SFTP 연결 및 파일 전송 기능을 제공하는 클래스 (개선된 버전)"""
    
    def __init__(self):
        """SSH 클라이언트 초기화"""
        # 서버 접속 정보
        self.server_ip = ""
        self.server_port = 22
        self.server_username = ""
        self.server_password = ""
        self.connection_timeout = 10  # 타임아웃을 10초로 증가
        
    def set_connection_info(self, ip, port, username, password, timeout=10):
        """서버 연결 정보 설정
        
        Args:
            ip (str): 서버 IP 주소
            port (int): SSH 포트 번호
            username (str): SSH 사용자 계정
            password (str): SSH 계정 암호
            timeout (int, optional): 연결 타임아웃 (초). Defaults to 10.
        """
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
            'timeout': self.connection_timeout
        }
    
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
                try:
                    ssh.close()
                except:
                    pass
    
    def get_client(self):
        """SSH 클라이언트 생성 및 연결
        
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

    # ============================================================
    # 새로운 독립적인 SFTP 세션 관리 메서드들
    # ============================================================
    def open_sftp(self):
        """새로운 SSH 연결과 SFTP 세션 열기
        
        Returns:
            tuple: (ssh_client, sftp_client)
        """
        ssh = self.get_client()
        sftp = ssh.open_sftp()
        return ssh, sftp

    def close_sftp(self, ssh, sftp):
        """SFTP와 SSH 연결 닫기
        
        Args:
            ssh: SSH 클라이언트
            sftp: SFTP 클라이언트
        """
        if sftp:
            try:
                sftp.close()
            except:
                pass
        if ssh:
            try:
                ssh.close()
            except:
                pass

    def ensure_remote_dir(self, sftp, remote_path):
        """원격 디렉토리 존재 확인 및 생성
        
        Args:
            sftp: SFTP 클라이언트
            remote_path (str): 원격 디렉토리 경로
        """
        try:
            sftp.stat(remote_path)
        except IOError:
            self._mkdir_p(sftp, remote_path)

    def download_with_sftp(self, sftp, remote_path, local_path, file_name):
        """기존 SFTP 세션을 사용한 파일 다운로드
        
        Args:
            sftp: SFTP 클라이언트
            remote_path (str): 원격 디렉토리 경로
            local_path (str): 로컬 저장 디렉토리 경로
            file_name (str): 다운로드할 파일명
            
        Returns:
            bool: 다운로드 성공 여부
        """
        try:
            os.makedirs(local_path, exist_ok=True)
            remote_file = os.path.join(remote_path, file_name)
            local_file = os.path.join(local_path, file_name)
            
            if os.path.exists(local_file):
                return False
                
            sftp.get(remote_file, local_file)
            return True
        except Exception as e:
            print(f"파일 다운로드 오류 ({file_name}): {e}")
            return False

    def upload_with_sftp(self, sftp, local_path, remote_path, file_name):
        """기존 SFTP 세션을 사용한 파일 업로드
        
        Args:
            sftp: SFTP 클라이언트
            local_path (str): 로컬 파일 디렉토리 경로
            remote_path (str): 원격 저장 디렉토리 경로
            file_name (str): 업로드할 파일명
            
        Returns:
            bool: 업로드 성공 여부
        """
        try:
            local_file = os.path.join(local_path, file_name)
            remote_file = os.path.join(remote_path, file_name)
            
            if not os.path.exists(local_file):
                return False
                
            self.ensure_remote_dir(sftp, remote_path)
            sftp.put(local_file, remote_file)
            return True
        except Exception as e:
            print(f"파일 업로드 오류 ({file_name}): {e}")
            return False

    # ============================================================
    # 기존 호환성 유지를 위한 메서드들 (단일 연결)
    # ============================================================
    def list_remote_files(self, remote_path, file_pattern=None):
        """원격 디렉토리의 파일 목록 조회 (단일 연결 사용)
        
        Args:
            remote_path (str): 원격 디렉토리 경로
            file_pattern (str, optional): 파일명 패턴. Defaults to None.
            
        Returns:
            list: 파일명 목록
        """
        ssh, sftp = None, None
        try:
            ssh, sftp = self.open_sftp()
            all_files = sftp.listdir(remote_path)
            
            if not file_pattern:
                return all_files
            
            filtered_files = [f for f in all_files if file_pattern in f.lower()]
            return filtered_files
        except Exception as e:
            print(f"파일 목록 조회 오류: {e}")
            return []
        finally:
            self.close_sftp(ssh, sftp)

    def list_files_by_pattern(self, remote_path, table_nm):
        """테이블명 패턴에 맞는 XML 파일 목록 조회
        
        Args:
            remote_path (str): 원격 디렉토리 경로
            table_nm (str): 테이블명 (파일명 패턴)
            
        Returns:
            set: 파일명 집합
        """
        try:
            all_files = self.list_remote_files(remote_path)
            xml_files = {
                file_name for file_name in all_files 
                if file_name.lower().endswith('.xml') and 
                file_name.lower().startswith(f"{table_nm.lower()}_")
            }
            return xml_files
        except Exception as e:
            print(f"파일 목록 조회 오류: {e}")
            return set()

    def execute_command(self, command):
        """원격 서버에서 명령어 실행
        
        Args:
            command (str): 실행할 명령어
            
        Returns:
            tuple: (stdout, stderr) 표준 출력과 오류
        """
        ssh = None
        try:
            ssh = self.get_client()
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout_data = stdout.read().decode('utf-8')
            stderr_data = stderr.read().decode('utf-8')
            return stdout_data, stderr_data
        except Exception as e:
            raise e
        finally:
            if ssh:
                try:
                    ssh.close()
                except:
                    pass

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
            if parent and parent != remote_path:
                self._mkdir_p(sftp, parent)
            try:
                sftp.mkdir(remote_path)
            except Exception:
                # 동시에 생성된 경우 무시
                try:
                    sftp.stat(remote_path)
                except IOError:
                    raise

    # ============================================================
    # 호환성 유지를 위한 기존 메서드들 (사용 안 함 - 삭제 예정)
    # ============================================================
    def download_file(self, remote_path, local_path, file_name):
        """단일 파일 다운로드 (호환성용 - 사용 권장하지 않음)"""
        ssh, sftp = None, None
        try:
            ssh, sftp = self.open_sftp()
            return self.download_with_sftp(sftp, remote_path, local_path, file_name)
        finally:
            self.close_sftp(ssh, sftp)

    def upload_file(self, local_path, remote_path, file_name):
        """단일 파일 업로드 (호환성용 - 사용 권장하지 않음)"""
        ssh, sftp = None, None
        try:
            ssh, sftp = self.open_sftp()
            return self.upload_with_sftp(sftp, local_path, remote_path, file_name)
        finally:
            self.close_sftp(ssh, sftp)