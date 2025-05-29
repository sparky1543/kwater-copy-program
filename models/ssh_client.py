import os
import paramiko
import time
import threading
from pathlib import Path


class SSHClient:
    """SSH/SFTP 연결 및 파일 전송 기능을 제공하는 클래스 (잘 되던 버전 기반)"""
    
    def __init__(self):
        """SSH 클라이언트 초기화"""
        # 서버 접속 정보
        self.server_ip = ""
        self.server_port = 22
        self.server_username = ""
        self.server_password = ""
        self.connection_timeout = 3  # 원래대로 3초
        
    def set_connection_info(self, ip, port, username, password, timeout=3):
        """서버 연결 정보 설정"""
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
    
    def get_client(self):
        """SSH 클라이언트 생성 및 연결 (잘 되던 버전)"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            self.server_ip,
            port=self.server_port,
            username=self.server_username,
            password=self.server_password,
            timeout=self.connection_timeout,
        )
        return ssh
    
    def test_connection(self):
        """SSH 연결 테스트 (잘 되던 버전)
        
        Returns:
            tuple: (연결 성공 여부, 오류 메시지)
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(
                self.server_ip,
                port=self.server_port,
                username=self.server_username,
                password=self.server_password,
                timeout=self.connection_timeout,
            )
            ssh.close()
            return True, ""
        except Exception as e:
            try:
                ssh.close()
            except Exception:
                pass
            return False, str(e)

    # ============================================================
    # SFTP 세션 관리 메서드들 (잘 되던 버전)
    # ============================================================
    def open_sftp(self):
        """새로운 SSH 연결과 SFTP 세션 열기 (잘 되던 버전)"""
        ssh = self.get_client()
        sftp = ssh.open_sftp()
        return ssh, sftp

    def close_sftp(self, ssh, sftp):
        """SFTP와 SSH 연결 닫기 (잘 되던 버전)"""
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass
        if ssh:
            try:
                ssh.close()
            except Exception:
                pass

    def ensure_remote_dir(self, sftp, remote_path):
        """원격 디렉토리 존재 확인 및 생성 (잘 되던 버전)"""
        try:
            sftp.stat(remote_path)
        except IOError:
            self._mkdir_p(sftp, remote_path)

    # ============================================================
    # 파일 전송 메서드들 (잘 되던 버전 - 단순함)
    # ============================================================
    def download_with_sftp(self, sftp, remote_path, local_path, file_name):
        """기존 SFTP 세션을 사용한 파일 다운로드 (잘 되던 단순 버전)"""
        os.makedirs(local_path, exist_ok=True)
        # 원격 경로는 항상 Unix 스타일 슬래시 사용
        remote_file = remote_path.rstrip('/') + '/' + file_name
        local_file = os.path.join(local_path, file_name)
        if os.path.exists(local_file):
            return False
        sftp.get(remote_file, local_file)
        return True

    def upload_with_sftp(self, sftp, local_path, remote_path, file_name):
        """기존 SFTP 세션을 사용한 파일 업로드 (잘 되던 단순 버전)"""
        local_file = os.path.join(local_path, file_name)
        # 원격 경로는 항상 Unix 스타일 슬래시 사용
        remote_file = remote_path.rstrip('/') + '/' + file_name
        if not os.path.exists(local_file):
            return False
        self.ensure_remote_dir(sftp, remote_path)
        sftp.put(local_file, remote_file)
        return True

    # ============================================================
    # 단일 연결 편의 메서드들 (잘 되던 버전)
    # ============================================================
    def list_remote_files(self, remote_path, file_pattern=None):
        """원격 디렉토리의 파일 목록 조회 (잘 되던 버전)"""
        ssh, sftp = None, None
        try:
            ssh, sftp = self.open_sftp()
            files = sftp.listdir(remote_path)
            if not file_pattern:
                return files
            return [f for f in files if file_pattern in f.lower()]
        finally:
            self.close_sftp(ssh, sftp)

    def download_file(self, remote_path, local_path, file_name):
        """단일 파일 다운로드 (잘 되던 버전)"""
        ssh, sftp = None, None
        try:
            ssh, sftp = self.open_sftp()
            return self.download_with_sftp(sftp, remote_path, local_path, file_name)
        finally:
            self.close_sftp(ssh, sftp)

    def download_files(self, remote_path, local_path, file_list):
        """여러 파일 다운로드 (잘 되던 버전)"""
        ssh, sftp = None, None
        results = {}
        try:
            ssh, sftp = self.open_sftp()
            for file_name in file_list:
                try:
                    results[file_name] = self.download_with_sftp(
                        sftp, remote_path, local_path, file_name
                    )
                except Exception:
                    results[file_name] = False
            return results
        finally:
            self.close_sftp(ssh, sftp)

    def upload_file(self, local_path, remote_path, file_name):
        """단일 파일 업로드 (잘 되던 버전)"""
        ssh, sftp = None, None
        try:
            ssh, sftp = self.open_sftp()
            return self.upload_with_sftp(sftp, local_path, remote_path, file_name)
        finally:
            self.close_sftp(ssh, sftp)

    def execute_command(self, command):
        """원격 서버에서 명령어 실행 (잘 되던 버전)"""
        ssh = None
        try:
            ssh = self.get_client()
            stdin, stdout, stderr = ssh.exec_command(command)
            return stdout.read().decode("utf-8"), stderr.read().decode("utf-8")
        finally:
            if ssh:
                ssh.close()

    def list_files_by_pattern(self, remote_path, table_nm):
        """테이블명 패턴에 맞는 XML 파일 목록 조회 (잘 되던 버전)"""
        try:
            files = self.list_remote_files(remote_path)
            return {
                f
                for f in files
                if f.lower().endswith(".xml") and f.lower().startswith(f"{table_nm.lower()}_")
            }
        except Exception:
            return set()

    def _mkdir_p(self, sftp, remote_path):
        """원격 디렉토리를 재귀적으로 생성 (잘 되던 버전)"""
        if not remote_path or remote_path == "/":
            return
        try:
            sftp.stat(remote_path)
            return
        except IOError:
            # 부모 디렉토리 경로도 Unix 스타일로 처리
            parent = '/'.join(remote_path.rstrip('/').split('/')[:-1])
            if parent and parent != remote_path:
                self._mkdir_p(sftp, parent)
            try:
                sftp.mkdir(remote_path)
            except Exception as e:
                try:
                    sftp.stat(remote_path)
                except IOError:
                    raise e