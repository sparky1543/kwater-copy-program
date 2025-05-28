import tkinter as tk
from tkinter import ttk, messagebox


class ConnectionView:
    """접속 정보 관리 뷰
    
    행안부 서버 및 WAS 서버 SSH 연결 정보를 관리하는 UI
    """
    
    def __init__(self, parent, settings_controller, connection_controller=None):
        """접속 정보 뷰 초기화
        
        Args:
            parent: 부모 위젯
            settings_controller: 설정 컨트롤러 객체
            connection_controller: 연결 컨트롤러 객체 (선택적)
        """
        self.parent = parent
        self.settings_controller = settings_controller
        self.connection_controller = connection_controller
        
        # 상태 메시지 라벨
        self.success_label_linux = None
        self.success_label_was = None
        
        # 타이머 ID 추적을 위한 변수
        self.linux_timer_id = None
        self.was_timer_id = None
        
        # UI 생성
        self.create_ui()
        
        # 현재 연결 정보 로드
        self.load_current_settings()
    
    def create_ui(self):
        """UI 구성 요소 생성"""
        # 제목 라벨
        title_label = tk.Label(self.parent, text="접속 정보 설정", font=("맑은 고딕", 15, "bold"))
        title_label.pack(pady=10)
        
        # 메인 프레임
        frame_conn = tk.Frame(self.parent)
        frame_conn.pack(fill="both", expand=True)
        
        # ------------------------------------------------------------
        # 행안부 서버 접속 정보 섹션
        # ------------------------------------------------------------
        frame_linux = tk.LabelFrame(frame_conn, text="행안부 접속 정보", padx=10, pady=10)
        frame_linux.pack(side="left", fill="both", expand=True, padx=10, pady=5)
        
        inner_frame_linux = tk.Frame(frame_linux)
        inner_frame_linux.pack(expand=True, padx=10, pady=10)
        
        # 서버 정보 입력 필드
        tk.Label(inner_frame_linux, text="서버 IP :", anchor="w").grid(row=0, column=0, sticky="w", pady=10)
        self.entry_linux_ip = tk.Entry(inner_frame_linux, width=20)
        self.entry_linux_ip.grid(row=0, column=1, padx=5, pady=10)
        
        tk.Label(inner_frame_linux, text="포트 번호 :", anchor="w").grid(row=1, column=0, sticky="w", pady=10)
        self.entry_linux_port = tk.Entry(inner_frame_linux, width=20)
        self.entry_linux_port.grid(row=1, column=1, padx=5, pady=10)
        
        tk.Label(inner_frame_linux, text="사용자명 :", anchor="w").grid(row=2, column=0, sticky="w", pady=10)
        self.entry_linux_user = tk.Entry(inner_frame_linux, width=20)
        self.entry_linux_user.grid(row=2, column=1, padx=5, pady=10)
        
        tk.Label(inner_frame_linux, text="비밀번호 :", anchor="w").grid(row=3, column=0, sticky="w", pady=10)
        self.entry_linux_pass = tk.Entry(inner_frame_linux, width=20, show="*")
        self.entry_linux_pass.grid(row=3, column=1, padx=5, pady=10)
        
        # 상태 메시지 라벨
        self.success_label_linux = tk.Label(inner_frame_linux, text="")
        self.success_label_linux.grid(row=5, column=0, columnspan=2, pady=5)
        
        # 저장 버튼
        btn_frame = tk.Frame(inner_frame_linux)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=10)
        
        self.btn_save_linux = tk.Button(btn_frame, text="저장하기", command=self.save_linux_info, width=10)
        self.btn_save_linux.pack(side="left", padx=5, pady=10)
        
        # 연결 테스트 버튼
        self.btn_test_linux = tk.Button(btn_frame, text="연결 테스트", command=self.test_linux_connection, width=10)
        self.btn_test_linux.pack(side="left", padx=5, pady=10)
        
        # ------------------------------------------------------------
        # WAS 서버 접속 정보 섹션
        # ------------------------------------------------------------
        frame_was = tk.LabelFrame(frame_conn, text="WAS 접속 정보", padx=10, pady=10)
        frame_was.pack(side="right", fill="both", expand=True, padx=10, pady=5)
        
        inner_frame_was = tk.Frame(frame_was)
        inner_frame_was.pack(expand=True, padx=10, pady=10)
        
        # 서버 정보 입력 필드
        tk.Label(inner_frame_was, text="서버 IP :", anchor="w").grid(row=0, column=0, sticky="w", pady=10)
        self.entry_was_ip = tk.Entry(inner_frame_was, width=20)
        self.entry_was_ip.grid(row=0, column=1, padx=5, pady=10)
        
        tk.Label(inner_frame_was, text="포트 번호 :", anchor="w").grid(row=1, column=0, sticky="w", pady=10)
        self.entry_was_port = tk.Entry(inner_frame_was, width=20)
        self.entry_was_port.grid(row=1, column=1, padx=5, pady=10)
        
        tk.Label(inner_frame_was, text="사용자명 :", anchor="w").grid(row=2, column=0, sticky="w", pady=10)
        self.entry_was_user = tk.Entry(inner_frame_was, width=20)
        self.entry_was_user.grid(row=2, column=1, padx=5, pady=10)
        
        tk.Label(inner_frame_was, text="비밀번호 :", anchor="w").grid(row=3, column=0, sticky="w", pady=10)
        self.entry_was_pass = tk.Entry(inner_frame_was, width=20, show="*")
        self.entry_was_pass.grid(row=3, column=1, padx=5, pady=10)
        
        # 상태 메시지 라벨
        self.success_label_was = tk.Label(inner_frame_was, text="")
        self.success_label_was.grid(row=5, column=0, columnspan=2, pady=5)
        
        # 저장 버튼
        btn_frame = tk.Frame(inner_frame_was)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=10)
        
        self.btn_save_was = tk.Button(btn_frame, text="저장하기", command=self.save_was_info, width=10)
        self.btn_save_was.pack(side="left", padx=5, pady=10)
        
        # 연결 테스트 버튼
        self.btn_test_was = tk.Button(btn_frame, text="연결 테스트", command=self.test_was_connection, width=10)
        self.btn_test_was.pack(side="left", padx=5, pady=10)
    
    def load_current_settings(self):
        """현재 연결 설정 로드"""
        # 설정 컨트롤러에서 현재 설정 조회
        connection_info = self.settings_controller.get_connection_info()
        
        # Linux SSH 연결 정보
        if 'linux_ssh' in connection_info:
            linux_info = connection_info['linux_ssh']
            self.entry_linux_ip.insert(0, linux_info.get('ip', ''))
            self.entry_linux_port.insert(0, linux_info.get('port', ''))
            self.entry_linux_user.insert(0, linux_info.get('username', ''))
            self.entry_linux_pass.insert(0, linux_info.get('password', ''))
        
        # WAS SSH 연결 정보
        if 'was_ssh' in connection_info:
            was_info = connection_info['was_ssh']
            self.entry_was_ip.insert(0, was_info.get('ip', ''))
            self.entry_was_port.insert(0, was_info.get('port', ''))
            self.entry_was_user.insert(0, was_info.get('username', ''))
            self.entry_was_pass.insert(0, was_info.get('password', ''))
    
    def save_linux_info(self):
        """행안부 서버 접속 정보 저장"""
        # 기존 타이머 취소
        self._cancel_timer(self.linux_timer_id)
        
        # 입력값 가져오기
        ip = self.entry_linux_ip.get().strip()
        port = self.entry_linux_port.get().strip()
        username = self.entry_linux_user.get().strip()
        password = self.entry_linux_pass.get()
        
        # 필수 입력값 확인
        if not ip or not port or not username or not password:
            messagebox.showerror("입력 오류", "모든 필드를 입력해주세요.")
            return
        
        try:
            # 포트 번호 유효성 검사
            port = int(port)
            if port <= 0 or port > 65535:
                raise ValueError("포트 번호는 1-65535 사이의 값이어야 합니다.")
                
            # SSH 정보 저장
            success = self.settings_controller.update_linux_info(ip, port, username, password)
            
            if success:
                # 성공 메시지 표시
                self.success_label_linux.config(text="행안부 접속 정보가 저장되었습니다.", fg="#4A4A4A")
                self.schedule_label_clear(self.success_label_linux, 'linux')
                
                # 연결 컨트롤러 경유로 연결 테스트
                if self.connection_controller:
                    self.connection_controller.check_connection_status()
            else:
                messagebox.showerror("저장 오류", "행안부 접속 정보 저장에 실패했습니다.")
                
        except ValueError as ve:
            messagebox.showerror("입력 오류", str(ve))
        except Exception as e:
            messagebox.showerror("저장 오류", f"행안부 접속 정보 저장 중 오류 발생: {str(e)}")
    
    def save_was_info(self):
        """WAS 서버 접속 정보 저장"""
        # 기존 타이머 취소
        self._cancel_timer(self.was_timer_id)
        
        # 입력값 가져오기
        ip = self.entry_was_ip.get().strip()
        port = self.entry_was_port.get().strip()
        username = self.entry_was_user.get().strip()
        password = self.entry_was_pass.get()
        
        # 필수 입력값 확인
        if not ip or not port or not username or not password:
            messagebox.showerror("입력 오류", "모든 필드를 입력해주세요.")
            return
        
        try:
            # 포트 번호 유효성 검사
            port = int(port)
            if port <= 0 or port > 65535:
                raise ValueError("포트 번호는 1-65535 사이의 값이어야 합니다.")
                
            # SSH 정보 저장
            success = self.settings_controller.update_was_info(ip, port, username, password)
            
            if success:
                # 성공 메시지 표시
                self.success_label_was.config(text="WAS 접속 정보가 저장되었습니다.", fg="#4A4A4A")
                self.schedule_label_clear(self.success_label_was, 'was')
                
                # 연결 컨트롤러 경유로 연결 테스트
                if self.connection_controller:
                    self.connection_controller.check_connection_status()
            else:
                messagebox.showerror("저장 오류", "WAS 접속 정보 저장에 실패했습니다.")
                
        except ValueError as ve:
            messagebox.showerror("입력 오류", str(ve))
        except Exception as e:
            messagebox.showerror("저장 오류", f"WAS 접속 정보 저장 중 오류 발생: {str(e)}")
    
    def test_linux_connection(self):
        """행안부 서버 연결 테스트"""
        # 기존 타이머 취소
        self._cancel_timer(self.linux_timer_id)
        
        # 입력값 가져오기
        ip = self.entry_linux_ip.get().strip()
        port = self.entry_linux_port.get().strip()
        username = self.entry_linux_user.get().strip()
        password = self.entry_linux_pass.get()
        
        # 필수 입력값 확인
        if not ip or not port or not username or not password:
            messagebox.showerror("입력 오류", "모든 필드를 입력해주세요.")
            return
        
        try:
            # 포트 번호 변환
            port = int(port)
            
            # 상태 변경
            self.btn_test_linux.config(state="disabled")
            self.success_label_linux.config(text="            연결 테스트 중...            ", fg="blue")
            self.parent.update_idletasks()
            
            # 연결 테스트 수행
            if self.connection_controller:
                # 연결 컨트롤러 사용 - Linux SSH 연결만 테스트
                # 연결 정보 업데이트
                self.connection_controller.linux_ssh_client.set_connection_info(ip, port, username, password)
                
                # Linux SSH 연결 테스트만 실행
                success = self.connection_controller.check_linux_connection()
                
                if success:
                    self.success_label_linux.config(text="            연결 성공            ", fg="green")
                    self.schedule_label_clear(self.success_label_linux, 'linux')
                else:
                    self.success_label_linux.config(text="            연결 실패            ", fg="red")
                    self.schedule_label_clear(self.success_label_linux, 'linux')
            else:
                # 설정 컨트롤러 사용
                self.settings_controller.update_linux_info(ip, port, username, password)
                self.success_label_linux.config(text="설정 저장됨 (연결 테스트 불가)", fg="#4A4A4A")
                self.schedule_label_clear(self.success_label_linux, 'linux')
                
        except Exception as e:
            self.success_label_linux.config(text=f"오류: {str(e)[:30]}...", fg="red")
            self.schedule_label_clear(self.success_label_linux, 'linux')
        finally:
            # 버튼 상태 복원
            self.btn_test_linux.config(state="normal")

    def test_was_connection(self):
        """WAS 서버 연결 테스트"""
        # 기존 타이머 취소
        self._cancel_timer(self.was_timer_id)
        
        # 입력값 가져오기
        ip = self.entry_was_ip.get().strip()
        port = self.entry_was_port.get().strip()
        username = self.entry_was_user.get().strip()
        password = self.entry_was_pass.get()
        
        # 필수 입력값 확인
        if not ip or not port or not username or not password:
            messagebox.showerror("입력 오류", "모든 필드를 입력해주세요.")
            return
        
        try:
            # 포트 번호 변환
            port = int(port)
            
            # 상태 변경
            self.btn_test_was.config(state="disabled")
            self.success_label_was.config(text="            연결 테스트 중...            ", fg="blue")
            self.parent.update_idletasks()
            
            # 연결 테스트 수행
            if self.connection_controller:
                # 연결 컨트롤러 사용 - WAS SSH 연결만 테스트
                # 연결 정보 업데이트
                self.connection_controller.was_ssh_client.set_connection_info(ip, port, username, password)
                
                # WAS SSH 연결 테스트만 실행
                success = self.connection_controller.check_was_connection()
                
                if success:
                    self.success_label_was.config(text="            연결 성공            ", fg="green")
                    self.schedule_label_clear(self.success_label_was, 'was')
                else:
                    self.success_label_was.config(text="            연결 실패            ", fg="red")
                    self.schedule_label_clear(self.success_label_was, 'was')
            else:
                # 설정 컨트롤러 사용
                self.settings_controller.update_was_info(ip, port, username, password)
                self.success_label_was.config(text="설정 저장됨 (연결 테스트 불가)", fg="#4A4A4A")
                self.schedule_label_clear(self.success_label_was, 'was')
                
        except Exception as e:
            self.success_label_was.config(text=f"오류: {str(e)[:30]}...", fg="red")
            self.schedule_label_clear(self.success_label_was, 'was')
        finally:
            # 버튼 상태 복원
            self.btn_test_was.config(state="normal")
    
    def _cancel_timer(self, timer_id):
        """기존 타이머 취소
        
        Args:
            timer_id: 취소할 타이머 ID
        """
        if timer_id:
            try:
                self.parent.after_cancel(timer_id)
            except (tk.TclError, Exception):
                pass  # 타이머가 이미 실행되었거나 없는 경우 무시
    
    def schedule_label_clear(self, label, timer_type):
        """라벨 텍스트 제거 타이머 설정
        
        Args:
            label: 클리어할 라벨 위젯
            timer_type: 타이머 유형 ('linux' 또는 'was')
        """
        def clear_label():
            try:
                # 라벨이 여전히 존재하는지 확인
                if label.winfo_exists():
                    label.config(text="")
                
                # 타이머 ID 리셋
                if timer_type == 'linux':
                    self.linux_timer_id = None
                else:
                    self.was_timer_id = None
            except (tk.TclError, Exception):
                pass  # 라벨이 존재하지 않는 경우 무시
        
        # 새 타이머 설정 및 ID 저장
        timer_id = self.parent.after(3000, clear_label)
        
        # 타이머 ID 저장
        if timer_type == 'linux':
            self.linux_timer_id = timer_id
        else:
            self.was_timer_id = timer_id