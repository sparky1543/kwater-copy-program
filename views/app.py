import tkinter as tk
from tkinter import ttk
import datetime
from models import DatabaseManager, SSHClient, DataProcessor, SchedulerManager
from controllers import ConnectionController, ExecutionController, SettingsController
from views.run_tab.online_view import OnlineRunView
from views.run_tab.offline_view import OfflineRunView
from views.manage_tab.connection_view import ConnectionView
from views.manage_tab.table_info_view import TableInfoView
from views.manage_tab.auto_config_view import AutoConfigView
from views.manage_tab.column_mapping_view import ColumnMappingView


class DataInsertApp:
    """데이터 복사 프로그램의 메인 애플리케이션 윈도우 클래스"""
    
    def __init__(self, root):
        """애플리케이션 초기화
        
        Args:
            root: Tkinter 루트 윈도우
        """
        # 메인 윈도우 설정
        self.root = root
        self.root.title("K-water File Copy Program")
        self.root.geometry("1000x780")
        self.root.resizable(True, True)
        
        # 글꼴 설정
        root.option_add("*Font", ("맑은 고딕", 13))
        
        # 스타일 설정
        style = ttk.Style()
        style.configure("Treeview", font=("맑은 고딕", 11))
        style.configure("Treeview.Heading", font=("맑은 고딕", 11))
        style.configure('TButton', font=('맑은 고딕', 12))
        
        # 모델 객체 생성
        self.db_manager = DatabaseManager()  # SQLite 사용
        self.linux_ssh_client = SSHClient()  # 행안부 서버용
        self.was_ssh_client = SSHClient()    # WAS 서버용
        self.data_processor = DataProcessor(self.db_manager)
        self.scheduler_manager = SchedulerManager(
            self.db_manager, 
            self.linux_ssh_client,
            self.was_ssh_client,
            self.data_processor
        )
        
        # 컨트롤러 객체 생성
        self.connection_controller = ConnectionController(
            self.db_manager, 
            self.linux_ssh_client,
            self.was_ssh_client
        )
        self.execution_controller = ExecutionController(
            self.db_manager, 
            self.linux_ssh_client,
            self.was_ssh_client,
            self.data_processor, 
            self.scheduler_manager
        )
        self.settings_controller = SettingsController(
            self.db_manager, 
            self.linux_ssh_client,
            self.was_ssh_client
        )
        
        # 현재 모드 및 탭 관련 변수
        self.current_mode = None  # 'online' 또는 'offline'
        self.run_tab_frame = None
        self.manage_tab_frame = None
        self.online_run_view = None
        self.offline_run_view = None
        self.current_manage_view = None
        
        # GUI 생성
        self.create_gui()
        
        # 연결 상태 모니터링 시작 및 콜백 설정
        self.connection_controller.set_callbacks(
            status_change_callback=self.on_connection_status_change,
            connection_message_callback=self.update_connection_message
        )
        
        # 초기 연결 상태 확인 (한 번만 수행)
        self.root.after(1000, self.connection_controller.check_connection_status)
    
    def create_gui(self):
        """GUI 생성"""
        # 탭 스타일 설정
        style = ttk.Style()
        style.configure("TNotebook.Tab", font=("맑은 고딕", 12))
        
        # ------------------------------------------------------------
        # 1. 연결 상태
        # ------------------------------------------------------------
        # 연결 상태 프레임
        self.frame_connection_status = tk.Frame(self.root)
        self.frame_connection_status.pack(fill="x", padx=10, pady=5)
        
        # 연결 상태 라벨
        self.lbl_connection_status = tk.Label(
            self.frame_connection_status, 
            text="◼ 연결 중...", 
            font=("맑은 고딕", 12),
            fg="blue"
        )
        self.lbl_connection_status.pack(side="left", padx=5)
        
        # 연결 메시지 라벨
        self.lbl_connection_message = tk.Label(
            self.frame_connection_status, 
            text="서버 연결 상태를 확인하는 중입니다...", 
            font=("맑은 고딕", 12)
        )
        self.lbl_connection_message.pack(side="left", padx=5)
        
        # 탭 노트북 생성
        self.notebook = ttk.Notebook(self.root, style="TNotebook")
        self.notebook.pack(fill="both", expand=True, padx=10, pady=5)

        # ------------------------------------------------------------
        # 2. 실행 탭
        # ------------------------------------------------------------
        self.run_tab_frame = tk.Frame(self.notebook)
        self.notebook.add(self.run_tab_frame, text="      실행      ")
        
        # ------------------------------------------------------------
        # 3. 관리 탭
        # ------------------------------------------------------------
        self.manage_tab_frame = tk.Frame(self.notebook)
        self.notebook.add(self.manage_tab_frame, text="      관리      ")
        
        # ------------------------------------------------------------
        # 3-1. [관리 탭] 네비게이션 바
        # ------------------------------------------------------------
        self.create_manage_tab_navbar()
        
        # 탭 변경 이벤트 바인딩
        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)
    
    def create_manage_tab_navbar(self):
        """관리 탭 네비게이션 바 생성"""
        navbar_frame = tk.LabelFrame(self.manage_tab_frame, text="정보 수정", padx=10, pady=5)
        navbar_frame.pack(side="top", fill="x", padx=10, pady=10)

        # 버튼 스타일 설정
        btn_width = 20
        btn_height = 1

        # 네비게이션 버튼 생성
        self.btn_connect = tk.Button(
            navbar_frame, text="접속 정보", width=btn_width, height=btn_height,
            command=lambda: self.show_manage_content("접속 정보")
        )
        self.btn_connect.pack(side="left", expand=True, fill="both", padx=5, pady=5)

        self.btn_table = tk.Button(
            navbar_frame, text="테이블 정보", width=btn_width, height=btn_height,
            command=lambda: self.show_manage_content("테이블 정보")
        )
        self.btn_table.pack(side="left", expand=True, fill="both", padx=5, pady=5)

        self.btn_auto = tk.Button(
            navbar_frame, text="자동화 정보", width=btn_width, height=btn_height,
            command=lambda: self.show_manage_content("자동화 정보")
        )
        self.btn_auto.pack(side="left", expand=True, fill="both", padx=5, pady=5)

        self.btn_mapping = tk.Button(
            navbar_frame, text="컬럼 매핑 정보", width=btn_width, height=btn_height,
            command=lambda: self.show_manage_content("컬럼 매핑 정보"), 
            state="disabled"  # 컬럼 매핑은 사용하지 않으므로 비활성화
        )
        self.btn_mapping.pack(side="left", expand=True, fill="both", padx=5, pady=5)

        # 콘텐츠 프레임 생성
        self.content_frame = tk.Frame(self.manage_tab_frame, padx=10, pady=10)
        self.content_frame.pack(fill="both", expand=True)
        
        # 초기 화면: 접속 정보
        self.update_button_style(self.btn_connect)
        self.show_manage_content("접속 정보")
    
    def update_button_style(self, selected_button):
        """네비게이션 버튼 스타일 업데이트
        
        Args:
            selected_button: 선택된 버튼 위젯
        """
        # 모든 버튼 스타일 초기화 (컬럼 매핑 제외)
        for btn in [self.btn_connect, self.btn_table, self.btn_auto]:
            btn.config(bg="SystemButtonFace")
        
        # 선택된 버튼의 배경색만 변경
        selected_button.config(bg="#e0e0e0")
    
    def show_manage_content(self, content_name):
        """관리 탭 내용 표시
        
        Args:
            content_name (str): 표시할 콘텐츠 이름
        """
        # 기존 위젯 제거
        for widget in self.content_frame.winfo_children():
            widget.destroy()
        
        # 연결 상태 확인
        is_online = self.connection_controller.is_online
        
        # 콘텐츠에 따라 다른 뷰 생성
        if content_name == "접속 정보":
            self.update_button_style(self.btn_connect)
            self.current_manage_view = ConnectionView(
                self.content_frame, 
                self.settings_controller,
                self.connection_controller
            )
        elif content_name == "테이블 정보" and is_online:
            self.update_button_style(self.btn_table)
            self.current_manage_view = TableInfoView(
                self.content_frame, 
                self.settings_controller,
                self.execution_controller.is_scheduler_running
            )
        elif content_name == "자동화 정보" and is_online:
            self.update_button_style(self.btn_auto)
            self.current_manage_view = AutoConfigView(
                self.content_frame, 
                self.settings_controller,
                self.execution_controller.is_scheduler_running
            )
        elif not is_online and content_name != "접속 정보":
            # 오프라인 상태에서는 접속 정보만 접근 가능
            self.update_button_style(self.btn_connect)
            self.current_manage_view = ConnectionView(
                self.content_frame, 
                self.settings_controller,
                self.connection_controller
            )
    
    def on_connection_status_change(self, is_online):
        """연결 상태 변경 이벤트 처리
        
        Args:
            is_online (bool): 연결 성공 여부
        """
        # 연결 상태 UI 업데이트
        if is_online:
            self.lbl_connection_status.config(text="◼ 연결 성공", fg="green")
        else:
            self.lbl_connection_status.config(text="◼ 연결 실패", fg="red")
        
        # 이전 모드 저장
        previous_mode = self.current_mode
        
        # 모드 상태 업데이트
        if is_online:
            self.current_mode = 'online'
        else:
            self.current_mode = 'offline'
        
        # 실행 컨트롤러에 모드 변경 알림
        self.execution_controller.set_online_mode(is_online)
        
        # 관리 탭 버튼 상태 업데이트
        self.update_manage_buttons()
        
        # 모드 변경 시 탭 내용 갱신
        if previous_mode != self.current_mode:
            self.update_run_tab_content()

    def update_connection_message(self, message, is_error):
        """연결 메시지 업데이트
        
        Args:
            message (str): 표시할 메시지
            is_error (bool): 오류 메시지 여부
        """
        # 모든 메시지에 동일한 색상 사용
        self.lbl_connection_message.config(text=message)
    
    def update_manage_buttons(self):
        """관리 탭 버튼 상태 업데이트"""
        # 관리 탭 생성 여부 확인
        if not hasattr(self, 'btn_table'):
            return
        
        if self.connection_controller.is_online:
            # 온라인 모드: 테이블 정보와 자동화 정보만 활성화 (컬럼 매핑은 비활성화 유지)
            self.btn_table.config(state="normal")
            self.btn_auto.config(state="normal")
            # self.btn_mapping.config(state="disabled")  # 항상 비활성화
        else:
            # 오프라인 모드: 접속 정보 버튼만 활성화
            self.btn_table.config(state="disabled")
            self.btn_auto.config(state="disabled")
            # self.btn_mapping.config(state="disabled")  # 항상 비활성화
            
            # 접속 정보 화면으로 전환
            self.show_manage_content("접속 정보")
    
    def update_run_tab_content(self):
        """실행 탭 내용 업데이트"""
        # 실행 탭 프레임이 없으면 작업 중단
        if not self.run_tab_frame:
            return
            
        # 실행 탭 프레임 내의 모든 위젯 제거
        for widget in self.run_tab_frame.winfo_children():
            widget.destroy()
        
        # 실행 중인 스케줄러 중지
        if hasattr(self, 'execution_controller') and self.execution_controller.is_scheduler_running():
            self.execution_controller.stop_data_insert()
        
        # 연결 상태에 따라 적절한 내용 생성
        if self.connection_controller.is_online:
            self.create_online_run_content()
        else:
            self.create_offline_run_content()
    
    def create_online_run_content(self):
        """온라인 모드 실행 탭 내용 생성"""
        # 온라인 모드 뷰 생성
        self.online_run_view = OnlineRunView(
            self.run_tab_frame, 
            self.execution_controller
        )
        self.current_mode = 'online'
    
    def create_offline_run_content(self):
        """오프라인 모드 실행 탭 내용 생성"""
        # 오프라인 모드 뷰 생성
        self.offline_run_view = OfflineRunView(
            self.run_tab_frame, 
            self.execution_controller
        )
        self.current_mode = 'offline'
    
    def on_tab_changed(self, event):
        """탭 전환 이벤트 처리
        
        Args:
            event: 이벤트 객체
        """
        # 현재 선택된 탭 인덱스 가져오기
        selected_tab = self.notebook.index("current")
        
        # 온라인 모드일 때만 진행 상태 새로고침
        if selected_tab == 0 and self.connection_controller.is_online:
            # 진행 상태 영역 새로고침
            if self.online_run_view:
                self.online_run_view.refresh_progress_view()


if __name__ == "__main__":
    root = tk.Tk()
    app = DataInsertApp(root)
    root.mainloop()