import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os


class OfflineRunView:
    """오프라인 모드 실행 탭 뷰
    
    XML 파일 파싱 및 CSV 변환을 위한 UI
    """
    
    def __init__(self, parent, execution_controller):
        """오프라인 실행 뷰 초기화
    
        Args:
            parent: 부모 위젯
            execution_controller: 실행 컨트롤러 객체
        """
        self.parent = parent
        self.execution_controller = execution_controller
        
        # 파싱 작업 상태
        self.is_parsing = False
        
        # 콜백 설정
        self.execution_controller.set_callbacks(
            progress_callback=self.update_progress,
            status_callback=self.update_status,
            log_callback=self.add_log,
            tree_update_callback=self.update_tree_columns,  # 트리뷰 컬럼 업데이트 콜백 추가
            tree_item_callback=self.add_tree_item          # 트리뷰 항목 추가 콜백 추가
        )
        
        # UI 생성
        self.create_ui()
    
    def create_ui(self):
        """UI 요소 생성"""
        # ------------------------------------------------------------
        # 1. 파일 선택 그룹
        # ------------------------------------------------------------
        self.create_file_select_section()
        
        # ------------------------------------------------------------
        # 2. 파싱 실행 그룹
        # ------------------------------------------------------------
        self.create_parse_control_section()
        
        # ------------------------------------------------------------
        # 3. 실행 로그 그룹
        # ------------------------------------------------------------
        self.create_log_section()
        
        # ------------------------------------------------------------
        # 4. 파싱 결과 그룹
        # ------------------------------------------------------------
        self.create_result_section()
    
    def create_file_select_section(self):
        """파일 선택 섹션 생성"""
        frame_file_select = tk.LabelFrame(self.parent, text="파일 선택", padx=10, pady=10)
        frame_file_select.pack(fill="x", padx=10, pady=5)
        
        # 파일 경로 선택
        tk.Label(frame_file_select, text="파일 경로 :").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.entry_select_path = tk.Entry(frame_file_select, width=60)
        self.entry_select_path.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        
        btn_browse_select = ttk.Button(
            frame_file_select, 
            text="찾아보기", 
            width=10,
            command=lambda: self.browse_path('select')
        )
        btn_browse_select.grid(row=0, column=2, padx=5, pady=5, ipadx=5)
        
        # 저장 경로 선택
        tk.Label(frame_file_select, text="저장 경로 :").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.entry_save_path = tk.Entry(frame_file_select, width=60)
        self.entry_save_path.grid(row=1, column=1, sticky="ew", padx=5, pady=5)
        
        # 현재 실행 파일 위치 기본값
        current_dir = os.getcwd()
        self.entry_save_path.insert(0, current_dir)
        
        btn_browse_save = ttk.Button(
            frame_file_select, 
            text="찾아보기", 
            width=10,
            command=lambda: self.browse_path('save')
        )
        btn_browse_save.grid(row=1, column=2, padx=5, pady=5, ipadx=5)
        
        # 그리드 열 설정
        frame_file_select.columnconfigure(1, weight=1)
    
    def create_parse_control_section(self):
        """파싱 제어 섹션 생성"""
        btn_frame = tk.Frame(self.parent)
        btn_frame.pack(fill="x", padx=10, pady=5)
        
        # 상태 표시 라벨
        self.lbl_parse_status = tk.Label(btn_frame, text="준비", font=("맑은 고딕", 12))
        self.lbl_parse_status.pack(side="left", padx=5)
        
        # 파싱 중지 버튼
        self.btn_stop_parse = ttk.Button(
            btn_frame, 
            text="파싱 중지", 
            width=10, 
            state="disabled",
            command=self.stop_parse
        )
        self.btn_stop_parse.pack(side="right", padx=5, pady=5, ipadx=5)
        
        # 파싱 시작 버튼
        self.btn_start_parse = ttk.Button(
            btn_frame, 
            text="파싱 시작", 
            width=10,
            command=self.start_parse
        )
        self.btn_start_parse.pack(side="right", padx=5, pady=5, ipadx=5)
    
    def create_log_section(self):
        """로그 섹션 생성"""
        frame_log = tk.LabelFrame(self.parent, text="실행 로그", padx=10, pady=10)
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)
        
        # 로그 텍스트 창
        self.text_log = tk.Text(frame_log, wrap="word", height=8)
        self.text_log.pack(side="left", fill="both", expand=True)
        
        # 스크롤바
        scrollbar_log = tk.Scrollbar(frame_log, command=self.text_log.yview)
        scrollbar_log.pack(side="right", fill="y")
        self.text_log.config(yscrollcommand=scrollbar_log.set)
    
    def create_result_section(self):
        """결과 섹션 생성"""
        frame_result = tk.LabelFrame(self.parent, text="파싱 결과", padx=10, pady=10)
        frame_result.pack(fill="both", expand=True, padx=10, pady=5)
        
        # 결과 트리뷰
        columns = ("col1", "col2", "col3", "col4")
        self.tree_result = ttk.Treeview(frame_result, columns=columns, show="headings", height=10)
        
        # 기본 열 헤더 설정
        for col in columns:
            self.tree_result.heading(col, text=col.title())
            self.tree_result.column(col, width=100)
        
        # 트리뷰 배치
        self.tree_result.grid(row=0, column=0, sticky="nsew")
        
        # 스크롤바
        scrollbar_y = tk.Scrollbar(frame_result, orient="vertical", command=self.tree_result.yview)
        scrollbar_y.grid(row=0, column=1, sticky="ns")
        
        scrollbar_x = tk.Scrollbar(frame_result, orient="horizontal", command=self.tree_result.xview)
        scrollbar_x.grid(row=1, column=0, sticky="ew")
        
        self.tree_result.config(xscrollcommand=scrollbar_x.set, yscrollcommand=scrollbar_y.set)
        
        # 그리드 설정
        frame_result.rowconfigure(0, weight=1)
        frame_result.columnconfigure(0, weight=1)
    
    # ============================================================
    # 이벤트 처리 함수들
    # ============================================================
    def browse_path(self, path_type):
        """파일 경로 선택 다이얼로그
        
        Args:
            path_type (str): 경로 유형 ('select' 또는 'save')
        """
        if path_type == 'select':
            # XML 파일 선택 다이얼로그
            file_path = filedialog.askopenfilename(
                filetypes=[("XML 파일", "*.xml"), ("모든 파일", "*.*")],
                initialdir=self.entry_select_path.get() if self.entry_select_path.get() else "/"
            )
            if file_path:
                self.entry_select_path.delete(0, tk.END)
                self.entry_select_path.insert(0, file_path)
        else:
            # 저장 경로 선택 다이얼로그
            dir_path = filedialog.askdirectory(
                initialdir=self.entry_save_path.get() if self.entry_save_path.get() else "/"
            )
            if dir_path:
                self.entry_save_path.delete(0, tk.END)
                self.entry_save_path.insert(0, dir_path)
    
    def add_log(self, message):
        """로그 메시지 추가
        
        Args:
            message (str): 로그 메시지
        """
        # 텍스트 위젯 존재 여부 확인
        if hasattr(self, 'text_log'):
            # 로그 메시지 추가
            self.text_log.insert(tk.END, message + "\n")
            self.text_log.see(tk.END)  # 최신 로그로 스크롤
            
            # UI 즉시 갱신
            self.parent.update_idletasks()
    
    def update_status(self, status, is_active=False):
        """상태 메시지 업데이트
        
        Args:
            status (str): 상태 메시지
            is_active (bool): 파싱 활성화 상태
        """
        # 상태 라벨 업데이트
        self.lbl_parse_status.config(text=status)
        
        # 파싱 활성화 상태에 따라 색상 변경
        if status == "파싱중":
            self.lbl_parse_status.config(fg="green")
            self.is_parsing = True
        elif status == "오류":
            self.lbl_parse_status.config(fg="red")
            self.is_parsing = False
        elif status == "종료":
            self.lbl_parse_status.config(fg="red")
            self.is_parsing = False
        else:
            self.lbl_parse_status.config(fg="black")
            self.is_parsing = False
        
        # 버튼 상태 업데이트
        self.update_button_state()
    
    def update_progress(self, table_nm, file_name, status, current=0, total=1):
        """진행 상태 업데이트 (온라인 모드와 호환성 유지)
        
        Args:
            table_nm: 테이블명 (무시됨)
            file_name: 파일명 (무시됨)
            status: 상태
            current: 현재 값
            total: 전체 값
        """
        # 트리뷰 갱신
        if hasattr(self, 'tree_result') and status == "파싱중":
            if current == 0 and total > 1:
                # 처리 시작 시 트리뷰 초기화
                for item in self.tree_result.get_children():
                    self.tree_result.delete(item)
    
    def update_button_state(self):
        """버튼 상태 업데이트"""
        if self.is_parsing:
            self.btn_start_parse.config(state="disabled")
            self.btn_stop_parse.config(state="normal")
        else:
            self.btn_start_parse.config(state="normal")
            self.btn_stop_parse.config(state="disabled")
    
    # ============================================================
    # 작업 제어 함수들
    # ============================================================
    def start_parse(self):
        """파싱 작업 시작"""
        # 입력값 유효성 검사
        xml_file_path = self.entry_select_path.get().strip()
        save_path = self.entry_save_path.get().strip()
        
        # 파일 경로 검증
        if not xml_file_path:
            messagebox.showerror("오류", "XML 파일을 선택해주세요.")
            return
        
        if not os.path.exists(xml_file_path):
            messagebox.showerror("오류", "선택한 XML 파일이 존재하지 않습니다.")
            return
        
        # 저장 경로 검증
        if not save_path:
            messagebox.showerror("오류", "저장 경로를 선택해주세요.")
            return
        
        # 로그 및 결과 초기화
        self.text_log.delete(1.0, tk.END)
        for item in self.tree_result.get_children():
            self.tree_result.delete(item)
        
        # 파싱 작업 시작
        success = self.execution_controller.start_parse(xml_file_path, save_path)
        
        if success:
            # 상태 업데이트
            self.update_status("파싱중", True)
        else:
            messagebox.showerror("오류", "파싱 작업을 시작할 수 없습니다.")
    
    def stop_parse(self):
        """파싱 작업 중지"""
        # 파싱 작업 중지
        success = self.execution_controller.stop_parse()
        
        if success:
            # 상태 업데이트
            self.update_status("종료", False)
        else:
            messagebox.showerror("오류", "파싱 작업을 중지할 수 없습니다.")
    
    # ============================================================
    # 데이터 처리 함수들
    # ============================================================
    def update_tree_columns(self, columns):
        """트리뷰 컬럼 업데이트
        
        Args:
            columns (list): 컬럼 목록
        """
        # 기존 컬럼 삭제
        self.tree_result["columns"] = columns
        
        # 컬럼 헤더 설정
        for col in columns:
            self.tree_result.heading(col, text=col)
            col_width = max(100, len(col) * 10)
            self.tree_result.column(col, width=col_width, minwidth=col_width)
    
    def add_tree_item(self, values):
        """트리뷰에 항목 추가
        
        Args:
            values (list): 데이터 값 목록
        """
        # 값이 없으면 무시
        if not values:
            return
            
        # 트리뷰에 항목 추가
        self.tree_result.insert("", "end", values=values)
        
        # UI 즉시 갱신
        self.parent.update_idletasks()