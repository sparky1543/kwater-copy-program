import tkinter as tk
from tkinter import ttk, messagebox


class AutoConfigView:
    """자동화 설정 관리 뷰
    
    테이블별 자동화 설정을 관리하는 UI
    """
    
    def __init__(self, parent, settings_controller, is_scheduler_running_callback=None):
        """자동화 설정 뷰 초기화
        
        Args:
            parent: 부모 위젯
            settings_controller: 설정 컨트롤러 객체
            is_scheduler_running_callback: 스케줄러 실행 상태 확인 콜백
        """
        self.parent = parent
        self.settings_controller = settings_controller
        self.is_scheduler_running_callback = is_scheduler_running_callback
        
        # 로그 콜백 설정
        self.settings_controller.set_log_callback(self.log)
        
        # UI 생성
        self.create_ui()
    
    def create_ui(self):
        """UI 구성 요소 생성"""
        # 제목 라벨
        title_label = tk.Label(self.parent, text="자동화 정보 관리", font=("맑은 고딕", 15, "bold"))
        title_label.pack(pady=10)
        
        # 메인 프레임
        frame_main = tk.Frame(self.parent)
        frame_main.pack(fill="both", expand=True)
        
        # ------------------------------------------------------------
        # 테이블 목록
        # ------------------------------------------------------------
        self.frame_left = tk.Frame(frame_main)
        self.frame_left.pack(side="left", fill="y", padx=10, pady=10)
        
        tk.Label(self.frame_left, text="테이블 목록", font=("맑은 고딕", 12)).pack()
        
        # 테이블 목록 리스트박스
        frame_listbox = tk.Frame(self.frame_left)
        frame_listbox.pack(fill="both", expand=True)
        
        self.listbox_tables = tk.Listbox(frame_listbox, height=15, width=25, exportselection=False)
        self.listbox_tables.pack(side="left", fill="both", expand=True)
        self.listbox_tables.bind("<<ListboxSelect>>", self.on_table_selected)
        
        # 스크롤바
        self.scrollbar_list = tk.Scrollbar(frame_listbox, orient="vertical", command=self.listbox_tables.yview)
        self.scrollbar_list.pack(side="right", fill="y")
        self.listbox_tables.config(yscrollcommand=self.scrollbar_list.set)
        
        # ------------------------------------------------------------
        # 자동화 상세 정보
        # ------------------------------------------------------------
        self.frame_right = tk.Frame(frame_main)
        self.frame_right.pack(side="left", fill="both", expand=True, padx=20, pady=10)
        
        tk.Label(self.frame_right, text="자동화 정보", font=("맑은 고딕", 12)).pack()
        
        # 정보 입력 영역
        frame_inner = tk.Frame(self.frame_right)
        frame_inner.place(relx=0.5, rely=0.5, anchor="center")
        
        # 입력 필드 배치
        frame_fields = tk.Frame(frame_inner)
        frame_fields.pack()
        
        # 테이블명 입력
        tk.Label(frame_fields, text="TABLE_NM :").grid(row=0, column=0, sticky="w", pady=10)
        self.entry_table_nm = tk.Entry(frame_fields, width=40)
        self.entry_table_nm.grid(row=0, column=1, padx=5, pady=10)
        
        # 소스 파일 경로 입력
        tk.Label(frame_fields, text="SRC_PATH :").grid(row=1, column=0, sticky="w", pady=10)
        self.entry_src_path = tk.Entry(frame_fields, width=40)
        self.entry_src_path.grid(row=1, column=1, padx=5, pady=10)
        
        # 목적지 파일 경로 입력
        tk.Label(frame_fields, text="DEST_PATH :").grid(row=2, column=0, sticky="w", pady=10)
        self.entry_dest_path = tk.Entry(frame_fields, width=40)
        self.entry_dest_path.grid(row=2, column=1, padx=5, pady=10)
        
        # 자동화 주기 입력
        tk.Label(frame_fields, text="AUTO_INTERVAL (분) :").grid(row=3, column=0, sticky="w", pady=10)
        self.entry_auto_interval = tk.Entry(frame_fields, width=40)
        self.entry_auto_interval.grid(row=3, column=1, padx=5, pady=10)
        
        # 사용 여부 입력
        tk.Label(frame_fields, text="USE_YN :").grid(row=4, column=0, sticky="w", pady=10)
        self.entry_use_yn = tk.Entry(frame_fields, width=40)
        self.entry_use_yn.grid(row=4, column=1, padx=5, pady=10)
        
        # 버튼 영역
        frame_buttons = tk.Frame(frame_inner)
        frame_buttons.pack(pady=(50, 0))
        
        self.btn_add = tk.Button(
            frame_buttons, 
            text="추가하기", 
            command=self.add_config, 
            width=10
        )
        self.btn_add.pack(side="left", padx=5)
        
        self.btn_save = tk.Button(
            frame_buttons, 
            text="저장하기", 
            command=self.save_config, 
            width=10
        )
        self.btn_save.pack(side="left", padx=5)
        
        self.btn_delete = tk.Button(
            frame_buttons, 
            text="삭제하기", 
            command=self.delete_config, 
            width=10
        )
        self.btn_delete.pack(side="left", padx=5)
        
        # 테이블 목록 로드
        self.load_table_list()
    
    def log(self, message):
        """로그 메시지 출력
        
        Args:
            message (str): 로그 메시지
        """
        print(f"[AutoConfigView] {message}")
    
    def load_table_list(self):
        """테이블 목록 로드"""
        try:
            # 기존 목록 초기화
            self.listbox_tables.delete(0, tk.END)
            
            # 자동화 설정 목록 조회
            table_list = self.settings_controller.get_auto_config_list()
            
            # 목록에 추가
            for table_nm in table_list:
                self.listbox_tables.insert(tk.END, table_nm)
            
        except Exception as e:
            messagebox.showerror("오류", f"테이블 목록 로드 중 오류 발생: {str(e)}")
    
    def on_table_selected(self, event=None):
        """테이블 선택 이벤트 처리
        
        Args:
            event: 이벤트 객체 (None일 수 있음)
        """
        # 선택된 테이블 확인
        selected_index = self.listbox_tables.curselection()
        if not selected_index:
            return
        
        table_nm = self.listbox_tables.get(selected_index[0])
        
        try:
            # 테이블 자동화 설정 정보 조회
            config = self.settings_controller.get_auto_config_details(table_nm)
            
            # 입력 필드 초기화
            self.clear_fields()
            
            # 조회 결과가 있으면 입력 필드에 표시
            if config:
                # config 튜플의 길이에 따라 처리
                if len(config) >= 4:
                    src_path, dest_path, auto_interval, use_yn = config[:4]
                    
                    self.entry_table_nm.insert(0, table_nm)
                    self.entry_src_path.insert(0, src_path or "")
                    self.entry_dest_path.insert(0, dest_path or "")
                    
                    if auto_interval is not None:
                        self.entry_auto_interval.insert(0, str(auto_interval))
                    
                    self.entry_use_yn.insert(0, use_yn or "Y")
                else:
                    # 예상보다 적은 값이 반환된 경우 안전하게 처리
                    self.entry_table_nm.insert(0, table_nm)
                    print(f"경고: {table_nm} 테이블의 자동화 설정에서 예상보다 적은 값이 반환됨: {config}")
                    
        except Exception as e:
            messagebox.showerror("오류", f"자동화 설정 정보 조회 중 오류 발생: {str(e)}")
            print(f"디버그: 오류 세부사항 - {str(e)}")
    
    def clear_fields(self):
        """입력 필드 초기화"""
        self.entry_table_nm.delete(0, tk.END)
        self.entry_src_path.delete(0, tk.END)
        self.entry_dest_path.delete(0, tk.END)
        self.entry_auto_interval.delete(0, tk.END)
        self.entry_use_yn.delete(0, tk.END)
    
    def add_config(self):
        """새 자동화 설정 추가"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 입력 필드 초기화
        self.clear_fields()
        
        # 테이블명 입력 필드 포커스
        self.entry_table_nm.focus_set()
        
        # 기본값 설정
        self.entry_use_yn.insert(0, "Y")
    
    def save_config(self):
        """자동화 설정 저장"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 입력값 가져오기
        table_nm = self.entry_table_nm.get().strip()
        src_path = self.entry_src_path.get().strip()
        dest_path = self.entry_dest_path.get().strip()
        auto_interval = self.entry_auto_interval.get().strip()
        use_yn = self.entry_use_yn.get().strip()
        
        # 유효성 검사
        if not table_nm:
            messagebox.showerror("입력 오류", "테이블명은 필수 입력 항목입니다.")
            return
        
        # 저장 확인
        confirm = messagebox.askyesno("저장 확인", f"{table_nm} 테이블의 자동화 설정을 저장하시겠습니까?")
        if not confirm:
            return
        
        try:
            # 자동화 설정 저장
            success = self.settings_controller.save_auto_config(
                table_nm,
                src_path,
                dest_path,
                auto_interval,
                use_yn
            )
            
            if success:
                # 테이블 목록 새로고침
                self.load_table_list()
                
                # 저장한 테이블 선택
                self.select_table(table_nm)
                
                messagebox.showinfo("저장 완료", f"{table_nm} 테이블의 자동화 설정이 저장되었습니다.")
        except Exception as e:
            messagebox.showerror("저장 오류", f"자동화 설정 저장 중 오류 발생: {str(e)}")
    
    def delete_config(self):
        """자동화 설정 삭제"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 선택된 테이블 확인
        selected_index = self.listbox_tables.curselection()
        if not selected_index:
            messagebox.showinfo("알림", "삭제할 테이블을 선택하세요.")
            return
        
        table_nm = self.listbox_tables.get(selected_index[0])
        
        # 삭제 확인
        confirm = messagebox.askyesno("삭제 확인", f"{table_nm} 테이블의 자동화 설정을 삭제하시겠습니까?")
        if not confirm:
            return
        
        try:
            # 자동화 설정 삭제
            success = self.settings_controller.delete_auto_config(table_nm)
            
            if success:
                # 테이블 목록 새로고침
                self.load_table_list()
                messagebox.showinfo("삭제 완료", f"{table_nm} 테이블의 자동화 설정이 삭제되었습니다.")
                
                # 입력 필드 초기화
                self.clear_fields()
        except Exception as e:
            messagebox.showerror("삭제 오류", f"자동화 설정 삭제 중 오류 발생: {str(e)}")
    
    def select_table(self, table_nm):
        """테이블 목록에서 특정 테이블 선택
        
        Args:
            table_nm (str): 선택할 테이블명
        """
        for i in range(self.listbox_tables.size()):
            if self.listbox_tables.get(i) == table_nm:
                self.listbox_tables.selection_clear(0, tk.END)
                self.listbox_tables.selection_set(i)
                self.listbox_tables.see(i)
                self.listbox_tables.event_generate("<<ListboxSelect>>")
                break