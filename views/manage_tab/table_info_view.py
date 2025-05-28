import tkinter as tk
from tkinter import ttk, messagebox


class TableInfoView:
    """테이블 정보 관리 뷰
    
    테이블 정보(설명, 소유자 등)를 관리하는 UI
    """
    
    def __init__(self, parent, settings_controller, is_scheduler_running_callback=None):
        """테이블 정보 뷰 초기화
        
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
        title_label = tk.Label(self.parent, text="테이블 정보 관리", font=("맑은 고딕", 15, "bold"))
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
        
        self.listbox_table_nms = tk.Listbox(frame_listbox, height=15, width=25, exportselection=False)
        self.listbox_table_nms.pack(side="left", fill="both", expand=True)
        self.listbox_table_nms.bind("<<ListboxSelect>>", self.on_table_selected)
        
        # 스크롤바
        self.scrollbar_list = tk.Scrollbar(frame_listbox, orient="vertical", command=self.listbox_table_nms.yview)
        self.scrollbar_list.pack(side="right", fill="y")
        self.listbox_table_nms.config(yscrollcommand=self.scrollbar_list.set)
        
        # ------------------------------------------------------------
        # 테이블 상세 정보
        # ------------------------------------------------------------
        self.frame_right = tk.Frame(frame_main)
        self.frame_right.pack(side="left", fill="both", expand=True, padx=20, pady=10)
        
        tk.Label(self.frame_right, text="테이블 정보", font=("맑은 고딕", 12)).pack()
        
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
        
        # 테이블 설명 입력
        tk.Label(frame_fields, text="TABLE_DC :").grid(row=1, column=0, sticky="w", pady=10)
        self.entry_table_dc = tk.Entry(frame_fields, width=40)
        self.entry_table_dc.grid(row=1, column=1, padx=5, pady=10)
        
        # 테이블 소유자 입력
        tk.Label(frame_fields, text="TABLE_OWNERSHIP :").grid(row=2, column=0, sticky="w", pady=10)
        self.entry_table_ownership = tk.Entry(frame_fields, width=40)
        self.entry_table_ownership.grid(row=2, column=1, padx=5, pady=10)
        
        # 버튼 영역
        frame_buttons = tk.Frame(frame_inner)
        frame_buttons.pack(pady=(100, 0))
        
        self.btn_add_table = tk.Button(
            frame_buttons, 
            text="추가하기", 
            command=self.add_table, 
            width=10
        )
        self.btn_add_table.pack(side="left", padx=5)
        
        self.btn_save = tk.Button(
            frame_buttons, 
            text="저장하기", 
            command=self.save_table_info, 
            width=10
        )
        self.btn_save.pack(side="left", padx=5)
        
        self.btn_delete_table = tk.Button(
            frame_buttons, 
            text="삭제하기", 
            command=self.delete_table, 
            width=10
        )
        self.btn_delete_table.pack(side="left", padx=5)
        
        # 테이블 목록 로드
        self.load_table_list()
    
    def log(self, message):
        """로그 메시지 출력
        
        Args:
            message (str): 로그 메시지
        """
        print(f"[TableInfoView] {message}")
    
    def load_table_list(self):
        """테이블 목록 로드"""
        try:
            # 테이블 목록 조회
            table_list = self.settings_controller.get_table_info_list()
            
            # 리스트박스 초기화 및 데이터 추가
            self.listbox_table_nms.delete(0, tk.END)
            for table in table_list:
                self.listbox_table_nms.insert(tk.END, table)
                
            # 목록이 있을 경우 첫 번째 항목 자동 선택
            if self.listbox_table_nms.size() > 0:
                self.listbox_table_nms.selection_set(0)
                self.listbox_table_nms.activate(0)
                self.listbox_table_nms.event_generate("<<ListboxSelect>>")
                self.on_table_selected()
        except Exception as e:
            messagebox.showerror("오류", f"테이블 목록 로드 중 오류 발생: {str(e)}")
    
    def on_table_selected(self, event=None):
        """테이블 선택 이벤트 처리
        
        Args:
            event: 이벤트 객체 (None일 수 있음)
        """
        # 선택된 테이블 확인
        selected_index = self.listbox_table_nms.curselection()
        if not selected_index:
            return
        
        table_nm = self.listbox_table_nms.get(selected_index[0])
        
        try:
            # 테이블 정보 조회
            table_info = self.settings_controller.get_table_details(table_nm)
            
            # 입력 필드 초기화
            self.clear_fields()
            
            # 조회 결과가 있으면 입력 필드에 표시
            if table_info:
                table_nm, table_dc, table_ownership = table_info
                
                self.entry_table_nm.insert(0, table_nm or "")
                self.entry_table_dc.insert(0, table_dc or "")
                self.entry_table_ownership.insert(0, table_ownership or "")
        except Exception as e:
            messagebox.showerror("오류", f"테이블 정보 조회 중 오류 발생: {str(e)}")
    
    def clear_fields(self):
        """입력 필드 초기화"""
        self.entry_table_nm.delete(0, tk.END)
        self.entry_table_dc.delete(0, tk.END)
        self.entry_table_ownership.delete(0, tk.END)
    
    def add_table(self):
        """새 테이블 정보 추가"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 입력 필드 초기화
        self.clear_fields()
        
        # 테이블명 입력 필드 포커스
        self.entry_table_nm.focus_set()
    
    def save_table_info(self):
        """테이블 정보 저장"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 입력값 가져오기
        table_nm = self.entry_table_nm.get().strip()
        table_dc = self.entry_table_dc.get().strip()
        table_ownership = self.entry_table_ownership.get().strip()
        
        # 테이블명 필수 확인
        if not table_nm:
            messagebox.showerror("입력 오류", "테이블명은 필수 입력 항목입니다.")
            return
        
        # 저장 확인
        confirm = messagebox.askyesno("저장 확인", f"{table_nm} 테이블 정보를 저장하시겠습니까?")
        if not confirm:
            return
        
        try:
            # 테이블 정보 저장
            success = self.settings_controller.save_table_info(
                table_nm, 
                table_dc, 
                table_ownership
            )
            
            if success:
                # 테이블 목록 새로고침
                self.load_table_list()
                
                # 저장한 테이블 선택
                self.select_table(table_nm)
                
                messagebox.showinfo("저장 완료", f"{table_nm} 테이블 정보가 저장되었습니다.")
        except Exception as e:
            messagebox.showerror("저장 오류", f"테이블 정보 저장 중 오류 발생: {str(e)}")
    
    def delete_table(self):
        """테이블 정보 삭제"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 선택된 테이블 확인
        selected_index = self.listbox_table_nms.curselection()
        if not selected_index:
            messagebox.showinfo("알림", "삭제할 테이블을 선택하세요.")
            return
        
        table_nm = self.listbox_table_nms.get(selected_index[0])
        
        # 삭제 확인
        confirm = messagebox.askyesno("삭제 확인", f"{table_nm} 테이블 정보를 삭제하시겠습니까?")
        if not confirm:
            return
        
        try:
            # 테이블 정보 삭제
            success = self.settings_controller.delete_table_info(table_nm)
            
            if success:
                # 테이블 목록 새로고침
                self.load_table_list()
                messagebox.showinfo("삭제 완료", f"{table_nm} 테이블 정보가 삭제되었습니다.")
                
                # 입력 필드 초기화
                self.clear_fields()
        except Exception as e:
            messagebox.showerror("삭제 오류", f"테이블 정보 삭제 중 오류 발생: {str(e)}")
    
    def select_table(self, table_nm):
        """테이블 목록에서 특정 테이블 선택
        
        Args:
            table_nm (str): 선택할 테이블명
        """
        for i in range(self.listbox_table_nms.size()):
            if self.listbox_table_nms.get(i) == table_nm:
                self.listbox_table_nms.selection_clear(0, tk.END)
                self.listbox_table_nms.selection_set(i)
                self.listbox_table_nms.see(i)
                self.listbox_table_nms.event_generate("<<ListboxSelect>>")
                break