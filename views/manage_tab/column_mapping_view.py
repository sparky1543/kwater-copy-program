import tkinter as tk
from tkinter import ttk, messagebox


class ColumnMappingView:
    """컬럼 매핑 정보 관리 뷰
    
    XML 태그와 DB 컬럼 간의 매핑 정보를 관리하는 UI
    """
    
    def __init__(self, parent, settings_controller, is_scheduler_running_callback=None):
        """컬럼 매핑 뷰 초기화
        
        Args:
            parent: 부모 위젯
            settings_controller: 설정 컨트롤러 객체
            is_scheduler_running_callback: 스케줄러 실행 상태 확인 콜백
        """
        self.parent = parent
        self.settings_controller = settings_controller
        self.is_scheduler_running_callback = is_scheduler_running_callback
        
        # 매핑 항목 저장 리스트
        self.mapping_entries = []
        
        # 툴팁 창
        self.tooltip = None
        
        # 로그 콜백 설정
        self.settings_controller.set_log_callback(self.log)
        
        # UI 생성
        self.create_ui()
    
    def create_ui(self):
        """UI 구성 요소 생성"""
        # 제목 라벨
        title_label = tk.Label(self.parent, text="컬럼 매핑 정보 관리", font=("맑은 고딕", 15, "bold"))
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
        # 매핑 정보 입력
        # ------------------------------------------------------------
        self.frame_right = tk.Frame(frame_main)
        self.frame_right.pack(side="left", fill="both", expand=True, padx=20, pady=10)
        
        tk.Label(self.frame_right, text="컬럼 매핑 정보", font=("맑은 고딕", 12)).pack()
        
        # 매핑 정보 컨테이너
        self.frame_inner = tk.Frame(self.frame_right)
        self.frame_inner.place(relx=0.5, rely=0.5, anchor="center")
        
        # 테이블 이름 입력
        frame_table = tk.Frame(self.frame_inner)
        frame_table.pack(fill="x")
        tk.Label(frame_table, text="TABLE_NM :").pack(side="left", padx=5)
        self.entry_table_nm = tk.Entry(frame_table, width=45)
        self.entry_table_nm.pack(side="left", padx=5)
        
        # 컬럼 헤더 영역
        frame_col_header = tk.Frame(self.frame_inner)
        frame_col_header.pack(fill="x", pady=(10, 0))
        
        # 헤더 라벨
        dup_check_label = tk.Label(frame_col_header, text="중복체크", font=("맑은 고딕", 9, "bold"))
        dup_check_label.pack(side="left", padx=(5, 0))
        
        # 도움말 아이콘
        help_icon = tk.Label(frame_col_header, text="?", font=("맑은 고딕", 9), fg="blue", cursor="hand2")
        help_icon.pack(side="left", padx=2)
        
        # 도움말 툴팁 이벤트
        help_icon.bind("<Enter>", lambda e: self.show_tooltip(e, "중복 데이터 처리에 사용할지 여부를 선택합니다. 체크된 컬럼들만 중복 확인에 사용됩니다."))
        help_icon.bind("<Leave>", lambda e: self.hide_tooltip())
        
        # 컬럼명 헤더
        db_col_label = tk.Label(frame_col_header, text="DB_COL_NM", font=("맑은 고딕", 9, "bold"))
        db_col_label.pack(side="left", padx=(55, 0))
        
        xml_col_label = tk.Label(frame_col_header, text="XML_COL_NM", font=("맑은 고딕", 9, "bold"))
        xml_col_label.pack(side="left", padx=(120, 0))
        
        # ------------------------------------------------------------
        # 매핑 입력 영역
        # ------------------------------------------------------------
        self.frame_canvas = tk.Frame(self.frame_inner)
        self.frame_canvas.pack(fill="both", expand=True, pady=5)
        self.frame_canvas.grid_rowconfigure(0, weight=1)
        self.frame_canvas.grid_columnconfigure(0, weight=1)
        
        # 스크롤바
        self.canvas_scrollbar = tk.Scrollbar(self.frame_canvas, orient="vertical")
        self.canvas_scrollbar.grid(row=0, column=1, sticky="ns")
        
        # 캔버스
        self.canvas_col = tk.Canvas(self.frame_canvas, yscrollcommand=self.canvas_scrollbar.set)
        self.canvas_col.grid(row=0, column=0, sticky="nsew")
        self.canvas_scrollbar.config(command=self.canvas_col.yview)
        
        # 매핑 항목 프레임
        self.frame_mappings = tk.Frame(self.canvas_col)
        self.canvas_col.create_window((0, 0), window=self.frame_mappings, anchor="nw")
        
        # 스크롤 이벤트
        self.canvas_col.bind("<MouseWheel>", self.on_mousewheel)  # Windows/MacOS
        self.canvas_col.bind("<Button-4>", self.on_mousewheel)  # Linux 위로 스크롤
        self.canvas_col.bind("<Button-5>", self.on_mousewheel)  # Linux 아래로 스크롤
        
        # 행 추가 영역
        frame_add_row = tk.Frame(self.frame_inner)
        frame_add_row.pack(fill="x", pady=5)
        
        # 행 추가 버튼
        self.add_row_label = tk.Label(
            frame_add_row, 
            text="➕ 행 추가", 
            font=("맑은 고딕", 9), 
            fg="blue", 
            cursor="hand2"
        )
        self.add_row_label.pack(side="right", padx=5)
        
        # 행 추가 이벤트
        self.add_row_label.bind("<Button-1>", lambda event: self.add_mapping_row())
        self.add_row_label.bind("<Enter>", lambda event: self.on_add_hover_enter())
        self.add_row_label.bind("<Leave>", lambda event: self.on_add_hover_leave())
        
        # 버튼 영역
        frame_buttons = tk.Frame(self.frame_inner)
        frame_buttons.pack(fill="x", pady=10)
        
        button_container = tk.Frame(frame_buttons)
        button_container.pack(side="top")
        
        self.btn_new = tk.Button(
            button_container, 
            text="추가하기", 
            command=self.add_new_mapping, 
            width=10
        )
        self.btn_new.pack(side="left", padx=5)
        
        self.btn_save = tk.Button(
            button_container, 
            text="저장하기", 
            command=self.save_column_mappings, 
            width=10
        )
        self.btn_save.pack(side="left", padx=5)
        
        self.btn_delete = tk.Button(
            button_container, 
            text="삭제하기", 
            command=self.delete_column_mappings, 
            width=10
        )
        self.btn_delete.pack(side="left", padx=5)
        
        # 기본 입력 행 추가
        for _ in range(3):
            self.add_mapping_row()
        
        # 크기 변경 이벤트
        self.frame_mappings.bind("<Configure>", self.on_frame_configure)
        
        # 테이블 목록 로드
        self.load_table_list()
    
    def log(self, message):
        """로그 메시지 출력
        
        Args:
            message (str): 로그 메시지
        """
        print(f"[ColumnMappingView] {message}")
    
    # ============================================================
    # 테이블 목록 관련 함수들
    # ============================================================
    def load_table_list(self):
        """테이블 목록 로드"""
        try:
            # 컬럼 매핑 테이블 목록 조회
            table_list = self.settings_controller.get_column_mapping_tables()
            
            # 리스트박스 초기화 및 데이터 추가
            self.listbox_tables.delete(0, tk.END)
            for table in table_list:
                self.listbox_tables.insert(tk.END, table)
                
            # 첫 번째 항목 자동 선택
            if self.listbox_tables.size() > 0:
                self.listbox_tables.selection_set(0)
                self.listbox_tables.activate(0)
                self.listbox_tables.event_generate("<<ListboxSelect>>")
                self.on_table_selected()
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
            # 테이블명 표시
            self.entry_table_nm.delete(0, tk.END)
            self.entry_table_nm.insert(0, table_nm)
            
            # 컬럼 매핑 정보 조회
            mappings = self.settings_controller.get_column_mappings(table_nm)
            
            # 기존 매핑 행 제거
            for _, _, frame_row, _, _ in self.mapping_entries:
                frame_row.destroy()
            self.mapping_entries.clear()
            
            # 매핑 데이터 표시
            if mappings:
                for db_col, xml_col, dup_check_yn in mappings:
                    self.add_mapping_row(db_col, xml_col, dup_check_yn)
            else:
                # 매핑이 없는 경우 빈 행 추가
                for _ in range(3):
                    self.add_mapping_row()
            
            # 스크롤 위치 초기화
            self.canvas_col.yview_moveto(0)
            
        except Exception as e:
            messagebox.showerror("오류", f"{table_nm} 컬럼 매핑 정보 조회 중 오류 발생: {str(e)}")
    
    # ============================================================
    # 매핑 행 관련 함수들
    # ============================================================
    def add_mapping_row(self, db_col="", xml_col="", dup_check="N"):
        """컬럼 매핑 행 추가
        
        Args:
            db_col (str): DB 컬럼명
            xml_col (str): XML 컬럼명
            dup_check (str): 중복 체크 여부 ('Y' 또는 'N')
            
        Returns:
            tuple: 생성된 위젯 참조 (entry_db, entry_xml, var_check)
        """
        # 행 컨테이너 프레임
        frame_row = tk.Frame(self.frame_mappings)
        frame_row.pack(fill="x", pady=2)
        
        # 중복체크 체크박스
        var_check = tk.BooleanVar(value=True if dup_check and dup_check.upper() == 'Y' else False)
        check_dup = tk.Checkbutton(frame_row, variable=var_check)
        check_dup.pack(side="left", padx=20)
        
        # DB 컬럼명 입력
        entry_db = tk.Entry(frame_row, width=20)
        entry_db.insert(0, db_col)
        entry_db.pack(side="left", padx=5)
        
        tk.Label(frame_row, text="-", font=("맑은 고딕", 12)).pack(side="left")
        
        # XML 컬럼명 입력
        entry_xml = tk.Entry(frame_row, width=20)
        entry_xml.insert(0, xml_col)
        entry_xml.pack(side="left", padx=5)
        
        # 행 삭제 라벨
        delete_label = tk.Label(frame_row, text="✕", font=("맑은 고딕", 9), fg="red", cursor="hand2")
        delete_label.pack(side="left", padx=5)
        
        # 삭제 이벤트
        delete_label.bind("<Button-1>", lambda event, fr=frame_row, e=(entry_db, entry_xml, var_check): self.delete_mapping_row(fr, e))
        delete_label.bind("<Enter>", lambda event, label=delete_label: self.on_delete_hover_enter(label))
        delete_label.bind("<Leave>", lambda event, label=delete_label: self.on_delete_hover_leave(label))
        
        # 스크롤 이벤트 바인딩
        for widget in [frame_row, check_dup, entry_db, entry_xml, delete_label]:
            widget.bind("<MouseWheel>", self.on_mousewheel)  # Windows/MacOS
            widget.bind("<Button-4>", self.on_mousewheel)  # Linux 위로 스크롤
            widget.bind("<Button-5>", self.on_mousewheel)  # Linux 아래로 스크롤
        
        # 매핑 정보 저장
        row_data = (entry_db, entry_xml, frame_row, delete_label, var_check)
        self.mapping_entries.append(row_data)
        
        # 스크롤 영역 업데이트
        self.on_frame_configure()
        
        return entry_db, entry_xml, var_check
    
    def delete_mapping_row(self, frame_row, entries, event=None):
        """컬럼 매핑 행 삭제
        
        Args:
            frame_row: 행 프레임 위젯
            entries: 행 내 위젯 참조 튜플
            event: 이벤트 객체 (None일 수 있음)
        """
        # 삭제 확인
        confirm = messagebox.askyesno("행 삭제 확인", "이 행을 삭제하시겠습니까?")
        if not confirm:
            return
        
        # 매핑 목록에서 항목 제거
        for i, (entry_db, entry_xml, fr, _, _) in enumerate(self.mapping_entries):
            if fr == frame_row:
                self.mapping_entries.pop(i)
                break
        
        # UI에서 행 제거
        frame_row.destroy()
        
        # 모든 행이 삭제된 경우 빈 행 추가
        if not self.mapping_entries:
            self.add_mapping_row()
        
        # 스크롤 영역 업데이트
        self.on_frame_configure()
    
    # ============================================================
    # 버튼 액션 함수들
    # ============================================================
    def add_new_mapping(self):
        """새 컬럼 매핑 추가"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 테이블명 초기화
        self.entry_table_nm.delete(0, tk.END)
        
        # 기존 매핑 행 제거
        for _, _, frame_row, _, _ in self.mapping_entries:
            frame_row.destroy()
        self.mapping_entries.clear()
        
        # 리스트 선택 해제
        self.listbox_tables.selection_clear(0, tk.END)
        
        # 빈 매핑 행 추가
        for _ in range(3):
            self.add_mapping_row()
        
        # 테이블명 입력 필드에 포커스
        self.entry_table_nm.focus_set()
    
    def save_column_mappings(self):
        """컬럼 매핑 정보 저장"""
        # 스케줄러 실행 상태 확인
        if self.is_scheduler_running_callback and self.is_scheduler_running_callback():
            messagebox.showwarning("경고", "스케줄러가 실행 중입니다.\n중단 후 다시 시도해주세요.")
            return
            
        # 테이블명 가져오기
        table_nm = self.entry_table_nm.get().strip()
        
        # 테이블명 필수 확인
        if not table_nm:
            messagebox.showerror("입력 오류", "테이블명은 필수 입력 항목입니다.")
            return
        
        # 매핑 정보 수집
        mappings = []
        for entry_db, entry_xml, _, _, var_check in self.mapping_entries:
            db_col = entry_db.get().strip()
            xml_col = entry_xml.get().strip()
            dup_check = 'Y' if var_check.get() else 'N'
            
            # 유효한 항목만 추가
            if db_col and xml_col:
                mappings.append((db_col, xml_col, dup_check))
        
        # 저장 확인
        confirm = messagebox.askyesno("저장 확인", f"{table_nm} 테이블의 컬럼 매핑 정보를 저장하시겠습니까?")
        if not confirm:
            return
        
        try:
            # 컬럼 매핑 정보 저장
            success = self.settings_controller.save_column_mappings(table_nm, mappings)
            
            if success:
                # 테이블 목록 새로고침
                self.load_table_list()
                
                # 저장한 테이블 선택
                self.select_table(table_nm)
                
                messagebox.showinfo("저장 완료", f"{table_nm} 테이블의 컬럼 매핑 정보가 저장되었습니다.")
        except Exception as e:
            messagebox.showerror("저장 오류", f"컬럼 매핑 정보 저장 중 오류 발생: {str(e)}")
    
    def delete_column_mappings(self):
        """컬럼 매핑 정보 삭제"""
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
        confirm = messagebox.askyesno("삭제 확인", f"{table_nm} 테이블의 컬럼 매핑 정보를 삭제하시겠습니까?")
        if not confirm:
            return
        
        try:
            # 컬럼 매핑 정보 삭제
            success = self.settings_controller.delete_column_mappings(table_nm)
            
            if success:
                # 테이블 목록 새로고침
                self.load_table_list()
                messagebox.showinfo("삭제 완료", f"{table_nm} 테이블의 컬럼 매핑 정보가 삭제되었습니다.")
                
                # 입력 필드 초기화
                self.entry_table_nm.delete(0, tk.END)
                
                # 기존 매핑 행 제거
                for _, _, frame_row, _, _ in self.mapping_entries:
                    frame_row.destroy()
                self.mapping_entries.clear()
                
                # 빈 매핑 행 추가
                for _ in range(3):
                    self.add_mapping_row()
        except Exception as e:
            messagebox.showerror("삭제 오류", f"컬럼 매핑 정보 삭제 중 오류 발생: {str(e)}")
    
    # ============================================================
    # UI 이벤트 함수들
    # ============================================================
    def on_add_hover_enter(self):
        """행 추가 버튼 마우스 진입 이벤트"""
        self.add_row_label.config(fg="darkblue", font=("맑은 고딕", 9, "bold"))
    
    def on_add_hover_leave(self):
        """행 추가 버튼 마우스 이탈 이벤트"""
        self.add_row_label.config(fg="blue", font=("맑은 고딕", 9))
    
    def on_delete_hover_enter(self, label):
        """행 삭제 버튼 마우스 진입 이벤트"""
        label.config(fg="darkred", font=("맑은 고딕", 9, "bold"))
    
    def on_delete_hover_leave(self, label):
        """행 삭제 버튼 마우스 이탈 이벤트"""
        label.config(fg="red", font=("맑은 고딕", 9))
    
    def on_frame_configure(self, event=None):
        """프레임 크기 변경 이벤트"""
        # 스크롤 영역 재계산
        self.canvas_col.configure(scrollregion=self.canvas_col.bbox("all"))
        
        # 캔버스 너비 조정
        if event:
            width = event.width
            self.canvas_col.itemconfig(self.canvas_col.find_all()[0], width=width)
    
    def on_mousewheel(self, event):
        """마우스 휠 스크롤 이벤트"""
        # Linux 시스템 이벤트
        if event.num == 4:
            self.canvas_col.yview_scroll(-1, "units")
        elif event.num == 5:
            self.canvas_col.yview_scroll(1, "units")
        else:
            # Windows/MacOS 이벤트
            delta = event.delta
            if abs(delta) >= 120:
                delta = delta // 120
            else:
                delta = -delta
            self.canvas_col.yview_scroll(-delta, "units")
    
    def show_tooltip(self, event, text):
        """툴팁 표시
        
        Args:
            event: 이벤트 객체
            text: 툴팁 텍스트
        """
        # 툴팁 위치 계산
        x, y, _, _ = event.widget.bbox("insert")
        x += event.widget.winfo_rootx() + 25
        y += event.widget.winfo_rooty() + 20
        
        # 툴팁 창 생성
        self.tooltip = tk.Toplevel(event.widget)
        self.tooltip.wm_overrideredirect(True)
        self.tooltip.wm_geometry(f"+{x}+{y}")
        
        # 툴팁 내용
        frame = tk.Frame(self.tooltip, background="#ffffe0", borderwidth=1, relief="solid")
        frame.pack(ipadx=5, ipady=3)
        
        label = tk.Label(
            frame, 
            text=text, 
            justify="left", 
            background="#ffffe0", 
            font=("맑은 고딕", 9), 
            wraplength=300
        )
        label.pack()
    
    def hide_tooltip(self):
        """툴팁 숨기기"""
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None
    
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