import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import datetime
import os


class OnlineRunView:
    """온라인 모드 실행 탭 뷰
    
    파일 복사 작업 실행 및 모니터링을 위한 UI
    """
    
    def __init__(self, parent, execution_controller):
        """온라인 실행 뷰 초기화
        
        Args:
            parent: 부모 위젯
            execution_controller: 실행 컨트롤러 객체
        """
        self.parent = parent
        self.execution_controller = execution_controller
        
        # 진행 상태 관련 변수
        self.progress_labels = {}
        self.progress_bars = {}
        self.progress_indicators = {}
        self.progress_data = {}
        
        # 테이블별 파일 개수 추적
        self.table_file_counts = {}
        self.table_current_file_index = {}
        
        # 콜백 설정
        self.execution_controller.set_callbacks(
            progress_callback=self.update_progress,
            status_callback=None,
            log_callback=None
        )
        
        # UI 생성
        self.create_ui()
    
    def create_ui(self):
        """UI 요소 생성"""
        # ------------------------------------------------------------
        # 1. 파일 복사 그룹
        # ------------------------------------------------------------
        self.create_file_copy_section()
        
        # ------------------------------------------------------------
        # 2. 진행 상태 그룹
        # ------------------------------------------------------------
        self.create_progress_section()
        
        # ------------------------------------------------------------
        # 3. 데이터 확인 그룹
        # ------------------------------------------------------------
        self.create_data_view_section()
    
    def create_file_copy_section(self):
        """파일 복사 섹션 생성"""
        frame_copy = tk.LabelFrame(self.parent, text="파일 복사", padx=5, pady=5)
        frame_copy.pack(fill="x", padx=10, pady=5)
        
        frame_copy.columnconfigure((0, 1), weight=1)
        
        # 실행 버튼
        self.btn_execute = tk.Button(
            frame_copy, 
            text="⭕ 실행하기", 
            foreground="blue", 
            command=self.start_file_copy
        )
        self.btn_execute.grid(row=0, column=0, sticky="ew", padx=5, pady=3)
        
        # 중단 버튼
        self.btn_stop = tk.Button(
            frame_copy, 
            text="❌ 중단하기", 
            foreground="red", 
            command=self.stop_scheduler, 
            state="disabled"
        )
        self.btn_stop.grid(row=0, column=1, sticky="ew", padx=5, pady=3)
    
    def create_progress_section(self):
        """진행 상태 섹션 생성"""
        frame_progress_box = tk.LabelFrame(self.parent, text="진행 상태", padx=5, pady=5)
        frame_progress_box.pack(fill="both", padx=10, pady=5, expand=True)
        
        # Canvas와 스크롤바 설정
        self.canvas = tk.Canvas(frame_progress_box, height=220, bg="white")
        self.scrollbar = ttk.Scrollbar(frame_progress_box, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas, bg="white")
        
        self.window_id = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        
        # 창 크기 변경 시 스크롤 영역 업데이트
        self.canvas.bind("<Configure>", self.on_canvas_configure)
        self.scrollable_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        
        # 마우스 스크롤 이벤트 바인딩
        self.scrollable_frame.bind("<Enter>", self.bind_mouse_scroll)
        self.scrollable_frame.bind("<Leave>", self.unbind_mouse_scroll)
        
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.scrollbar.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)
    
    def create_data_view_section(self):
        """데이터 확인 섹션 생성"""
        frame_db = tk.LabelFrame(self.parent, text="데이터 확인", padx=5, pady=5)
        frame_db.pack(fill="both", padx=10, pady=5, expand=True)
        
        # 다운로드 버튼 영역
        top_frame = tk.Frame(frame_db)
        top_frame.pack(fill="x", padx=5, pady=(5, 0))
        
        # CSV 다운로드 라벨
        self.lbl_excel_download = tk.Label(
            top_frame, 
            text="⭳ CSV로 다운로드", 
            cursor="hand2", 
            fg="blue", 
            font=("맑은 고딕", 11)
        )
        self.lbl_excel_download.pack(side="right", padx=5)
        
        # 이벤트 바인딩
        self.lbl_excel_download.bind("<Button-1>", self.export_to_csv)
        self.lbl_excel_download.bind("<Enter>", self.on_download_hover_enter)
        self.lbl_excel_download.bind("<Leave>", self.on_download_hover_leave)
        
        # 테이블 목록 및 데이터 영역
        bottom_frame = tk.Frame(frame_db)
        bottom_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # 테이블 목록
        frame_left = tk.Frame(bottom_frame)
        frame_left.pack(side="left", fill="y", padx=(0, 5))
        
        self.listbox = tk.Listbox(frame_left, height=15, exportselection=False)
        self.listbox.pack(side="left", fill="both")
        self.listbox.bind("<<ListboxSelect>>", self.load_table_data)
        
        scrollbar_list = tk.Scrollbar(frame_left, orient="vertical")
        scrollbar_list.pack(side="right", fill="y")
        
        self.listbox.config(yscrollcommand=scrollbar_list.set)
        scrollbar_list.config(command=self.listbox.yview)
        
        # 테이블 데이터
        frame_right = tk.Frame(bottom_frame)
        frame_right.pack(side="right", fill="both", expand=True)
        
        self.tree = ttk.Treeview(frame_right, show="headings", height=15)
        self.tree.grid(row=0, column=0, sticky="nsew")
        
        self.scrollbar_x = tk.Scrollbar(frame_right, orient="horizontal", command=self.tree.xview)
        self.scrollbar_x.grid(row=1, column=0, sticky="ew")
        self.tree.config(xscrollcommand=self.scrollbar_x.set)
        
        frame_right.columnconfigure(0, weight=1)
        frame_right.rowconfigure(0, weight=1)
        
        # 테이블 목록 로드
        self.load_table_list()
    
    # ============================================================
    # 파일 복사 관련 함수들
    # ============================================================
    def start_file_copy(self):
        """파일 복사 작업 시작"""
        # 작업 시작
        success = self.execution_controller.start_data_insert()
        
        if success:
            # 버튼 상태 변경
            self.btn_execute.config(state="disabled")
            self.btn_stop.config(state="normal")
        else:
            messagebox.showerror("오류", "파일 복사 작업을 시작할 수 없습니다.")
    
    def stop_scheduler(self):
        """파일 복사 작업 중지"""
        # 작업 중지
        success = self.execution_controller.stop_data_insert()
        
        if success:
            # 버튼 상태 변경
            self.btn_execute.config(state="normal")
            self.btn_stop.config(state="disabled")
        else:
            messagebox.showerror("오류", "파일 복사 작업을 중지할 수 없습니다.")
    
    # ============================================================
    # 진행 상태 관련 함수들
    # ============================================================
    def update_progress(self, table_nm, file_name, status, current=0, total=100):
        """진행 상태 업데이트
        
        Args:
            table_nm (str): 테이블명
            file_name (str): 파일명
            status (str): 상태 (진행 중, 완료, 실패)
            current (int): 현재 진행률 (0-100)
            total (int): 전체 (항상 100)
        """
        # 새로고침 요청인 경우
        if status == "refresh":
            self.refresh_progress_view()
            return
        
        # UI 스레드에서 실행
        self.parent.after(0, lambda: self.update_progress_ui(table_nm, file_name, status, current, total))
    
    def update_progress_ui(self, table_nm, file_name, status, current, total):
        """진행 상태 UI 업데이트
        
        Args:
            table_nm (str): 테이블명
            file_name (str): 파일명
            status (str): 상태 (진행 중, 완료, 실패)
            current (int): 현재 진행률 (0-100)
            total (int): 전체 (항상 100)
        """
        key = (table_nm, file_name)
        
        # current와 total이 None인 경우 기본값 설정
        if current is None:
            current = 0
        if total is None:
            total = 100
            
        # 테이블별 파일 개수 관리
        if table_nm not in self.table_file_counts:
            # 해당 테이블의 총 파일 개수 조회
            try:
                pending_files = self.execution_controller.db_manager.get_pending_files(table_nm)
                self.table_file_counts[table_nm] = len(pending_files) if pending_files else 1
                self.table_current_file_index[table_nm] = 0
            except:
                self.table_file_counts[table_nm] = 1
                self.table_current_file_index[table_nm] = 0
        
        # 파일 순서 업데이트
        if status == "진행 중" and current == 0:  # 새 파일 시작
            if table_nm in self.table_current_file_index:
                self.table_current_file_index[table_nm] += 1
            else:
                self.table_current_file_index[table_nm] = 1
        
        # 현재 파일 순서 및 총 파일 수
        current_file_num = self.table_current_file_index.get(table_nm, 1)
        total_files = self.table_file_counts.get(table_nm, 1)
        
        # 진행률 계산 (0-100%) - None 체크 추가
        try:
            progress_value = min(100, max(0, int(current) if current is not None else 0))
        except (ValueError, TypeError):
            progress_value = 0
        
        # 상태에 따른 색상 설정
        if status == "진행 중":
            color = "blue"
            text_color = "blue"
        elif status == "완료":
            color = "#4CAF50"
            text_color = "green"
        else:
            color = "red"
            text_color = "red"
        
        # 기존 UI 요소가 있는 경우 업데이트
        if key in self.progress_labels:
            # 기존 위젯 참조 가져오기
            lbl_status = self.progress_labels[key]
            progress_outer = self.progress_bars[key]
            progress_inner = self.progress_indicators[key]
            
            # 상태 텍스트 및 색상 업데이트
            lbl_status.config(text=status, fg=text_color)
            
            # 프로그레스 바 너비 업데이트
            bar_width = progress_outer.winfo_width()
            indicator_width = int(bar_width * (progress_value / 100))
            progress_inner.config(width=indicator_width, bg=color)
            
            # 파일 순서 텍스트 업데이트
            if key in self.progress_data and "file_order_label" in self.progress_data[key]:
                file_order_label = self.progress_data[key]["file_order_label"]
                file_order_label.config(text=f"({current_file_num}/{total_files})")
            
            # 데이터 상태 업데이트
            self.progress_data[key].update({
                "current": current if current is not None else 0,
                "total": total if total is not None else 100,
                "progress": progress_value
            })
        
        else:
            # 새로운 진행 상태 UI 생성
            frame = tk.Frame(self.scrollable_frame, bg="white")
            frame.pack(fill="x", padx=5, pady=2)
            
            # 파일 정보 라벨
            lbl_data = tk.Label(frame, text=f"[{table_nm}] {file_name}", width=32, anchor="w", bg="white")
            lbl_data.pack(side="left", padx=5)
            
            # 프로그레스 바 컨테이너
            progress_outer = tk.Frame(frame, width=240, height=20, bg="#E0E0E0", highlightthickness=1, highlightbackground="#AAAAAA")
            progress_outer.pack(side="left", padx=5, fill="x")
            progress_outer.pack_propagate(False)
            
            # 프로그레스 바 인디케이터
            indicator_width = int(240 * (progress_value / 100))
            progress_inner = tk.Frame(progress_outer, width=indicator_width, height=18, bg=color)
            progress_inner.place(x=0, y=0, anchor="nw")
            
            # 파일 순서 텍스트 라벨
            lbl_file_order = tk.Label(frame, text=f"({current_file_num}/{total_files})", bg="white")
            lbl_file_order.pack(side="left", padx=2)
            
            # 상태 표시 라벨
            lbl_status = tk.Label(frame, text=status, width=10, anchor="e", bg="white", fg=text_color)
            lbl_status.pack(side="right", padx=5)
            
            # 표시 위치 조정
            children = self.scrollable_frame.winfo_children()
            if children and len(children) > 1:
                frame.pack(fill="x", expand=True, pady=2, after=children[-1])
            else:
                frame.pack(fill="x", expand=True, pady=2)
            
            # 참조 저장
            self.progress_labels[key] = lbl_status
            self.progress_bars[key] = progress_outer
            self.progress_indicators[key] = progress_inner
            self.progress_data[key] = {
                "current": current if current is not None else 0,
                "total": total if total is not None else 100,
                "progress": progress_value,
                "file_order_label": lbl_file_order
            }
            
            # 크기 변경 이벤트 처리
            progress_outer.bind("<Configure>", lambda e, k=key: self.on_progress_bar_resize(e, k))
        
        # 스크롤 영역 업데이트
        self.parent.update_idletasks()
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        
        # 새 항목 추가 시 자동 스크롤
        if key not in self.progress_labels:
            self.canvas.yview_moveto(1.0)
    
    def on_progress_bar_resize(self, event, key):
        """프로그레스 바 크기 변경 이벤트 처리
        
        Args:
            event: 이벤트 객체
            key: 진행 항목 키
        """
        if key in self.progress_data and key in self.progress_indicators:
            progress_value = self.progress_data[key]["progress"]
            progress_inner = self.progress_indicators[key]
            
            # 너비 비율에 맞게 인디케이터 크기 조정
            bar_width = event.width
            indicator_width = int(bar_width * (progress_value / 100))
            progress_inner.config(width=indicator_width)
    
    def on_canvas_configure(self, event):
        """캔버스 크기 변경 이벤트 처리
        
        Args:
            event: 이벤트 객체
        """
        # 캔버스 너비에 맞게 윈도우 크기 조정
        width = event.width
        self.canvas.itemconfig(self.window_id, width=width)
        
        # 스크롤 영역 재계산
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def bind_mouse_scroll(self, event):
        """마우스 스크롤 이벤트 바인딩
        
        Args:
            event: 이벤트 객체
        """
        # 마우스가 영역에 들어올 때 스크롤 활성화
        self.canvas.bind_all("<MouseWheel>", self.on_mouse_scroll)
        # 리눅스 시스템 호환
        self.canvas.bind_all("<Button-4>", self.on_mouse_scroll)
        self.canvas.bind_all("<Button-5>", self.on_mouse_scroll)
    
    def unbind_mouse_scroll(self, event):
        """마우스 스크롤 이벤트 해제
        
        Args:
            event: 이벤트 객체
        """
        # 마우스가 영역을 벗어날 때 스크롤 해제
        self.canvas.unbind_all("<MouseWheel>")
        self.canvas.unbind_all("<Button-4>")
        self.canvas.unbind_all("<Button-5>")
    
    def on_mouse_scroll(self, event):
        """마우스 스크롤 이벤트 처리
        
        Args:
            event: 이벤트 객체
        """
        # 윈도우/맥 환경
        if event.num == 4 or event.num == 5:
            # 리눅스 환경
            if event.num == 4:
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.canvas.yview_scroll(1, "units")
        else:
            # 윈도우/맥 환경에서는 delta 속성 사용
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
    
    def refresh_progress_view(self):
        """진행 상태 뷰 새로고침"""
        # 스크롤 영역 재계산
        if hasattr(self, 'scrollable_frame') and hasattr(self, 'canvas'):
            self.scrollable_frame.update_idletasks()
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
            
            # 프로그레스 바 크기 업데이트
            for key in self.progress_bars.keys():
                if key in self.progress_data and key in self.progress_indicators:
                    progress_value = self.progress_data[key]["progress"]
                    progress_inner = self.progress_indicators[key]
                    outer_frame = self.progress_bars[key]
                    
                    # 프레임 너비에 비례하여 내부 인디케이터 크기 조정
                    self.parent.update_idletasks()
                    bar_width = outer_frame.winfo_width()
                    if bar_width > 0:  # 0으로 나누기 방지
                        indicator_width = int(bar_width * (progress_value / 100))
                        progress_inner.config(width=indicator_width)
    
    # ============================================================
    # 데이터 확인 관련 함수들
    # ============================================================
    def load_table_list(self):
        """테이블 목록 로드"""
        try:
            # 테이블 목록 조회 (SQLite에서는 TABLE_INFO 테이블 목록)
            table_names = self.execution_controller.load_table_list()
            
            # 리스트박스 초기화 및 데이터 추가
            self.listbox.delete(0, tk.END)
            for table in table_names:
                self.listbox.insert(tk.END, table)
                
            # 첫 번째 항목 선택
            if self.listbox.size() > 0:
                self.listbox.selection_set(0)
                self.load_table_data(None)  # 선택한 테이블 데이터 로드
        except Exception as e:
            print(f"테이블 목록 불러오기 오류: {e}")
    
    def load_table_data(self, event):
        """테이블 데이터 로드
        
        Args:
            event: 이벤트 객체 (None일 수 있음)
        """
        # 선택된 테이블 확인
        selected_index = self.listbox.curselection()
        if not selected_index:
            return
        
        table_name = self.listbox.get(selected_index[0])
        
        try:
            # 테이블 데이터 조회 (SQLite 관리 테이블 데이터 표시)
            columns, rows = self.execution_controller.load_table_data(table_name)
            
            # 트리뷰 초기화 및 컬럼 설정
            self.tree["columns"] = columns
            self.tree.delete(*self.tree.get_children())
            
            for col in columns:
                self.tree.heading(col, text=col)
                self.tree.column(col, anchor="center", width=150, minwidth=150, stretch=False)
            
            # 조회 데이터 트리뷰에 추가
            for row in rows:
                self.tree.insert("", "end", values=row)
                
            # UI 갱신
            self.parent.update_idletasks()
            self.tree.update_idletasks()
        except Exception as e:
            print(f"{table_name} 데이터 불러오기 오류: {e}")
    
    def export_to_csv(self, event=None):
        """테이블 데이터 CSV 내보내기
        
        Args:
            event: 이벤트 객체 (None일 수 있음)
        """
        # 선택된 테이블 확인
        selected_index = self.listbox.curselection()
        if not selected_index:
            messagebox.showinfo("알림", "테이블을 선택해주세요.")
            return
        
        table_name = self.listbox.get(selected_index[0])
        
        # 파일 저장 경로 선택
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV 파일", "*.csv"), ("모든 파일", "*.*")],
            initialfile=f"{table_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        
        if not file_path:
            return
        
        try:
            # CSV 내보내기 실행
            success = self.execution_controller.export_to_csv(table_name, file_path)
            
            if success:
                messagebox.showinfo("완료", f"{table_name} 테이블이 CSV 파일로 저장되었습니다.\n\n저장 위치 : {file_path}")
        except Exception as e:
            messagebox.showerror("오류", f"CSV 파일 저장 중 오류가 발생했습니다.\n{str(e)}")
    
    def on_download_hover_enter(self, event):
        """다운로드 라벨 마우스 진입 이벤트
        
        Args:
            event: 이벤트 객체
        """
        self.lbl_excel_download.config(fg="darkblue", font=("맑은 고딕", 11, "bold"))
    
    def on_download_hover_leave(self, event):
        """다운로드 라벨 마우스 이탈 이벤트
        
        Args:
            event: 이벤트 객체
        """
        self.lbl_excel_download.config(fg="blue", font=("맑은 고딕", 11))