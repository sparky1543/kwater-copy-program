import threading
import datetime
import logging
import os
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed


class SchedulerManager:
    """자동화 스케줄링 관리 클래스 (파일 복사용)"""
    
    def __init__(self, db_manager=None, linux_ssh_client=None, was_ssh_client=None, data_processor=None):
        """스케줄러 매니저 초기화
        
        Args:
            db_manager: 데이터베이스 매니저 객체
            linux_ssh_client: Linux SSH 클라이언트 객체 (행안부 서버)
            was_ssh_client: WAS SSH 클라이언트 객체 (WAS 서버)
            data_processor: 데이터 프로세서 객체 (호환성용)
        """
        self.db_manager = db_manager
        self.linux_ssh_client = linux_ssh_client
        self.was_ssh_client = was_ssh_client
        self.data_processor = data_processor  # 호환성을 위해 유지
        
        # 스케줄러 설정
        self.scheduler = None
        self.scheduler_running = False
        
        # 처리 중인 테이블 추적 집합
        self.tables_in_process = set()
        
        # 작업 콜백 함수 (UI 업데이트용)
        self.progress_update_callback = None
        self.status_update_callback = None
        self.log_callback = None
        
        # 현재 처리 중인 테이블-파일 매핑
        self.current_processing_files = {}
        
        # 로깅 설정
        self.logger = logging.getLogger('SchedulerManager')
        self.logger.setLevel(logging.INFO)
        
    def set_callbacks(self, progress_callback=None, status_callback=None, log_callback=None):
        """콜백 함수 설정"""
        self.progress_update_callback = progress_callback
        self.status_update_callback = status_callback
        self.log_callback = log_callback
    
    def log(self, message, level='info'):
        """로그 메시지 기록"""
        if level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        else:
            self.logger.info(message)
        
        if self.log_callback:
            self.log_callback(message, level)
    
    def start_scheduler(self):
        """스케줄러 시작"""
        if self.scheduler_running:
            self.log("스케줄러가 이미 실행 중입니다.")
            return False
        
        # 종속성 검사
        if not self.db_manager or not self.linux_ssh_client or not self.was_ssh_client:
            self.log("스케줄러를 시작하기 위한 필수 모듈이 없습니다.", 'error')
            return False
        
        try:
            # 스케줄러 상태 설정
            self.scheduler_running = True
            self.scheduler = BackgroundScheduler()
            
            # 자동화 설정 조회 및 작업 설정
            self._configure_scheduler_jobs()
            
            # 스케줄러 시작
            if self.scheduler_running:
                self.scheduler.start()
                self.log("스케줄러가 시작되었습니다.")
                
                # 즉시 실행 작업 처리
                self._process_immediate_tasks()
                
                return True
            
        except Exception as e:
            self.log(f"스케줄러 시작 오류: {e}", 'error')
            self.stop_scheduler()
            return False
    
    def stop_scheduler(self):
        """스케줄러 중지"""
        if self.scheduler and self.scheduler_running:
            self.log("스케줄러 중지 요청: 진행 중인 작업을 취소합니다.")
            
            # 현재 처리 중인 테이블 목록 저장
            processing_tables = self.tables_in_process.copy()
            
            # 스케줄러 상태 변경
            self.scheduler_running = False
            
            # 진행 중인 작업들 실패로 표시
            if self.progress_update_callback and processing_tables:
                for table_nm in processing_tables:
                    file_name = self.current_processing_files.get(table_nm)
                    self.log(f"[{table_nm}] 작업 강제 중단됨: 파일={file_name}")
                    self.progress_update_callback(table_nm, file_name, "실패", None, None)
            
            # 스케줄러 종료
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=False)
                
            self.scheduler = None
            
            # SSH 연결 정리
            try:
                self.linux_ssh_client.close_connection()
                self.was_ssh_client.close_connection()
                self.log("SSH 연결이 정리되었습니다.")
            except Exception as e:
                self.log(f"SSH 연결 정리 중 오류: {e}", 'warning')
            
            self.log("스케줄러가 중지되었습니다.")
            
            # 상태 콜백 호출
            if self.status_update_callback:
                self.status_update_callback(False)
            
            return True
        
        return False
    
    def is_running(self):
        """스케줄러 실행 상태 반환"""
        return self.scheduler_running
    
    def _configure_scheduler_jobs(self):
        """자동화 설정 기반으로 스케줄러 작업 구성"""
        try:
            # 자동화 설정 조회
            config_data = self.db_manager.get_all_auto_configs()
            
            copy_intervals = {}  # {인터벌: [테이블명, ...]}
            
            # 설정 분류
            for row in config_data:
                table_nm, dest_path, auto_interval, _ = row
                
                # COPY 작업 인터벌 설정
                if auto_interval and auto_interval > 0:
                    if auto_interval not in copy_intervals:
                        copy_intervals[auto_interval] = []
                    copy_intervals[auto_interval].append(table_nm)
            
            # 주기적 COPY 작업 예약
            for auto_interval, table_list in copy_intervals.items():
                if self.scheduler_running:
                    self.scheduler.add_job(
                        self.process_tables_parallel, 
                        'interval', 
                        minutes=auto_interval,
                        args=[table_list],
                        id=f"copy_interval_{auto_interval}",
                        replace_existing=True
                    )
                    self.log(f"{auto_interval}분 간격 COPY 작업 설정: {', '.join(table_list)}")
            
        except Exception as e:
            self.log(f"스케줄러 구성 오류: {e}", 'error')
            raise
    
    def _process_immediate_tasks(self):
        """즉시 실행 작업 처리"""
        try:
            # 자동화 설정 조회
            config_data = self.db_manager.get_all_auto_configs()
            
            immediate_copy_tables = []
            copy_then_process_tables = []
            
            current_time = datetime.datetime.now()
            
            # 테이블별 처리 방식 결정
            for row in config_data:
                table_nm, dest_path, auto_interval, last_timestamp = row
                
                if auto_interval and auto_interval > 0:
                    # 대기 중인 파일 확인
                    pending_files_count = self.db_manager.get_pending_files_count(table_nm)
                    
                    # 인터벌 경과 여부 확인
                    interval_passed = False
                    if not last_timestamp:
                        interval_passed = True
                    else:
                        try:
                            last_time = datetime.datetime.strptime(last_timestamp, '%Y-%m-%d %H:%M:%S')
                            if (current_time - last_time).total_seconds() / 60 >= auto_interval:
                                interval_passed = True
                        except:
                            interval_passed = True
                    
                    # 테이블 처리 방식 결정
                    if not interval_passed and pending_files_count > 0:
                        immediate_copy_tables.append(table_nm)
                    elif interval_passed:
                        copy_then_process_tables.append(table_nm)
            
            # 즉시 COPY 작업 실행
            if immediate_copy_tables:
                self.log(f"즉시 COPY 작업 실행: {', '.join(immediate_copy_tables)}")
                threading.Thread(
                    target=self.process_copy_only, 
                    args=(immediate_copy_tables,), 
                    daemon=True
                ).start()
            
            # 파일 발견 후 COPY 작업 실행
            if copy_then_process_tables:
                self.log(f"파일 발견 후 COPY 작업 실행: {', '.join(copy_then_process_tables)}")
                threading.Thread(
                    target=self.process_discover_then_copy, 
                    args=(copy_then_process_tables,), 
                    daemon=True
                ).start()
                
        except Exception as e:
            self.log(f"즉시 실행 작업 처리 오류: {e}", 'error')
    
    def process_table(self, table_nm, skip_file_discovery=False):
        """단일 테이블 처리"""
        try:
            # 스케줄러 상태 확인
            if not self.scheduler_running:
                self.log(f"[{table_nm}] 스케줄러 중지됨: 작업 취소", 'warning')
                return
            
            # 이미 처리 중인 테이블 확인
            if table_nm in self.tables_in_process:
                self.log(f"[{table_nm}] 이미 처리 중인 테이블입니다.")
                return
            
            # 처리 중인 테이블 집합에 추가
            self.tables_in_process.add(table_nm)
            self.log(f"[{table_nm}] 테이블 처리 시작")
            
            # 자동화 설정 조회
            config = self.db_manager.get_auto_config_details(table_nm)
            
            if not config:
                self.log(f"[{table_nm}] 자동화 설정이 없습니다.", 'warning')
                return
            
            src_path, dest_path, _, use_yn = config
            
            # 테이블 사용 여부 확인
            if use_yn and use_yn.upper() == 'N':
                self.log(f"[{table_nm}] 사용하지 않는 테이블입니다.")
                return
            
            # 파일 발견 처리
            if not skip_file_discovery:
                self._discover_files_for_table(table_nm, src_path)
                
                # 중단 요청 확인
                if not self.scheduler_running:
                    self.log(f"[{table_nm}] 스케줄러 중지됨: 강제 종료", 'warning')
                    return
            
            # 복사 대상 파일 조회
            target_files = self.db_manager.get_pending_files(table_nm)
            
            if not target_files:
                self.log(f"[{table_nm}] 복사 대상 파일이 없습니다.")
                return
            
            # 파일별 복사 처리 (순차적으로 처리)
            for file_name, _ in target_files:
                if not self.scheduler_running:
                    break
                
                self._process_single_file(table_nm, file_name, src_path, dest_path)
            
        except Exception as e:
            self.log(f"[{table_nm}] 처리 중 오류 발생: {e}", 'error')
        finally:
            # 처리 중인 테이블 집합에서 제거
            if table_nm in self.tables_in_process:
                self.tables_in_process.remove(table_nm)
                self.log(f"[{table_nm}] 테이블 처리 완료")
    
    def _discover_files_for_table(self, table_nm, src_path):
        """테이블에 대한 파일 발견 처리"""
        try:
            self.log(f"[{table_nm}] 파일 발견 시작: {src_path}")
            
            # 원격 파일 목록 조회
            xml_files_in_remote = self.linux_ssh_client.list_files_by_pattern(src_path, table_nm)
            self.log(f"[{table_nm}] 원격 디렉토리 파일 수: {len(xml_files_in_remote)}")
            
            if not xml_files_in_remote:
                self.log(f"[{table_nm}] 원격 디렉토리에 XML 파일이 없습니다.")
                return
            
            # 이미 등록된 파일 목록 조회
            existing_files_in_db = self.db_manager.get_existing_files(table_nm)
            
            # 새로 발견된 파일 계산
            new_files = xml_files_in_remote - existing_files_in_db
            
            if not new_files:
                self.log(f"[{table_nm}] 새로 발견된 XML 파일이 없습니다.")
                return
            
            self.log(f"[{table_nm}] 새로 발견된 파일 수: {len(new_files)}")
            
            # 파일별 DB 등록
            for file_name in sorted(new_files):  # 정렬하여 순서대로 처리
                if not self.scheduler_running:
                    self.log(f"[{table_nm}] 스케줄러 중지됨: 파일 발견 중단", 'warning')
                    break
                
                # 파일 정보 DB 등록
                self.db_manager.register_file(table_nm, file_name)
                self.log(f"[{table_nm}] FILE_INFO 테이블 업데이트 완료: {file_name}")
            
        except Exception as e:
            self.log(f"[{table_nm}] 파일 발견 중 오류 발생: {e}", 'error')
            raise
    
    def _process_single_file(self, table_nm, file_name, src_path, dest_path):
        """단일 파일 처리 (파일 복사)"""
        # 현재 처리 중인 파일 등록
        self.current_processing_files[table_nm] = file_name

        # 진행 상태 표시 초기화 (0%에서 시작)
        if self.progress_update_callback:
            self.progress_update_callback(table_nm, file_name, "진행 중", 0, 100)
        
        self.log(f"[{table_nm}] 파일 복사 시작: {file_name}")
        
        # 처리 정보 초기화
        start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        copy_success = False
        
        try:
            # 중단 요청 확인
            if not self.scheduler_running:
                self.log(f"[{table_nm}] 스케줄러 중지됨: 강제 종료", 'warning')
                if self.progress_update_callback:
                    self.progress_update_callback(table_nm, file_name, "실패", 0, 100)
                self.db_manager.log_task(table_nm, file_name, start_time)
                return
            
            # 진행률 50% 업데이트 (복사 시작)
            if self.progress_update_callback:
                self.progress_update_callback(table_nm, file_name, "진행 중", 50, 100)
            
            # 파일 복사 수행 (행안부 서버 → WAS 서버)
            copy_success = self._copy_file_between_servers(
                src_path, dest_path, file_name, table_nm
            )
            
            # 중단 요청 확인
            if not self.scheduler_running:
                self.log(f"[{table_nm}] 스케줄러 중지됨: 강제 종료", 'warning')
                if self.progress_update_callback:
                    self.progress_update_callback(table_nm, file_name, "실패", 50, 100)
                self.db_manager.log_task(table_nm, file_name, start_time)
                return
            
            if copy_success:
                # 복사 성공 시 파일 상태 업데이트
                self.db_manager.update_file_status(file_name, 'Y')
                
                # 작업 로그 저장
                self.db_manager.log_task(table_nm, file_name, start_time)
                
                self.log(f"[{table_nm}] 파일 복사 완료: {file_name}")
                
                # 진행 상태 완료로 업데이트 (100%)
                if self.progress_update_callback:
                    self.progress_update_callback(table_nm, file_name, "완료", 100, 100)
            else:
                # 복사 실패 시 파일 상태 업데이트
                self.db_manager.update_file_status(file_name, 'N')
                
                self.log(f"[{table_nm}] 파일 복사 실패: {file_name}", 'error')
                
                # 진행 상태 실패로 업데이트
                if self.progress_update_callback:
                    self.progress_update_callback(table_nm, file_name, "실패", 50, 100)
                
                # 작업 로그 저장
                self.db_manager.log_task(table_nm, file_name, start_time)
            
        except Exception as e:
            error_msg = str(e)
            self.log(f"[{table_nm}] 파일 복사 중 오류 발생: {error_msg}", 'error')
            
            # 오류 발생 시 파일 상태 업데이트
            self.db_manager.update_file_status(file_name, 'N')
            
            # 진행 상태 실패로 업데이트
            if self.progress_update_callback:
                self.progress_update_callback(table_nm, file_name, "실패", 0, 100)
            
            # 작업 로그 저장
            self.db_manager.log_task(table_nm, file_name, start_time)
        
        finally:
            # 처리 완료된 파일 정보 제거
            if table_nm in self.current_processing_files:
                del self.current_processing_files[table_nm]
    
    def _copy_file_between_servers(self, src_path, dest_path, file_name, table_nm):
        """서버 간 파일 복사 수행"""
        try:
            # 임시 로컬 파일 경로
            temp_local_path = f"temp_{table_nm}_{file_name}"
            
            # Step 1: 행안부 서버에서 로컬로 다운로드
            self.log(f"[{table_nm}] 행안부 서버에서 다운로드 중: {file_name}")
            download_success = self.linux_ssh_client.download_file(
                src_path, ".", file_name
            )
            
            if not download_success:
                self.log(f"[{table_nm}] 행안부 서버에서 다운로드 실패: {file_name}", 'error')
                return False
            
            # 다운로드된 파일을 임시 이름으로 변경
            if os.path.exists(file_name):
                os.rename(file_name, temp_local_path)
            else:
                self.log(f"[{table_nm}] 다운로드된 파일을 찾을 수 없음: {file_name}", 'error')
                return False
            
            # Step 2: 로컬에서 WAS 서버로 업로드
            self.log(f"[{table_nm}] WAS 서버로 업로드 중: {file_name}")
            upload_success = self.was_ssh_client.upload_file(
                ".", dest_path, temp_local_path
            )
            
            # 업로드된 파일을 원본 이름으로 변경 (WAS 서버에서)
            if upload_success:
                try:
                    # WAS 서버에서 파일명 변경
                    rename_command = f"cd {dest_path} && mv {temp_local_path} {file_name}"
                    stdout, stderr = self.was_ssh_client.execute_command(rename_command)
                    
                    if stderr:
                        self.log(f"[{table_nm}] WAS 서버에서 파일명 변경 경고: {stderr}", 'warning')
                    
                except Exception as e:
                    self.log(f"[{table_nm}] WAS 서버에서 파일명 변경 실패: {e}", 'warning')
            
            # Step 3: 임시 로컬 파일 정리
            try:
                if os.path.exists(temp_local_path):
                    os.remove(temp_local_path)
            except Exception as e:
                self.log(f"[{table_nm}] 임시 파일 삭제 실패: {e}", 'warning')
            
            if upload_success:
                self.log(f"[{table_nm}] 파일 복사 성공: {file_name} ({src_path} → {dest_path})")
                return True
            else:
                self.log(f"[{table_nm}] WAS 서버로 업로드 실패: {file_name}", 'error')
                return False
                
        except Exception as e:
            self.log(f"[{table_nm}] 서버 간 파일 복사 중 오류: {e}", 'error')
            
            # 정리 작업
            try:
                temp_local_path = f"temp_{table_nm}_{file_name}"
                if os.path.exists(temp_local_path):
                    os.remove(temp_local_path)
                if os.path.exists(file_name):
                    os.remove(file_name)
            except:
                pass
            
            return False
    
    def process_tables_parallel(self, table_names, skip_file_discovery=False):
        """여러 테이블 병렬 처리"""
        try:
            self.log(f"병렬 파일 복사 작업 시작: {table_names}")
            
            # 이미 처리 중인 테이블 제외
            tables_to_process = [
                table_nm for table_nm in table_names 
                if table_nm not in self.tables_in_process and self.scheduler_running
            ]
            
            if not tables_to_process:
                self.log("처리할 테이블이 없습니다.")
                return
                
            self.log(f"실제 처리할 테이블: {tables_to_process}")
            
            # 스레드풀을 사용한 병렬 처리
            with ThreadPoolExecutor(max_workers=min(5, len(tables_to_process))) as executor:
                future_to_table = {
                    executor.submit(self.process_table, table_nm, skip_file_discovery): table_nm 
                    for table_nm in tables_to_process
                }
                
                # 작업 완료 처리
                for future in as_completed(future_to_table):
                    table_nm = future_to_table[future]
                    
                    # 중단 상태 확인
                    if not self.scheduler_running:
                        self.log(f"[{table_nm}] 스케줄러 중지됨: 강제 종료", 'warning')
                        break

                    try:
                        future.result()
                    except Exception as e:
                        self.log(f"[{table_nm}] 병렬 처리 중 복사 작업 오류 발생: {e}", 'error')

        except Exception as e:
            self.log(f"병렬 복사 작업 오류 발생: {e}", 'error')
    
    def process_copy_only(self, table_names):
        """파일 발견 없이 복사만 처리"""
        try:
            self.log(f"[복사 전용] 작업 시작: {table_names}")
            self.process_tables_parallel(table_names, skip_file_discovery=True)
        except Exception as e:
            self.log(f"[복사 전용] 오류 발생: {e}", 'error')
    
    def process_discover_then_copy(self, table_names):
        """파일 발견 후 복사 처리"""
        try:
            self.log(f"[파일 발견 후 복사] 작업 시작: {table_names}")
            
            # 테이블별 병렬 처리
            for table_nm in table_names:
                if not self.scheduler_running:
                    break
                
                # 각 테이블을 개별적으로 처리
                threading.Thread(
                    target=self._discover_and_copy_independently,
                    args=(table_nm,),
                    daemon=True
                ).start()
                
        except Exception as e:
            self.log(f"[파일 발견 후 복사] 오류 발생: {e}", 'error')
    
    def _discover_and_copy_independently(self, table_nm):
        """개별 테이블의 파일 발견 및 복사 처리"""
        try:
            self.log(f"[{table_nm}] 파일 발견 후 복사 독립 처리 시작")
            
            # 테이블 설정 조회
            config = self.db_manager.get_auto_config_details(table_nm)
            if not config or not self.scheduler_running:
                return
                
            src_path, dest_path, _, use_yn = config
            
            # 테이블 사용 여부 확인
            if use_yn and use_yn.upper() == 'N':
                return
            
            # 파일 발견 및 복사 수행
            try:
                self._discover_files_for_table(table_nm, src_path)
                
                # LAST_TIMESTAMP 업데이트
                if self.scheduler_running:
                    self.db_manager.update_auto_config_timestamp(table_nm)
                    
                # 발견 완료 후 즉시 복사 작업 실행
                if self.scheduler_running:
                    self.process_table(table_nm, skip_file_discovery=True)
                    
            except Exception as e:
                self.log(f"[{table_nm}] 발견 또는 복사 작업 오류: {e}", 'error')
                
        except Exception as e:
            self.log(f"[{table_nm}] 독립 처리 오류: {e}", 'error')