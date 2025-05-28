import tkinter as tk
import sys
import os
import logging
import datetime

# 프로젝트 루트 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 로그 설정
def setup_logging():
    log_dir = os.path.join(current_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = os.path.join(log_dir, f'app_{datetime.datetime.now().strftime("%Y%m%d")}.log')
    
    # 로그 포맷 설정
    log_format = '%(asctime)s [%(levelname)s] - %(name)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # 파일 핸들러 설정
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 시작 로그
    root_logger.info("애플리케이션 시작")
    
    return root_logger

# SQLite 초기화 확인
def setup_sqlite():
    try:
        import sqlite3
        
        # SQLite 버전 확인
        sqlite_version = sqlite3.sqlite_version
        logging.info(f"SQLite 버전: {sqlite_version}")
        
        # 테스트 연결
        test_db_path = os.path.join(current_dir, 'test.db')
        conn = sqlite3.connect(test_db_path)
        conn.close()
        
        # 테스트 파일 삭제
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
        
        logging.info("SQLite 연결 테스트 성공")
        return True
    except Exception as e:
        logging.error(f"SQLite 설정 오류: {e}")
        return False

def main():
    # 로깅 설정
    logger = setup_logging()
    
    try:
        # SQLite 설정 확인
        if not setup_sqlite():
            logger.error("SQLite 설정 실패. 애플리케이션을 종료합니다.")
            return
        
        # 메인 애플리케이션 임포트 및 실행
        from views import DataInsertApp
        
        # 루트 윈도우 생성
        root = tk.Tk()
        
        # 애플리케이션 생성 및 실행
        app = DataInsertApp(root)
        
        # 애플리케이션 종료 시 처리
        def on_closing():
            logger.info("애플리케이션 종료")
            root.destroy()
        
        root.protocol("WM_DELETE_WINDOW", on_closing)
        
        # 메인 이벤트 루프 시작
        root.mainloop()
        
    except Exception as e:
        logger.exception(f"애플리케이션 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    main()