from views.run_tab.online_view import OnlineRunView
from views.run_tab.offline_view import OfflineRunView

# 실행 탭 뷰 클래스들을 직접 임포트할 수 있도록 노출
__all__ = [
    'OnlineRunView',
    'OfflineRunView'
]