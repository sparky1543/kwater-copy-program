from views.manage_tab.connection_view import ConnectionView
from views.manage_tab.table_info_view import TableInfoView
from views.manage_tab.auto_config_view import AutoConfigView
from views.manage_tab.column_mapping_view import ColumnMappingView

# 관리 탭 뷰 클래스들을 직접 임포트할 수 있도록 노출
__all__ = [
    'ConnectionView',
    'TableInfoView',
    'AutoConfigView',
    'ColumnMappingView'
]