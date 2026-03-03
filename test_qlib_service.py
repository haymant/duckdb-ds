import pytest
import os
import pandas as pd
from unittest import mock

# Mocking the Qlib objects in sys.modules to prevent actual qlib initialization during test
import sys
mock_qlib_handler = mock.MagicMock()
mock_qlib_data = mock.MagicMock()
mock_qlib_constant = mock.MagicMock()
mock_qlib = mock.MagicMock()

class MockAlpha158:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def fetch(self, **kwargs):
        return pd.DataFrame({
            "datetime": [pd.Timestamp("2020-01-01")], 
            "instrument": ["AAPL"], 
            "factor1": [1.0]
        }).set_index(["datetime", "instrument"])
        
class MockAlpha158e(MockAlpha158):
    pass

class MockAlpha360(MockAlpha158):
    pass

mock_qlib_handler.Alpha158 = MockAlpha158
mock_qlib_handler.Alpha360 = MockAlpha360
mock_qlib_constant.REG_CN = 'cn'
mock_qlib_constant.REG_US = 'us'

sys.modules['qlib.contrib.data.handler'] = mock_qlib_handler
sys.modules['qlib.data'] = mock_qlib_data
sys.modules['qlib.constant'] = mock_qlib_constant
sys.modules['qlib'] = mock_qlib

from services.qlib_service import load_qlib_dataframes, Alpha158e
Alpha158e.__bases__ = (MockAlpha158,)

@mock.patch('os.path.isdir', return_value=True)
@mock.patch.dict(os.environ, {
    'QLIB_US_DATA_DIR': '/fake/us/dir',
    'QLIB_US_ALPHA_TYPE': '158, 158e, 360',
    'QLIB_CN_DATA_DIR': '/fake/cn/dir',
    'QLIB_CN_ALPHA_TYPE': '158',
})
def test_load_qlib_dataframes(mock_isdir):
    # Override Alpha158e constructor specifically for testing
    from services import qlib_service
    qlib_service.Alpha158e = MockAlpha158e
    
    dfs = load_qlib_dataframes()
    
    assert 'alpha158_us' in dfs
    assert 'alpha158e_us' in dfs
    assert 'alpha360_us' in dfs
    assert 'alpha158_cn' in dfs
    assert 'alpha158e_cn' not in dfs
    
    for name, df in dfs.items():
        assert isinstance(df, pd.DataFrame)
        assert 'date' in df.columns
        assert 'symbol' in df.columns
        assert 'factor1' in df.columns
