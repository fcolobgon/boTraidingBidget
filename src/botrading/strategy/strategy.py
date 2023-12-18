import pandas

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum

from configs.config import settings as settings

class Strategy:
    
    name:str
    
    def __init__(self, name:str):
        
        self.name = name

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.WAIT, ColumStateValues.SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame: pandas.DataFrame
        filtered_data_frame = data_frame.query(state_query)

        filtered_data_frame[DataFrameColum.SIDE_TYPE.value] = "short"

        filtered_data_frame[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
        return filtered_data_frame
    

    def apply_sell(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        filtered_data_frame = data_frame

        rules = [ColumStateValues.BUY, ColumStateValues.ERR_SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame = filtered_data_frame.query(state_query)


        filtered_data_frame[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value


        return filtered_data_frame