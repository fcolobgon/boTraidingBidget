import pandas
import numpy

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.future_values import FutureValues
from src.botrading.telegram.telegram_notify import TelegramNotify
from src.botrading.utils.dataframe_util import DataFrameUtil


from configs.config import settings as settings

class Strategy:
    
    name:str
    
    def __init__(self, name:str):
        
        self.name = name

        # Giro ADX segun bitman
    @staticmethod
    def apply_buy(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.WAIT, ColumStateValues.SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame: pandas.DataFrame
        filtered_data_frame = data_frame.query(state_query)
        
        time_range = TimeRanges("HOUR_1")
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range)

        config_ma = ConfigMA(length = 50, type="ema")
        filtered_data_frame = bitget_data_util.updating_ma(time_range = time_range, config_ma = config_ma, data_frame = filtered_data_frame, prices_history_dict = prices_history)
        filtered_data_frame['MA_50_LST'] = filtered_data_frame[DataFrameColum.MA_LAST.value]       

        config_ma = ConfigMA(length = 100, type="ema")
        filtered_data_frame = bitget_data_util.updating_ma(time_range = time_range, config_ma = config_ma, data_frame = filtered_data_frame, prices_history_dict = prices_history)
        filtered_data_frame['MA_100_LST'] = filtered_data_frame[DataFrameColum.MA_LAST.value]  

        config_ma = ConfigMA(length = 150, type="ema")
        filtered_data_frame = bitget_data_util.updating_ma(time_range = time_range, config_ma = config_ma, data_frame = filtered_data_frame, prices_history_dict = prices_history)
        filtered_data_frame['MA_150_LST'] = filtered_data_frame[DataFrameColum.MA_LAST.value]  

        query = "(MA_50_LST > MA_100_LST) and (MA_100_LST > MA_150_LST)"  
        buy_data_frame = filtered_data_frame.query(query)

        if buy_data_frame.empty == False:
            buy_data_frame[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            TelegramNotify.notify_buy(settings=settings, dataframe=buy_data_frame)
            
            return buy_data_frame
            
        return pandas.DataFrame()
    
    @staticmethod
    def apply_sell(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        filtered_data_frame = data_frame

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame = filtered_data_frame.query(state_query)
        
        # Percentage profit
        filtered_data_frame = bitget_data_util.update_percentage_profit(filtered_data_frame)
        actual_profit_dataframe = filtered_data_frame
        filtered_data_frame = DataFrameUtil.increase_level(data_frame=filtered_data_frame, take_profit_flag=True)
        
        Strategy.print_data_frame(message="INICIO VENTA", data_frame=filtered_data_frame, print_empty=False)
        
        query = DataFrameColum.STOP_LOSS_LEVEL.value + " < 0"        
        filtered_data_frame = filtered_data_frame.query(query)
                
        if filtered_data_frame.empty == False:
            query = DataFrameColum.PERCENTAGE_PROFIT.value + " > 0.9"      
            filtered_data_frame = filtered_data_frame.query(query)
            if filtered_data_frame.empty == False:
                TelegramNotify.notify(settings=settings, message="TOUCH STOP LOSS LEVEL")
                return Strategy.return_for_sell(data_frame=filtered_data_frame)
        
        return actual_profit_dataframe
    
    @staticmethod
    def return_for_sell(data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        data_frame[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
        data_frame[DataFrameColum.STOP_LOSS_LEVEL.value] = 0
        data_frame[DataFrameColum.LOOK.value] = False
        data_frame[DataFrameColum.SIDE_TYPE.value] = "-"
        TelegramNotify.notify_sell(settings=settings, dataframe=data_frame)
        return data_frame
    
    @staticmethod
    def print_data_frame(message: str, data_frame: pandas.DataFrame, print_empty:bool=True):

        if data_frame.empty == False:
            print(message)
            print("#####################################################################################################################")
            print(
                data_frame[
                    [
                        DataFrameColum.BASE.value,
                        DataFrameColum.ADX_ANGLE.value,
                        DataFrameColum.AO_ASCENDING.value,
                        DataFrameColum.SOPORTES.value,
                        DataFrameColum.STOP_LOSS_LEVEL.value,
                        DataFrameColum.PERCENTAGE_PROFIT.value,
                        DataFrameColum.SIDE_TYPE.value,
                        DataFrameColum.LOOK.value
                    ]
                ]
            )