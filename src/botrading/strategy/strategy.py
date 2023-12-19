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
        
        rules = [ColumStateValues.SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        reset_data_frame = data_frame.query(state_query)
        
        if reset_data_frame.empty == False:
            
            data_frame[DataFrameColum.LOOK.value] = False
            data_frame[DataFrameColum.SIDE_TYPE.value] = "-"
            data_frame[DataFrameColum.STATE.value] = ColumStateValues.WAIT.value
            data_frame[DataFrameColum.STOP_LOSS_LEVEL.value] = 0
            
            return data_frame
        
        rules = [ColumStateValues.WAIT]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame = data_frame.query(state_query)
        look_data_frame = filtered_data_frame
        
        filtered_data_frame = filtered_data_frame.query(DataFrameColum.LOOK.value + " == False")        
           
        time_range = TimeRanges("HOUR_4")
        
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range)
        filtered_data_frame = bitget_data_util.updating_adx(time_range=time_range, data_frame=filtered_data_frame, prices_history_dict=prices_history)
        filtered_data_frame = bitget_data_util.updating_ao(time_range=time_range, data_frame=filtered_data_frame, prices_history_dict=prices_history)
        
        filtered_data_frame = bitget_data_util.updating_price(data_frame = filtered_data_frame)
        
        Strategy.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=filtered_data_frame)
        
        #query = DataFrameColum.AO_ASCENDING.value + " == True"
        #filtered_data_frame = filtered_data_frame.query(query)
        #Strategy.print_data_frame(message="DATOS COMPRA AO_ASCENDING FILTRADO", data_frame=filtered_data_frame)

        query = DataFrameColum.ADX_ANGLE.value + " < 110"
        filtered_data_frame = filtered_data_frame.query(query)
        Strategy.print_data_frame(message="DATOS COMPRA ADX_ANGLE FILTRADO", data_frame=filtered_data_frame)
        
        if filtered_data_frame.empty == False:
            
            filtered_data_frame[DataFrameColum.LOOK.value] = True
            
            for ind in filtered_data_frame.index:
            
                symbol = filtered_data_frame[DataFrameColum.SYMBOL.value][ind]
                ao_asc = filtered_data_frame[DataFrameColum.AO_ASCENDING.value][ind]
                prices = prices_history[symbol]
                
                prices_open = prices['Open'].astype(float)
                prices_close = prices['Close'].astype(float)
                prices_open = numpy.array(prices_open)
                prices_open = prices_open[~numpy.isnan(prices_open)]
                    
                prices_close = numpy.array(prices_close)
                prices_close = prices_close[~numpy.isnan(prices_close)]
                    
                low = prices_open[-1]
                high = prices_close[-1]
                price_range = high - low
                filtered_data_frame.loc[ind, DataFrameColum.SOPORTES.value] = low + price_range / 4
                
                if ao_asc:
                    
                    filtered_data_frame.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                    
                else:
                    
                    filtered_data_frame.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value

            Strategy.print_data_frame(message="SOPORTES CALCULADOS", data_frame=filtered_data_frame)

            return filtered_data_frame
        
        buy_data_frame = look_data_frame.query(DataFrameColum.LOOK.value + " == True")        
        buy_data_frame = bitget_data_util.updating_price(data_frame = buy_data_frame)  
        buy_data_frame_short = buy_data_frame
        buy_data_frame_long = buy_data_frame    
        
        Strategy.print_data_frame(message="EJECUCION COMPRA ", data_frame=buy_data_frame)
        
        query_short = DataFrameColum.SIDE_TYPE.value + " == " + FutureValues.SIDE_TYPE_SHORT.value + " and " + DataFrameColum.PRICE_BUY.value + " > " + DataFrameColum.SOPORTES.value
        buy_data_frame_short = buy_data_frame_short.query(query_short)
        
        if buy_data_frame_short.empty == False:
            buy_data_frame_short[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            TelegramNotify.notify_buy(settings=settings, dataframe=buy_data_frame_short)
            return buy_data_frame_short
        
        query_long =  DataFrameColum.SIDE_TYPE.value + " == " + FutureValues.SIDE_TYPE_LONG.value + " and " + DataFrameColum.PRICE_BUY.value + " < " + DataFrameColum.SOPORTES.value
        buy_data_frame_long = buy_data_frame_long.query(query_long)
        
        if buy_data_frame_long.empty == False:
            buy_data_frame_long[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            TelegramNotify.notify_buy(settings=settings, dataframe=buy_data_frame_long)
            return buy_data_frame_long
                
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
                        DataFrameColum.SYMBOL.value,
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