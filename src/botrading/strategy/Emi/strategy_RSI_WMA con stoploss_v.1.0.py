import time
import numpy
import pandas_ta 

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil

from src.botrading.utils import koncorde
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.dataframe_util import DataFrameUtil
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.colum_good_bad_values import ColumLineValues
from src.botrading.utils.enums.future_values import FutureValues
from src.botrading.telegram.telegram_notify import TelegramNotify
from src.botrading.utils.price_util import PriceUtil
from src.botrading.utils import excel_util
from datetime import datetime, timedelta

#from src.botrading.bit import BitgetClienManager
from src.botrading.utils import traiding_operations

from matplotlib import pyplot as plt


from configs.config import settings as settings

class Strategy:
    name:str

    def __init__(self, name:str):

        self.name = name
        
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL, ColumStateValues.ERR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame: pandas.DataFrame
        filtered_data_frame = data_frame.query(state_query)

        filtered_df_master = filtered_data_frame

        time_range = TimeRanges("HOUR_1") #DAY_1  HOUR_4  MINUTES_1
        hours_window_check = 10

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range, limit=1000)
        filtered_data_frame = bitget_data_util.updating_rsi(length=9, data_frame=filtered_data_frame, prices_history_dict=prices_history, 
                                                            ascending_count=2)
        filtered_data_frame = Strategy.updating_wma(bitget_data_util=bitget_data_util, length=20, data_frame=filtered_data_frame, 
                                                    prices_history_dict=prices_history, ascending_count=3)

        #excel_util.save_data_frame( data_frame=filtered_data_frame, exel_name="wma.xlsx")

        # -------------------------------- L O N G  ------------------------------------
     
        query = "(" + DataFrameColum.RSI_LAST.value + " < 30)"
        df_long_step_1 = filtered_data_frame.query(query)
        
        if df_long_step_1.empty == False:
            df_long_step_1.loc[:, DataFrameColum.NOTE.value] = "CHECK_LNG"
            df_long_step_1.loc[:, DataFrameColum.NOTE_3.value] = Strategy.next_hour_up(hours = hours_window_check)
            
            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_long_step_1)

        query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG') and (" + DataFrameColum.WMA_ASCENDING.value + " == True)"
        df_long_step_2 = filtered_df_master.query(query)

        if df_long_step_2.empty == False:
            for ind in df_long_step_2.index:
                df_long_step_2.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                df_long_step_2.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
                df_long_step_2.loc[ind, DataFrameColum.LEVEREAGE.value] = 10

                symbol = df_long_step_2.loc[ind, DataFrameColum.SYMBOL.value]
                prices_history = prices_history[symbol]
                close = prices_history['Close'].astype(float)   
                last_10 = close[-15:]
                df_long_step_2.loc[ind, DataFrameColum.STOP_LOSS.value] =  min(last_10)     

                df_long_step_2.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_long_step_2)

        # -------------------------------- S H O R T  ------------------------------------
     
        query = "(" + DataFrameColum.RSI_LAST.value + " > 70)"
        df_short_step_1 = filtered_data_frame.query(query)
        
        if df_short_step_1.empty == False:
            df_short_step_1.loc[:, DataFrameColum.NOTE.value] = "CHECK_SHRT"
            df_short_step_1.loc[:, DataFrameColum.NOTE_3.value] = Strategy.next_hour_up(hours = hours_window_check)

            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_short_step_1)

        query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT') and (" + DataFrameColum.WMA_ASCENDING.value + " == False)"
        df_short_step_2 = filtered_df_master.query(query)

        if df_short_step_2.empty == False:
            for ind in df_short_step_2.index:
                df_short_step_2.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                df_short_step_2.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
                df_short_step_2.loc[ind, DataFrameColum.LEVEREAGE.value] = 10
                
                symbol = df_short_step_2.loc[ind,DataFrameColum.SYMBOL.value]
                prices_history = prices_history[symbol]
                close = prices_history['Close'].astype(float)   
                last_10 = close[-15:]
                df_short_step_2.loc[ind, DataFrameColum.STOP_LOSS.value] =  max(last_10)  

                df_short_step_2.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_short_step_2)


        # ------------------------------------- BORRAR DATES DE WINDOWS ------------------------------------------------
        
        query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT') or (" + DataFrameColum.NOTE.value + " == 'CHECK_LNG')"
        df_check = filtered_df_master.query(query)

        #Formato de fecha
        df_check[DataFrameColum.NOTE_3.value] = pandas.to_datetime(df_check[DataFrameColum.NOTE_3.value], format='%d-%m-%Y %H:%M:%S')

    
        # Filtrar los registros que superan la hora límite
        df_check = df_check[df_check[DataFrameColum.NOTE_3.value] < datetime.now()]

        if df_check.empty == False:
            df_check.loc[:, DataFrameColum.NOTE_3.value] = "-"
            df_check.loc[:, DataFrameColum.NOTE.value] = "-"

            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_check)

        Strategy.print_data_frame(message="COMPRA ", data_frame=filtered_df_master)

        return filtered_df_master
    
    @staticmethod
    def apply_sell(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame = data_frame.query(state_query)

        startTime = datetime.now()
        startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)

        filtered_data_frame =  bitget_data_util.updating_pnl_roe_orders(data_frame=filtered_data_frame, startTime=startTime)

        if filtered_data_frame.empty == False:
            query = DataFrameColum.ORDER_OPEN.value + " == False"      
            df_order = filtered_data_frame.query(query)

            if df_order.empty == False:
                df_order[DataFrameColum.ORDER_ID.value] = "-"
                df_order[DataFrameColum.TAKE_PROFIT.value] = 0.0
                df_order[DataFrameColum.STOP_LOSS.value] = 0.0
                df_order.loc[:, DataFrameColum.NOTE.value] = ""
                df_order[DataFrameColum.STATE.value] = ColumStateValues.SELL.value

                return df_order

        Strategy.print_data_frame(message="VENTA ", data_frame=filtered_data_frame)

        time_range = TimeRanges("HOUR_1")  #DAY_1  HOUR_4  MINUTES_1

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range, limit=1000)
        filtered_data_frame = Strategy.updating_wma(bitget_data_util=bitget_data_util, length=20, data_frame=filtered_data_frame, prices_history_dict=prices_history, ascending_count=2)

       # -------------------------------- L O N G  ------------------------------------

        query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG') and (" + DataFrameColum.WMA_ASCENDING.value + " == False)"
        df_long_step_1 = filtered_data_frame.query(query)

        if df_long_step_1.empty == False:
            df_long_step_1.loc[:, DataFrameColum.STOP_LOSS.value] = 0.0
            df_long_step_1.loc[:, DataFrameColum.NOTE.value] = ""
            df_long_step_1.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value

            return df_long_step_1 

       # -------------------------------- S H O R T  ------------------------------------

        query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT') and (" + DataFrameColum.WMA_ASCENDING.value + " == True)"
        df_short_step_1 = filtered_data_frame.query(query)

        if df_short_step_1.empty == False:
            df_short_step_1.loc[:, DataFrameColum.STOP_LOSS.value] = 0.0
            df_short_step_1.loc[:, DataFrameColum.NOTE.value] = ""
            df_short_step_1.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value

            return df_short_step_1 
        
       # -------------------------------- M O V E  S T O P L O S S   ------------------------------------

        #filtered_data_frame.loc[:,DataFrameColum.PRESET_STOP_LOSS_PRICE.value] = 0.5215
        """
        query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG')"
        df_lng_sl = filtered_data_frame.query(query)

        if df_lng_sl.empty == False:
        
            for ind in df_lng_sl.index:
                sl = yo puedo verodf_lng_sl.loc[ind, DataFrameColum.STOP_LOSS.value]
        
                #INFORMAR STOPLOSS

                return df_lng_sl


        query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT')"
        df_shrt_sl = filtered_data_frame.query(query)

        if df_shrt_sl.empty == False:
            df_shrt_sl.loc[:, DataFrameColum.STOP_LOSS.value] = 0
            df_shrt_sl.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
        """
        
        return filtered_data_frame



# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def mark_price_exceeds_limit(data_frame: pandas.DataFrame, value_limit: float = 1) -> pandas.DataFrame:
        """Marcamos las cons con el profit superior al limite marcado"""
        data_frame.loc[data_frame[DataFrameColum.ROE.value] >= value_limit, DataFrameColum.LOOK.value] = 'headdress!'
        return data_frame
    

    def updating_wma(bitget_data_util: BitgetDataUtil, length:int = 14, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):        
        if DataFrameColum.WMA.value not in data_frame.columns:
            data_frame[DataFrameColum.WMA.value] = "-"

        if DataFrameColum.WMA_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.WMA_LAST.value] = 0.0

        if DataFrameColum.WMA_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.WMA_ASCENDING.value] = "-"
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            try: 
                prices_history = prices_history_dict[symbol]
                close = prices_history['Close'].astype(float)   

                wma = pandas_ta.wma(pandas.Series(close), length = length)

                wma_numpy = numpy.array(wma)
                wma_numpy = wma_numpy[~numpy.isnan(wma_numpy)]
                                
                data_frame[DataFrameColum.WMA.value][ind] = wma_numpy
                data_frame.loc[ind, DataFrameColum.WMA_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = wma_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.WMA_LAST.value] = bitget_data_util.get_last_element(element_list = wma_numpy, previous_period = previous_period)
               
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "WMA", e)
                continue
        
        return data_frame
    
    @staticmethod
    def next_hour_up(hours: int = 1):
        now = datetime.now()
        start_time = now.replace(hour=2, minute=0, second=0, microsecond=0)
        time_difference = now - start_time
        tiempo_restante = timedelta(hours=hours) + (time_difference % timedelta(hours=hours))
        fecha_proximo_periodo = now + tiempo_restante

        return (fecha_proximo_periodo)

    @staticmethod
    def next_hour_down(hours: int = 1):
        now = datetime.now()
        start_time = now.replace(hour=2, minute=0, second=0, microsecond=0)
        time_difference = now - start_time
        tiempo_restante = timedelta(hours=hours) - (time_difference % timedelta(hours=hours))
        fecha_proximo_periodo = now + tiempo_restante

        return (fecha_proximo_periodo)

    @staticmethod
    def print_data_frame(message: str, data_frame: pandas.DataFrame):

        if data_frame.empty == False:
            print("#####################################################################################################################")
            print(message)
            print(
                data_frame[[DataFrameColum.SYMBOL.value,
                            DataFrameColum.SIDE_TYPE.value,
                            DataFrameColum.NOTE.value,
                            DataFrameColum.NOTE_3.value,
                            DataFrameColum.WMA_ASCENDING.value,
                            DataFrameColum.ROE.value, 
                            DataFrameColum.PNL.value, 
                            DataFrameColum.STOP_LOSS.value
                            ]])
            print("#####################################################################################################################")
        else:
            print("#####################################################################################################################")
            print(message + " SIN DATOS")
            print("#####################################################################################################################")
