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
#from scipy.signal import savgol_filter


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

        #if Strategy.is_weekend_schedule():
        #    return filtered_data_frame

        filtered_data_frame = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, time_range = TimeRanges("HOUR_4"), 
                                                             num_elements_wma=2, num_elements_macd=2, num_elements_chart_macd=2)

        #excel_util.save_data_frame( data_frame=filtered_data_frame, exel_name="wma.xlsx")

        # -------------------------------- L O N G  ------------------------------------
        query = "(" + DataFrameColum.WMA_ASCENDING.value + " == True)"
        df_long_4H = filtered_data_frame.query(query)

        if df_long_4H.empty == False:

            df_2H = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, time_range = TimeRanges("HOUR_2"), 
                                                             num_elements_wma=3, num_elements_macd=4, num_elements_chart_macd=3)

            query = "(" + DataFrameColum.WMA_ASCENDING.value + " == True)"
            df_long_2H = df_2H.query(query)

            if df_long_2H.empty == False:
                df_long_4H.loc[:, DataFrameColum.NOTE.value] = "CHECK_LNG"
                df_long_4H.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                df_long_4H.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                df_long_4H.loc[:, DataFrameColum.LEVEREAGE.value] = 15
                df_long_4H.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

                filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_long_4H)

        # -------------------------------- S H O R T  ------------------------------------

        #query = "((" + DataFrameColum.WMA_ASCENDING.value + " == False) and (" + DataFrameColum.MACD_ASCENDING.value + " == False))"
        query = "(" + DataFrameColum.WMA_ASCENDING.value + " == False)"
        df_short_4H = filtered_data_frame.query(query)

        if df_short_4H.empty == False:

            df_2H = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, time_range = TimeRanges("HOUR_2"), 
                                                             num_elements_wma=3, num_elements_macd=4, num_elements_chart_macd=3)

            query = "(" + DataFrameColum.WMA_ASCENDING.value + " == False)"
            df_short_2H = df_2H.query(query)

            if df_short_2H.empty == False:
                df_short_4H.loc[:, DataFrameColum.NOTE.value] = "CHECK_SHRT"
                df_short_4H.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                df_short_4H.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                df_short_4H.loc[:, DataFrameColum.LEVEREAGE.value] = 15
                df_short_4H.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_short_4H)

        Strategy.print_data_frame(message="COMPRA ", data_frame=filtered_df_master)

        return filtered_df_master


    @staticmethod
    def apply_sell(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame = data_frame.query(state_query)

        if filtered_data_frame.empty:
            return pandas.DataFrame()
        else:

            startTime = datetime.now()
            startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)

            filtered_data_frame =  bitget_data_util.updating_pnl_roe_orders(data_frame=filtered_data_frame, startTime=startTime)
            #open_order = traiding_operations.get_open_positions(productType='umcbl')
            #excel_util.save_data_frame( data_frame=open_order, exel_name="order.xlsx")

        # -------------------------------- C O N T R O L  V E N T A  M A N U A L  ------------------------------------
            if filtered_data_frame.empty == False:
                query = DataFrameColum.ORDER_OPEN.value + " == False"      
                df_order = filtered_data_frame.query(query)

                if df_order.empty == False:
                    df_order = Strategy.clearing_fields_sell (clean_df = filtered_data_frame) 
                    df_order[DataFrameColum.STATE.value] = ColumStateValues.SELL.value
                    return df_order

        # -------------------------------- C O N T R O L  V E N T A  M A N U A L  ------------------------------------                    

            Strategy.print_data_frame(message="VENTA ", data_frame=filtered_data_frame)

            filtered_data_frame = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, 
                                                                 time_range = TimeRanges("HOUR_2"), num_elements_wma=3, num_elements_macd=2, 
                                                                 num_elements_chart_macd=2)

        # -------------------------------- L O N G  ------------------------------------

            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG') and (" + DataFrameColum.WMA_ASCENDING.value + " == False) "
            df_2H = filtered_data_frame.query(query)

            if df_2H.empty == False:

                df_4H = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, 
                                                       time_range = TimeRanges("HOUR_4"), num_elements_wma=3, num_elements_macd=3, 
                                                       num_elements_chart_macd=2)

                query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG') and (" + DataFrameColum.WMA_ASCENDING.value + " == False) "
                df_4H_sell = df_4H.query(query)

                if df_4H_sell.empty == False:
                    return Strategy.clearing_fields_sell (clean_df = filtered_data_frame) 
                
        # -------------------------------- S H O R T  ------------------------------------

            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT') and (" + DataFrameColum.WMA_ASCENDING.value + " == True)"
            df_2H = filtered_data_frame.query(query)

            if df_2H.empty == False:

                df_4H = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, 
                                                       time_range = TimeRanges("HOUR_4"), num_elements_wma=2, num_elements_macd=3, 
                                                       num_elements_chart_macd=2)

                query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT') and (" + DataFrameColum.WMA_ASCENDING.value + " == True) "
                df_4H_sell = df_4H.query(query)

                if df_4H_sell.empty == False:
                    return Strategy.clearing_fields_sell (clean_df = filtered_data_frame) 


            return filtered_data_frame



# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def clearing_fields_sell (clean_df:pandas.DataFrame = pandas.DataFrame()):
        clean_df.loc[:, DataFrameColum.STOP_LOSS.value] = 0.0
        clean_df.loc[:, DataFrameColum.SIDE_TYPE] = "-"
        clean_df.loc[:, DataFrameColum.WMA_ASCENDING] = "-"
        clean_df.loc[:, DataFrameColum.MACD_CRUCE_LINE] = "-"
        clean_df.loc[:, DataFrameColum.MACD_ASCENDING] = "-"
        clean_df.loc[:, DataFrameColum.MACD_CHART_ASCENDING] = "-"
        clean_df.loc[:, DataFrameColum.ROE] = 0.0
        clean_df.loc[:, DataFrameColum.PNL] = 0.0
        clean_df.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value

        return clean_df


    def is_weekend_schedule():
        # Obtener la hora actual
        ahora = datetime.now()
        
        # Obtener el día de la semana (0 = lunes, 6 = domingo)
        dia_semana = ahora.weekday()
        
        # Reglas para el horario de trading de fin de semana
        if dia_semana == 4:  # Viernes
            return ahora.time() >= datetime.strptime('15:00', '%H:%M').time()
        elif dia_semana == 5:  # Sábado
            return True
        elif dia_semana == 6:  # Domingo
            return ahora.time() <= datetime.strptime('19:00', '%H:%M').time()
        
        return False
    

    @staticmethod
    def calculate_indicators (bitget_data_util: BitgetDataUtil, df_master:pandas.DataFrame = pandas.DataFrame(), time_range:TimeRanges=None, num_elements_wma:int=3, num_elements_macd:int=2, num_elements_chart_macd:int=2):
        prices_history_dict = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_master, time_range = time_range, limit=1000)
        #filtered_data_frame = bitget_data_util.updating_rsi(length=9, data_frame=filtered_data_frame, prices_history_dict=prices_history_dict, ascending_count=2)
        filtered_data_frame = Strategy.updating_wma(bitget_data_util=bitget_data_util, length=20, data_frame=df_master, prices_history_dict=prices_history_dict, ascending_count=num_elements_wma)
        
        config_macd = ConfigMACD(fast=12, slow=26, signal=9)
        #filtered_data_frame = bitget_data_util.updating_macd(config_macd = config_macd, data_frame = filtered_data_frame, prices_history_dict = prices_history_dict, ascending_count = 2)
        filtered_data_frame = Strategy.updating_macd_modif(bitget_data_util=bitget_data_util, config_macd = config_macd, data_frame = filtered_data_frame, 
                                                           prices_history_dict = prices_history_dict, ascending_count = num_elements_macd, asc_count_chart=num_elements_chart_macd)
        
        return filtered_data_frame


    @staticmethod
    def updating_macd_modif(bitget_data_util: BitgetDataUtil, config_macd:ConfigMACD=ConfigMACD(), data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0, asc_count_chart:int = 3):
        
        fast=config_macd.fast
        slow=config_macd.slow
        signal=config_macd.signal

        if DataFrameColum.MACD_GOOD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_GOOD_LINE.value] = "-"

        if DataFrameColum.MACD_BAD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_BAR_CHART.value] = "-"

        if DataFrameColum.MACD_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_LAST.value] = "-"

        if DataFrameColum.MACD_LAST_CHART.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_LAST_CHART.value] = "-"
    
        if DataFrameColum.MACD_PREVIOUS_CHART.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_PREVIOUS_CHART.value] = "-"

        if DataFrameColum.MACD_CHART_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CHART_ASCENDING.value] = "-"

        if DataFrameColum.MACD_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_ASCENDING.value] = "-"

        if DataFrameColum.MACD_CRUCE_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CRUCE_LINE.value] = "-"

        if DataFrameColum.MACD_CRUCE_ZERO.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CRUCE_ZERO.value] = "-"


        chars = "MACDh_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        blue_line = "MACD_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        red_line = "MACDs_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        
        data_frame = DataFrameCheckUtil.create_macd_columns(data_frame=data_frame)
                        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                prices_history = prices_history_dict[symbol]
                
                prices_close = prices_history['Close'].astype(float)

                macd =  pandas_ta.macd(close = prices_close, fast=fast, slow=slow, signal=signal)
                
                macd_numpy = numpy.array(macd[blue_line])
                macd_h_numpy = numpy.array(macd[chars])
                macd_s_numpy = numpy.array(macd[red_line])
                
                macd_numpy = macd_numpy[~numpy.isnan(macd_numpy)]
                macd_h_numpy = macd_h_numpy[~numpy.isnan(macd_h_numpy)]
                macd_s_numpy = macd_s_numpy[~numpy.isnan(macd_s_numpy)]
        
                #data_frame[DataFrameColum.MACD_BAR_CHART.value][ind] = macd_h_numpy
                data_frame[DataFrameColum.MACD_GOOD_LINE.value][ind] = macd_numpy
                data_frame[DataFrameColum.MACD_BAD_LINE.value][ind] = macd_s_numpy
                data_frame.loc[ind, DataFrameColum.MACD_LAST.value] = bitget_data_util.get_last_element(element_list = macd_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_LAST_CHART.value] = bitget_data_util.get_last_element(element_list = macd_h_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_PREVIOUS_CHART.value] = macd_h_numpy[-2]
                data_frame.loc[ind, DataFrameColum.MACD_CHART_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = macd_h_numpy, ascending_count = asc_count_chart)
                data_frame.loc[ind, DataFrameColum.MACD_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = macd_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_LINE.value] = bitget_data_util.good_indicator_on_top_of_bad(macd_numpy, macd_s_numpy)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_ZERO.value] = bitget_data_util.cruce_zero(macd_h_numpy)
                
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "MACD", e)
                continue
        
        return data_frame 

    def updating_wma(bitget_data_util: BitgetDataUtil, length:int = 14, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3):        
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

                print (symbol)

                data_frame.loc[ind, DataFrameColum.WMA_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = wma_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.WMA_LAST.value] = bitget_data_util.get_last_element(element_list = wma_numpy)
               
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "WMA", e)
                continue
        
        return data_frame

    @staticmethod
    def print_data_frame(message: str, data_frame: pandas.DataFrame):

        if data_frame.empty == False:
            print("#####################################################################################################################")
            print(message)
            print(
                data_frame[[DataFrameColum.SYMBOL.value,
                            DataFrameColum.SIDE_TYPE.value,
                            DataFrameColum.WMA_ASCENDING.value,
                            DataFrameColum.ROE.value, 
                            DataFrameColum.PNL.value
                            ]])
            print("#####################################################################################################################")
        else:
            print("#####################################################################################################################")
            print(message + " SIN DATOS")
            print("#####################################################################################################################")
