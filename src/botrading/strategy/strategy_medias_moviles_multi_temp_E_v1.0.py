import time
from finta import TA
import numpy

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.dataframe_util import DataFrameUtil
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.future_values import FutureValues
from src.botrading.telegram.telegram_notify import TelegramNotify

from configs.config import settings as settings

class Strategy:
    
    name:str
    first_iteration = True
    step_counter = "STEP_COUNTER"
    ma_50_colum = "MA_50"
    ma_50_ascending_colum = "MA_50_ASCENDING"
    ma_100_colum = "MA_100"
    ma_100_ascending_colum = "MA_100_ASCENDING"
    ma_150_colum = "MA_150"
    ma_150_ascending_colum = "MA_150_ASCENDING"
    time_range_colum = "TIME_RANGE"
    startTime:datetime
    
    
    def __init__(self, name:str):
        
        self.name = name
        self.step_counter = "STEP_COUNTER"
        self.ma_50_colum = "MA_50"
        self.ma_50_ascending_colum = "MA_50_ASCENDING"
        self.ma_100_colum = "MA_100"
        self.ma_100_ascending_colum = "MA_100_ASCENDING"
        self.ma_150_colum = "MA_150"
        self.ma_150_ascending_colum = "MA_150_ASCENDING"
        self.time_range_colum = "TIME_RANGE"
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        
    def get_time_ranges(self) -> []:
        return ["MINUTES_5", "MINUTES_15", "HOUR_1"]

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        if self.first_iteration:
            
            df = DataFrameCheckUtil.add_columns_to_dataframe(
                column_names=[self.step_counter,
                              self.ma_50_colum,
                              self.ma_50_ascending_colum,
                              self.ma_100_colum,
                              self.ma_100_ascending_colum,
                              self.ma_150_colum,
                              self.ma_150_ascending_colum,
                              self.time_range_colum], df=df)
            
            nuevos_valores_tipo = self.get_time_ranges()
            df = pandas.concat([df.assign(TIME_RANGE=time_range) for time_range in nuevos_valores_tipo], ignore_index=True)
            
            df[self.step_counter] = 0
            self.first_iteration = False

            self.print_data_frame(message="CREADO DATAFRAME", data_frame=df)
            return df
        
        #return self.return_for_buy_test(bitget_data_util=bitget_data_util, df=df)
        time_ranges = self.get_time_ranges()
        
        for t in time_ranges:
            
            time_range = TimeRanges.get_time_range_by_name(t)
            df_t = df.loc[df[self.time_range_colum] == t]

            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_t, time_range = time_range)

            config_ma_50 = ConfigMA(length=50, type="sma")
            df_t = bitget_data_util.updating_ma(config_ma= config_ma_50, data_frame=df_t, prices_history_dict=prices_history)

            df_t[self.ma_50_colum] = df_t[DataFrameColum.MA_LAST.value]
            df_t[self.ma_50_ascending_colum] = df_t[DataFrameColum.MA_ASCENDING.value]
            
            config_ma_100 = ConfigMA(length=100, type="sma")
            df_t = bitget_data_util.updating_ma(config_ma= config_ma_100, data_frame=df_t, prices_history_dict=prices_history)
            
            df_t[self.ma_100_colum] = df_t[DataFrameColum.MA_LAST.value]
            df_t[self.ma_100_ascending_colum] = df_t[DataFrameColum.MA_ASCENDING.value]
            
            config_ma_150 = ConfigMA(length=150, type="sma")
            df_t = bitget_data_util.updating_ma(config_ma= config_ma_150, data_frame=df_t, prices_history_dict=prices_history)
                    
            df_t[self.ma_150_colum] = df_t[DataFrameColum.MA_LAST.value]
            df_t[self.ma_150_ascending_colum] = df_t[DataFrameColum.MA_ASCENDING.value]
            
            self.print_data_frame(message="INICIO COMPRA", data_frame=df_t)
            
            df = DataFrameUtil.replace_rows_df_backup_with_df_for_index(df, df_t)
            
        df_big_data = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history)

        df_big_data = df_big_data.sort_values(by=self.step_counter, ascending=False)
        self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)

        # LONG
        query = self.ma_50_colum + " > " + self.ma_100_colum + " and " + self.ma_100_colum + " > " + self.ma_150_colum
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 1
        df_big_data.loc[df_big_data.query(query).index, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value

        # SHORT
        query = self.ma_50_colum + " < " + self.ma_100_colum + " and " + self.ma_100_colum + " < " + self.ma_150_colum
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 2
        df_big_data.loc[df_big_data.query(query).index, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value

        # LONG - PASO 1
        query = "(" + self.step_counter + " == 1) and (" + DataFrameColum.PRICE_CLOSE.value + " > " + self.ma_100_colum + ") and (" + DataFrameColum.PRICE_CLOSE.value + " < " + self.ma_50_colum + ")"
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 3

        self.print_data_frame(message="INICIO COMPRA", data_frame=df_big_data)

        # SHORT - PASO 2
        query = "(" + self.step_counter + " == 2) and (" + DataFrameColum.PRICE_CLOSE.value + " < " + self.ma_100_colum + ") and (" + DataFrameColum.PRICE_CLOSE.value + " > " + self.ma_50_colum + ")"
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 4

        self.print_data_frame(message="INICIO COMPRA", data_frame=df_big_data)

        # Obtener valores únicos de la columna 'columna_deseada'
        list_time_range = df_big_data[self.time_range_colum].unique()

        for time_range_name in list_time_range:

            time_range = TimeRanges.get_time_range_by_name(time_range_name)

            query = self.time_range_colum + " == '" + time_range_name + "'"
            df_time = df_big_data.query(query)

            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_time, time_range = time_range)
            df_time = bitget_data_util.updating_price_indicators(data_frame=df_time, prices_history_dict=prices_history, previous_period=1) #! PREVIOUS
            df_time = Strategy.calculate_support_and_resistance (df = df_time, currency_data_dictionary = prices_history)

            # LONG - PASO COMPRA
            query = "(" + self.step_counter + " == 3)"
            df_stp_3 = df_time.query(query)

            query = "(" + DataFrameColum.PRICE_CLOSE.value + " > " + self.ma_50_colum + ")"
            df_stp_3 = df_stp_3.query(query)

            if df_stp_3.empty == False:
                TelegramNotify.notify_buy(settings=settings, dataframe=df_stp_3)

                df_stp_3[DataFrameColum.STOP_LOSS.value] = df_stp_3['S4']
                df_stp_3[DataFrameColum.STOP_LOSS.value] = df_stp_3['S4']
                df_stp_3[DataFrameColum.TAKE_PROFIT.value] = df_stp_3['R4'] #! CAMBIAR A PROFIT + PRECIO
                df_stp_3[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
                df_stp_3[DataFrameColum.LEVEREAGE.value] = 5
                df_stp_3[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                df_stp_3[self.step_counter] = 5

                DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = df_big_data, df_slave = df_stp_3)

            # SHORT - PASO COMPRA
            query = "(" + self.step_counter + " == 4)"
            df_stp_4 = df_time.query(query)

            query = "(" + DataFrameColum.PRICE_CLOSE.value + " < " + self.ma_50_colum + ")"
            df_stp_4 = df_stp_4.query(query)

            if df_stp_4.empty == False:
                TelegramNotify.notify_buy(settings=settings, dataframe=df_stp_4)
                df_stp_4[DataFrameColum.STOP_LOSS.value] = df_stp_4['R4']
                df_stp_4[DataFrameColum.TAKE_PROFIT.value] = df_stp_4['S4'] #! CAMBIAR A PROFIT + PRECIO
                df_stp_4[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
                df_stp_4[DataFrameColum.LEVEREAGE.value] = 5
                df_stp_4[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                df_stp_4[self.step_counter] = 5

                DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = df_big_data, df_slave = df_stp_4)
            
                
        return df_big_data
    
    def return_for_buy(self, bitget_data_util: BitgetDataUtil, df: pandas.DataFrame) -> pandas.DataFrame:
        
        profit_percentage = 0.5

        for ind in df.index:
            
            t = df.loc[ind, self.time_range_colum]
            time_range = TimeRanges.get_time_range_by_name(t)
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

            actual_price = df.loc[ind, DataFrameColum.PRICE_CLOSE.value]   
            step = df.loc[ind, self.step_counter]
            
            percentage = actual_price * profit_percentage / 100
            symbol = df[DataFrameColum.SYMBOL.value][ind]
            
            prices_history_dict = prices_history[symbol]

            if step == 3: #LONG
                value_S4 =  TA.PIVOT(prices_history_dict)['s4'].iloc[-1]
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_S4
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = actual_price + percentage
            else: #SHORT
                value_R4 =  TA.PIVOT(prices_history_dict)['r4'].iloc[-1]
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_R4
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = actual_price - percentage
            
            df.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            df.loc[ind, DataFrameColum.LEVEREAGE.value] = 5
            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df.loc[ind, self.step_counter] = 5
            
        self.print_data_frame(message="EJECUTAR COMPRA", data_frame=df)
        return df
    
    def calculate_support_and_resistance (df: pandas.DataFrame, currency_data_dictionary:dict=None):
        
        for ind in df.index:
            symbol = df[DataFrameColum.SYMBOL.value][ind]    
            prices_history_dict = currency_data_dictionary[symbol]

            list_res_sup = TA.PIVOT(prices_history_dict)

            df.loc[ind, "R1"] = list_res_sup['r1'].iloc[-1]
            df.loc[ind, "R2"] = list_res_sup['r2'].iloc[-1]
            df.loc[ind, "R3"] = list_res_sup['r3'].iloc[-1]
            df.loc[ind, "R4"] = list_res_sup['r4'].iloc[-1]

            df.loc[ind, "S1"] = list_res_sup['s1'].iloc[-1]
            df.loc[ind, "S2"] = list_res_sup['s2'].iloc[-1]
            df.loc[ind, "S3"] = list_res_sup['s3'].iloc[-1]
            df.loc[ind, "S4"] = list_res_sup['s4'].iloc[-1]
        
        return df
    
    def apply_sell(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        if df.empty:
            
            time.sleep(2)
            return pandas.DataFrame()
        else:
            
            df =  bitget_data_util.updating_open_orders(data_frame=df, startTime=self.startTime)
            
            if df.empty == False:
                query = DataFrameColum.ORDER_OPEN.value + " == False"      
                sell_df = df.query(query)
                if sell_df.empty == False:
                    TelegramNotify.notify_sell(settings=settings, dataframe=sell_df)
                    return sell_df
            
            self.print_data_frame(message="VENTA ", data_frame=df)
            return df

    def return_for_buy_test(self, bitget_data_util: BitgetDataUtil, df: pandas.DataFrame) -> pandas.DataFrame:
        
        for ind in df.index:
            
            df.loc[ind, DataFrameColum.STOP_LOSS.value] =  57001.3
            df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = 35001.4
            df.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            df.loc[ind, DataFrameColum.LEVEREAGE.value] = 5
            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df.loc[ind, self.step_counter] = 5
            df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            
        self.print_data_frame(message="EJECUTAR COMPRA", data_frame=df)
        return df
    
    def print_data_frame(self, message: str, data_frame: pandas.DataFrame, print_empty:bool=True):

        if data_frame.empty == False:
            print(message)
            print("#####################################################################################################################")
            print(data_frame[[
                DataFrameColum.ORDER_ID.value,
                DataFrameColum.BASE.value,
                DataFrameColum.PERCENTAGE_PROFIT.value,
                DataFrameColum.TAKE_PROFIT.value,
                DataFrameColum.STOP_LOSS.value,
                DataFrameColum.SIDE_TYPE.value,            
                self.step_counter,
                DataFrameColum.PRICE_CLOSE.value,
                self.ma_50_colum,
                self.ma_50_ascending_colum,
                self.ma_100_colum,
                self.ma_100_ascending_colum,
                self.ma_150_colum,
                self.ma_150_ascending_colum,
                self.time_range_colum
                ]])
            