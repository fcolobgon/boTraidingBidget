import pandas
import time
from finta import TA

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
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
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        
    def get_time_range(self) -> TimeRanges:
        return TimeRanges("MINUTES_5")
        #return TimeRanges("MINUTES_15")


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
                              self.ma_150_ascending_colum], df=df)
            
            df[self.step_counter] = 0
            self.first_iteration = False
            self.print_data_frame(message="CREADO DATAFRAME", data_frame=df)
            return df
        
        #return self.return_for_buy_test(bitget_data_util=bitget_data_util, df=df)
        
        self.print_data_frame(message="INICIO COMPRA", data_frame=df)
        time_range = self.get_time_range()
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

        config_ma_50 = ConfigMA(length=50, type="sma")
        df = bitget_data_util.updating_ma(config_ma= config_ma_50, data_frame=df, prices_history_dict=prices_history)

        df[self.ma_50_colum] = df[DataFrameColum.MA_LAST.value]
        df[self.ma_50_ascending_colum] = df[DataFrameColum.MA_ASCENDING.value]
        
        config_ma_100 = ConfigMA(length=100, type="sma")
        df = bitget_data_util.updating_ma(config_ma= config_ma_100, data_frame=df, prices_history_dict=prices_history)
        
        df[self.ma_100_colum] = df[DataFrameColum.MA_LAST.value]
        df[self.ma_100_ascending_colum] = df[DataFrameColum.MA_ASCENDING.value]
        
        config_ma_150 = ConfigMA(length=150, type="sma")
        df = bitget_data_util.updating_ma(config_ma= config_ma_150, data_frame=df, prices_history_dict=prices_history)
                
        df[self.ma_150_colum] = df[DataFrameColum.MA_LAST.value]
        df[self.ma_150_ascending_colum] = df[DataFrameColum.MA_ASCENDING.value]
        
        df = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history)

        df = df.sort_values(by=self.step_counter, ascending=False)
        self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)
        
        for ind in df.index:

            symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
            
            step = df.loc[ind, self.step_counter]
            
            #if step == 0:
                
            ma_50 = df.loc[ind, self.ma_50_colum]
            ma_50_ascending = df.loc[ind, self.ma_50_ascending_colum]
                
            ma_100 = df.loc[ind, self.ma_100_colum]
            ma_100_ascending = df.loc[ind, self.ma_100_ascending_colum]
                
            ma_150 = df.loc[ind, self.ma_150_colum]
            ma_150_ascending = df.loc[ind, self.ma_150_ascending_colum]
            
            if ma_50 > ma_100 and ma_100 > ma_150:
                if ma_50_ascending and ma_100_ascending and ma_150_ascending:
                    df.loc[ind, self.step_counter] = 1
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                    
                    self.print_data_frame(message=symbol + " -> PASO 0 FINALIZADO", data_frame=df)
                    return df
                
            if ma_50 < ma_100 and ma_100 < ma_150:
                if not ma_50_ascending and not ma_100_ascending and not ma_150_ascending:
                    df.loc[ind, self.step_counter] = 2
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            
                    self.print_data_frame(message=symbol + " -> PASO 0 FINALIZADO", data_frame=df)
                    return df
            
            if step == 1: #LONG
                
                actual_price = df.loc[ind, DataFrameColum.PRICE_CLOSE.value]
                ma_50 = df.loc[ind, self.ma_50_colum]
                ma_100 = df.loc[ind, self.ma_100_colum]
                
                #Tipo long
                if actual_price > ma_100 and actual_price < ma_50:
                    
                    df.loc[ind, self.step_counter] = 3
                    self.print_data_frame(message=symbol + " -> PASO 1 FINALIZADO", data_frame=df)
                    return df
            
            if step == 2: #SHORT
                
                actual_price = df.loc[ind, DataFrameColum.PRICE_CLOSE.value]
                ma_50 = df.loc[ind, self.ma_50_colum]
                ma_100 = df.loc[ind, self.ma_100_colum]
                
                #Tipo long
                if actual_price < ma_100 and actual_price > ma_50:
                    
                    df.loc[ind, self.step_counter] = 4
                    self.print_data_frame(message=symbol + " -> PASO 2 FINALIZADO", data_frame=df)
                    return df
                
            if step == 3: #LONG 
                
                time_range = self.get_time_range()
                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
                df = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history, previous_period=1)
                
                previous_price = df.loc[ind, DataFrameColum.PRICE_CLOSE.value]
                ma_50 = df.loc[ind, self.ma_50_colum]
                
                if previous_price > ma_50:
                    TelegramNotify.notify_buy(settings=settings, dataframe=df)
                    return self.return_for_buy(bitget_data_util=bitget_data_util, df=df)
            
            if step == 4: #SHORT
                
                time_range = self.get_time_range()
                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
                df = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history, previous_period=1)
                
                previous_price = df.loc[ind, DataFrameColum.PRICE_CLOSE.value]
                ma_50 = df.loc[ind, self.ma_50_colum]
                
                if previous_price < ma_50:
                    TelegramNotify.notify_buy(settings=settings, dataframe=df)
                    return self.return_for_buy(bitget_data_util=bitget_data_util, df=df)
                
        return pandas.DataFrame()
    
    def return_for_buy_test(self, bitget_data_util: BitgetDataUtil, df: pandas.DataFrame) -> pandas.DataFrame:
        
        for ind in df.index:
            
            df.loc[ind, DataFrameColum.STOP_LOSS.value] =  47000
            df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = 45000
            df.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            df.loc[ind, DataFrameColum.LEVEREAGE.value] = 5
            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df.loc[ind, self.step_counter] = 5
            df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            
        self.print_data_frame(message="EJECUTAR COMPRA", data_frame=df)
        return df
    
    def return_for_buy(self, bitget_data_util: BitgetDataUtil, df: pandas.DataFrame) -> pandas.DataFrame:
        
        profit_percentage = 0.5
        time_range = self.get_time_range()
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

        df = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history)
        
        for ind in df.index:

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
                self.ma_50_colum,
                self.ma_50_ascending_colum,
                self.ma_100_colum,
                self.ma_100_ascending_colum,
                self.ma_150_colum,
                self.ma_150_ascending_colum
                ]])