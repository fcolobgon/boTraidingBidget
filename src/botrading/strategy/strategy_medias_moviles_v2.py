import pandas
import time
from finta import TA
import pandas_ta 

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.price_util import PriceUtil
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
    percentage = 0.5
    startTime:datetime
    
    
    def __init__(self, name:str):
        
        self.name = name
        self.step_counter = "STEP_COUNTER"
        self.profit = 0.5
        self.leverage = 10
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        
    def get_time_range(self) -> TimeRanges:
        #return TimeRanges("MINUTES_5")
        return TimeRanges("MINUTES_15")


    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        if self.first_iteration:
            
            df = DataFrameCheckUtil.add_columns_to_dataframe(
                column_names=[self.step_counter], df=df)
            
            df[self.step_counter] = 0
            self.first_iteration = False
            self.print_data_frame(message="CREADO DATAFRAME", data_frame=df)
            return df
        
        #return self.return_for_buy_test(bitget_data_util=bitget_data_util, df=df)
        
        self.print_data_frame(message="INICIO COMPRA", data_frame=df)
        time_range = self.get_time_range()
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

        for ind in df.index:

            symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
            step = df.loc[ind, self.step_counter]
            
            prices = prices_history[symbol]
            
            close = prices['Close']
            open = prices['Open']
            actual_price = close.iloc[-1]
            prev_price = close.iloc[-2]
            prev_open_price = open.iloc[-2]
             
            type="sma"
            length=50
            ma_50 = pandas_ta.ma(type, close, length = length)
            ma_50_prev = ma_50.iloc[-2]
            ma_50 = ma_50.iloc[-1]
            length=100
            ma_100 = pandas_ta.ma(type, close, length = length)
            #ma_100_prev = ma_100.iloc[-2]
            ma_100 = ma_100.iloc[-1]
            length=150
            ma_150 = pandas_ta.ma(type, close, length = length)
            #ma_150_prev = ma_150.iloc[-2]
            ma_150 = ma_150.iloc[-1]
            
            #LONG
            if ma_50 > ma_100 and ma_100 > ma_150:
                if  actual_price > ma_50 and step != 3:
                    df.loc[ind, self.step_counter] = 1
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                 
            #SHORT   
            elif ma_50 < ma_100 and ma_100 < ma_150:
                if actual_price < ma_50 and step != 4:
                    df.loc[ind, self.step_counter] = 2
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            else:
                df.loc[ind, self.step_counter] = 0
                df.loc[ind, DataFrameColum.SIDE_TYPE.value] = "-"
                
            if step == 1: #LONG
                    
                if actual_price > ma_100 and actual_price < ma_50:
                    if prev_price < ma_50_prev and prev_open_price < ma_50_prev:
                        df.loc[ind, self.step_counter] = 3
                
            if step == 2: #SHORT
                    
                if actual_price < ma_100 and actual_price > ma_50:
                    if prev_price > ma_50_prev and prev_open_price > ma_50_prev:
                        df.loc[ind, self.step_counter] = 4
                    
            if step == 3: #LONG 
                    
                if prev_price > ma_50_prev:
                    df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                    value_S4 =  TA.PIVOT(prices)['s4'].iloc[-1]
                    df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_S4
                    df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = PriceUtil.plus_percentage_price(actual_price, self.profit)
                
            if step == 4: #SHORT
                    
                if prev_price < ma_50_prev:
                    df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                    value_R4 =  TA.PIVOT(prices)['r4'].iloc[-1]
                    df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_R4
                    df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  PriceUtil.minus_percentage_price(actual_price, self.profit)
        
        
        sell_df = self.return_for_buy(df=df)
        
        if sell_df.empty:
            self.print_data_frame(message="COMPRA ACTUALIZADA", data_frame=df)
            return df
        
        return sell_df

    
    def return_for_buy(self, df: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.READY_FOR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = df.query(state_query)
        
        df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
        df[DataFrameColum.LEVEREAGE.value] = self.leverage
        df[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
        df[self.step_counter] = 5
        
        TelegramNotify.notify_df(settings=settings, dataframe=df, message="Nueva compra ", colums=[DataFrameColum.SIDE_TYPE.value, 
                                                                                                   DataFrameColum.TAKE_PROFIT.value,
                                                                                                   DataFrameColum.STOP_LOSS.value])
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
                    TelegramNotify.notify_df(settings=settings, dataframe=sell_df, message="Nueva venta ", colums=[DataFrameColum.SIDE_TYPE.value, 
                                                                                                   DataFrameColum.TAKE_PROFIT.value,
                                                                                                   DataFrameColum.STOP_LOSS.value,
                                                                                                   DataFrameColum.PERCENTAGE_PROFIT.value])
                    sell_df[DataFrameColum.ORDER_ID.value] = "-"
                    sell_df[self.step_counter] = 0
                    sell_df[DataFrameColum.TAKE_PROFIT.value] = 0.0
                    sell_df[DataFrameColum.STOP_LOSS.value] = 0.0
                    sell_df[DataFrameColum.SIDE_TYPE.value] = "-"
                    sell_df[DataFrameColum.STATE.value] = ColumStateValues.SELL.value
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
                self.step_counter
                ]])