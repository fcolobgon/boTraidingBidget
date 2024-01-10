import pandas
import time
from finta import TA
import numpy

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
            
            impulse = self.impulse_macd(data=prices_history[symbol])
                
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
    
    def calc_smma(self, src, len):
        if len == 0:
            return src

        smma = numpy.empty(len + 1, dtype=float)
        smma[0] = src[0]
        for i in range(1, len + 1):
            smma[i] = (smma[i - 1] * (len - 1) + src[i]) / len
        return smma

    def calc_zlema(self, src, length):
        ema1 = numpy.empty(length + 1, dtype=float)
        ema1[0] = numpy.mean(src[0:length])
        for i in range(1, length + 1):
            ema1[i] = (ema1[i - 1] * (length - 1) + src[i]) / length
        ema2 = numpy.empty(length + 1, dtype=float)
        ema2[0] = ema1[0]
        for i in range(1, length + 1):
            ema2[i] = (ema2[i - 1] * (length - 1) + ema1[i]) / length
        return ema1 + ema2[-1] - ema1[0]

    def impulse_macd(self, data, length_ma=34, length_signal=9):
        hlc3 = data["Open"] + data["High"] + data["Low"] / 3
        high = self.calc_smma(data["High"], length_ma)
        low = self.calc_smma(data["Low"], length_ma)
        mid = self.calc_zlema(hlc3, length_ma)

        md = numpy.where(mid > high, mid - high, numpy.where(mid < low, mid - low, 0))
        md = md[-length_signal:]
        sb = numpy.mean(md, axis=0)
        sh = md - sb
        
        mid = mid[-length_signal:]
        sb = numpy.repeat(sb, length_signal)
        sh = sh[-length_signal:]
        
        impulse = pandas.DataFrame({
            "MidLine": mid,
            "ImpulseMACD": md,
            "ImpulseHisto": sh,
            "ImpulseMACDCDSignal": sb
        })
       
        print(impulse)
       
        return impulse
    
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