import pandas
import time
from finta import TA
from datetime import datetime, timedelta

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
    update_4h:datetime
    startTime:datetime
    levereage:int
    take_percentage:float
    stop_percentage:float
    
    def __init__(self, name:str):
        
        self.name = name
        self.update_4h = datetime.now()
        self.startTime = datetime.now()
        self.take_percentage = 0.30
        self.stop_percentage = 5
        self.levereage = 5
        
    def get_time_range(self) -> TimeRanges:
        return TimeRanges("HOUR_4")
    
    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        if self.first_iteration:
            
            self.first_iteration = False
            data_frame[DataFrameColum.LOOK.value] = False
            data_frame[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            data_frame[DataFrameColum.LEVEREAGE.value] = self.levereage

            #self.print_data_frame(message="CREADO DATAFRAME", data_frame=data_frame)
            return data_frame
        
        rules = [ColumStateValues.WAIT]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        df_long = self.apply_buy_long_short(bitget_data_util=bitget_data_util, data_frame=df, is_long=True)
        
        if df_long.empty == False:
            return df_long
        
        df_short = self.apply_buy_long_short(bitget_data_util=bitget_data_util, data_frame=df, is_long=False)
        
        if df_short.empty == False:
            return df_short
        
        rules = [ColumStateValues.SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        df_revert_long = self.apply_buy_long_short_revert(bitget_data_util=bitget_data_util, data_frame=df, is_long=True)
        
        if df_revert_long.empty == False:
            return df_revert_long
        
        df_revert_short = self.apply_buy_long_short_revert(bitget_data_util=bitget_data_util, data_frame=df, is_long=False)
        
        if df_revert_short.empty == False:
            return df_revert_short
        
        # Reset una o dos veces al dia/ cada 4h
        #query = DataFrameColum.NOTE.value + " == 'F'"
        #df_reset_sell = df.query(query)
        
        #if df_reset_sell.empty == False:
        #    df_reset_sell[DataFrameColum.STATE.value] = ColumStateValues.WAIT.value
        #    return df_reset_sell
            
        
        return pandas.DataFrame()


    def apply_buy_long_short(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame, is_long:bool) -> pandas.DataFrame:

        df = data_frame
        time_range = TimeRanges("HOUR_4")

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
        df = bitget_data_util.updating_adx(data_frame=df, prices_history_dict=prices_history)
        df = bitget_data_util.updating_ao(data_frame=df, prices_history_dict=prices_history)
            
        self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)
        
        if is_long:
            query = DataFrameColum.AO_ASCENDING.value + " == True"
            df = df.query(query)
            self.print_data_frame(message="DATOS COMPRA AO_ASCENDING LONG FILTRADO", data_frame=df)
        else:
            query = DataFrameColum.AO_ASCENDING.value + " == False"
            df = df.query(query)
            self.print_data_frame(message="DATOS COMPRA AO_ASCENDING SHORT FILTRADO", data_frame=df)

        query = DataFrameColum.ADX_ANGLE.value + " < 110"
        df = df.query(query)
        self.print_data_frame(message="DATOS COMPRA ADX_ANGLE FILTRADO", data_frame=df)
        
        if df.empty == False:
            
            df = df.head(settings.MAX_COIN_BUY)
            
            time_range = self.get_time_range()
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

            for ind in df.index:
                
                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                price_place = int(df.loc[ind,DataFrameColum.PRICEPLACE.value])
                prices = prices_history[symbol]
                
                close = prices['Close']
                actual_price = close.iloc[-1]
                
                if is_long:
                    df.loc[ind,DataFrameColum.NOTE.value] = "S"
                    df.loc[ind,DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                    take = PriceUtil.minus_percentage_price(price=actual_price, percentage=0.5)
                    stop = PriceUtil.plus_percentage_price(price=actual_price, percentage=self.stop_percentage)
                    
                else:
                    df.loc[ind,DataFrameColum.NOTE.value] = "L"
                    df.loc[ind,DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                    take = PriceUtil.plus_percentage_price(price=actual_price, percentage=self.take_percentage)
                    stop = PriceUtil.minus_percentage_price(price=actual_price, percentage=self.stop_percentage)
                    
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value 
           
            TelegramNotify.notify_buy(settings=settings, dataframe=df)
            return df
                
        return pandas.DataFrame()
    
    def apply_buy_long_short_revert(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame, is_long:bool) -> pandas.DataFrame:

        df = data_frame
        time_range = TimeRanges("HOUR_4")

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
        df = bitget_data_util.updating_ao(data_frame=df, prices_history_dict=prices_history)
            
        self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)
        
        if is_long:
            query = DataFrameColum.AO_ASCENDING.value + " == True"
            df = df.query(query)
            self.print_data_frame(message="DATOS COMPRA AO_ASCENDING LONG FILTRADO", data_frame=df)
            query = DataFrameColum.NOTE.value + " == 'S1'"
            df = df.query(query)
            self.print_data_frame(message="DATOS COMPRA NOTE LONG FILTRADO", data_frame=df)
        else:
            query = DataFrameColum.AO_ASCENDING.value + " == False"
            df = df.query(query)
            self.print_data_frame(message="DATOS COMPRA AO_ASCENDING LONG FILTRADO", data_frame=df)
            query = DataFrameColum.NOTE.value + " == 'L1'"
            df = df.query(query)
            self.print_data_frame(message="DATOS COMPRA NOTE LONG FILTRADO", data_frame=df)

        self.print_data_frame(message="DATOS COMPRA ADX_ANGLE FILTRADO", data_frame=df)
        
        if df.empty == False:
            
            df = df.head(settings.MAX_COIN_BUY)
            
            time_range = self.get_time_range()
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

            for ind in df.index:
                
                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                price_place = int(df.loc[ind,DataFrameColum.PRICEPLACE.value])
                prices = prices_history[symbol]
                note = df.loc[ind, DataFrameColum.NOTE.value]
                
                close = prices['Close']
                actual_price = close.iloc[-1]
                
                if is_long:
                    df.loc[ind,DataFrameColum.NOTE.value] = "F"
                    df.loc[ind,DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                    take = PriceUtil.plus_percentage_price(price=actual_price, percentage=self.take_percentage)
                    stop = PriceUtil.minus_percentage_price(price=actual_price, percentage=self.stop_percentage)
                else:
                    df.loc[ind,DataFrameColum.NOTE.value] = "F"
                    df.loc[ind,DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                    take = PriceUtil.minus_percentage_price(price=actual_price, percentage=self.take_percentage)
                    stop = PriceUtil.plus_percentage_price(price=actual_price, percentage=self.stop_percentage)
                
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value 
           
            TelegramNotify.notify_buy(settings=settings, dataframe=df)
            return df
                
        return pandas.DataFrame()
    
    def apply_re_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        df = data_frame
        time_range = TimeRanges("HOUR_4")

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
        df = bitget_data_util.updating_ao(data_frame=df, prices_history_dict=prices_history)
            
        self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)
        

        query = DataFrameColum.NOTE.value + " == 'S'" + " or " + DataFrameColum.NOTE.value + " == 'L'"
        df = df.query(query)
        self.print_data_frame(message="DATOS COMPRA NOTE LONG FILTRADO", data_frame=df)
        
        if df.empty == False:
            
            df = df.head(settings.MAX_COIN_BUY)
            
            time_range = self.get_time_range()
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

            for ind in df.index:
                
                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                price_place = int(df.loc[ind,DataFrameColum.PRICEPLACE.value])
                prices = prices_history[symbol]
                note = df.loc[ind, DataFrameColum.NOTE.value]
                
                close = prices['Close']
                actual_price = close.iloc[-1]
                
                if note == 'S':
                    df.loc[ind,DataFrameColum.NOTE.value] = "L1"
                    df.loc[ind,DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                    take = PriceUtil.plus_percentage_price(price=actual_price, percentage=self.take_percentage)
                    stop = PriceUtil.minus_percentage_price(price=actual_price, percentage=self.stop_percentage)
                else:
                    df.loc[ind,DataFrameColum.NOTE.value] = "S1"
                    df.loc[ind,DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                    take = PriceUtil.minus_percentage_price(price=actual_price, percentage=self.take_percentage)
                    stop = PriceUtil.plus_percentage_price(price=actual_price, percentage=self.stop_percentage)
                
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value 
           
            TelegramNotify.notify_buy(settings=settings, dataframe=df)
            return df
                
        return pandas.DataFrame()
    
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
                    
                    TelegramNotify.notify(message="Nueva venta", settings=settings)
                    
                    df_rebuy = self.apply_re_buy(bitget_data_util=bitget_data_util, data_frame=sell_df)
        
                    if df_rebuy.empty == False:
                        return df_rebuy
                    
                    sell_df[DataFrameColum.ORDER_ID.value] = "-"
                    sell_df[DataFrameColum.TAKE_PROFIT.value] = 0.0
                    sell_df[DataFrameColum.STOP_LOSS.value] = 0.0
                    sell_df[DataFrameColum.STATE.value] = ColumStateValues.SELL.value
                    return sell_df
            
            self.print_data_frame(message="VENTA ", data_frame=df)
            return df    
    
    def print_data_frame(self, message: str="", data_frame: pandas.DataFrame=pandas.DataFrame(), print_empty:bool=True):

        if data_frame.empty == False:
            print(message)
            print("#####################################################################################################################")
            print(
                data_frame[
                    [
                        DataFrameColum.SYMBOL.value,
                        DataFrameColum.STATE.value,
                        DataFrameColum.ADX_ANGLE.value,
                        DataFrameColum.AO_ASCENDING.value,
                        DataFrameColum.PERCENTAGE_PROFIT.value,
                        DataFrameColum.NOTE.value,
                        DataFrameColum.TAKE_PROFIT.value,
                        DataFrameColum.STOP_LOSS.value,
                        DataFrameColum.PERCENTAGE_PROFIT_FLAG.value
                    ]
                ]
            )