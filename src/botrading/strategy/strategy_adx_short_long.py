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
    
    def __init__(self, name:str):
        
        self.name = name
        self.update_4h = datetime.now()
        
    def get_time_range(self) -> TimeRanges:
        return TimeRanges("HOUR_4")

    def side_type(self, row):
        
        if row[DataFrameColum.AO_ASCENDING.value] == True:
            return FutureValues.SIDE_TYPE_LONG.value
        
        if row[DataFrameColum.AO_ASCENDING.value] == False:
            return FutureValues.SIDE_TYPE_SHORT.value
        
        return "-"

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.SELL, ColumStateValues.WAIT]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        fecha_actual = datetime.now()
        
        if self.first_iteration:
            
            self.first_iteration = False
            df[DataFrameColum.LOOK.value] = False
            df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            df[DataFrameColum.LEVEREAGE.value] = 10

            self.print_data_frame(message="CREADO DATAFRAME", data_frame=df)
            return df
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL, ColumStateValues.ERR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = df.query(state_query)
        
        if fecha_actual > self.update_4h:
            
            self.update_4h = fecha_actual + timedelta(hours=1)
            
            df = df.query(DataFrameColum.LOOK.value + " == False")        
            
            time_range = TimeRanges("HOUR_4")
            print("Obteniendo historial de monedas a 4h")
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
            df = bitget_data_util.updating_adx(data_frame=df, prices_history_dict=prices_history)
            df = bitget_data_util.updating_ao(data_frame=df, prices_history_dict=prices_history)
            
            self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)
            
            df = df.apply(self.side_type, axis=1)
            self.print_data_frame(message="DATOS COMPRA AO_ASCENDING FILTRADO", data_frame=df)

            query = DataFrameColum.ADX_ANGLE.value + " < 110"
            df = df.query(query)
            self.print_data_frame(message="DATOS COMPRA ADX_ANGLE FILTRADO", data_frame=df)
            
            if df.empty == False:
                
                df[DataFrameColum.LOOK.value] = True
                                
                for ind in df.index:
                    
                    symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                    prices = prices_history[symbol]
                    soportes_resistencias =  TA.PIVOT(prices).iloc[-1]
                    df.loc[ind, DataFrameColum.SOPORTES.value] = soportes_resistencias['s1']
                
                self.print_data_frame(message="SOPORTES CALCULADOS", data_frame=df)

                return df
                
        df = df.query(DataFrameColum.LOOK.value + " == True")        
        df = bitget_data_util.updating_price(data_frame = df)  

        if df.empty:
            self.update_4h = fecha_actual
            return df          
        
        self.print_data_frame(message="EJECUCION COMPRA ", data_frame=df)
        
        query = DataFrameColum.PRICE_BUY.value + " < " + DataFrameColum.SOPORTES.value
        df = df.query(query)
        
        if df.empty == False:
            
            time_range = self.get_time_range()
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

            for ind in df.index:
                
                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                price_place = int(df.loc[ind,DataFrameColum.PRICEPLACE.value])
                prices = prices_history[symbol]
                
                close = prices['Close']
                actual_price = close.iloc[-1]
                
                take = PriceUtil.plus_percentage_price(price=actual_price, percentage=1)
                stop = PriceUtil.minus_percentage_price(price=actual_price, percentage=1)
                
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
                    TelegramNotify.notify(message="Nueva venta LONG", settings=settings)
                    TelegramNotify.notify_df(settings=settings, dataframe=sell_df, message="Nueva venta ", colums=[DataFrameColum.SIDE_TYPE.value, 
                                                                                                   DataFrameColum.TAKE_PROFIT.value,
                                                                                                   DataFrameColum.STOP_LOSS.value,
                                                                                                   DataFrameColum.PERCENTAGE_PROFIT.value])
                    sell_df[DataFrameColum.ORDER_ID.value] = "-"
                    sell_df[DataFrameColum.TAKE_PROFIT.value] = 0.0
                    sell_df[DataFrameColum.STOP_LOSS.value] = 0.0
                    sell_df[DataFrameColum.STOP_LOSS_LEVEL.value] = 0
                    sell_df[DataFrameColum.LOOK.value] = False
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
                        DataFrameColum.ADX_ANGLE.value,
                        DataFrameColum.AO_ASCENDING.value,
                        DataFrameColum.SOPORTES.value,
                        DataFrameColum.STOP_LOSS_LEVEL.value,
                        DataFrameColum.PERCENTAGE_PROFIT.value,
                        DataFrameColum.LOOK.value
                    ]
                ]
            )