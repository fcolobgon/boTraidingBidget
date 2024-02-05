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
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL, ColumStateValues.ERR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        if self.first_iteration:
            
            df = DataFrameCheckUtil.add_columns_to_dataframe(
                column_names=[self.step_counter], df=df)
            
            df[self.step_counter] = 0
            df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
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
             
            type="sma"
            length=50
            ma_50 = pandas_ta.ma(type, close, length = length)
            ma_50_prev = ma_50.iloc[-2]
            ma_50 = ma_50.iloc[-1]
            ma_50_asc:bool = ma_50_prev < ma_50
            length=100
            ma_100 = pandas_ta.ma(type, close, length = length)
            ma_100_prev = ma_100.iloc[-2]
            ma_100 = ma_100.iloc[-1]
            ma_100_asc:bool = ma_100_prev < ma_100
            length=150
            ma_150 = pandas_ta.ma(type, close, length = length)
            ma_150_prev = ma_150.iloc[-2]
            ma_150 = ma_150.iloc[-1]
            ma_150_asc:bool = ma_150_prev < ma_150
                        
            #ORDEN SHORT 
            if ma_50 < ma_100 and ma_100 < ma_150:
                if step == 0:
                    step = 1
                    df.loc[ind, self.step_counter] = 1
            
            #ORDEN LONG   
            elif ma_50 > ma_100 and ma_100 > ma_150:
                if step == 0:
                    step = -1
                    df.loc[ind, self.step_counter] = -1
            
            #NO LONG - NO SHORT
            else:
                step = 0
                df.loc[ind, self.step_counter] = 0
                
            #RESET
            #RESET SHORT
            if step > 0:
                #if ma_50_asc == True or  ma_100_asc == True or  ma_150_asc == True:
                if ma_100_asc == True or  ma_150_asc == True:
                    step = 0
                    df.loc[ind, self.step_counter] = 0
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = "-"
            #RESET LONG
            if step < 0:
                #if ma_50_asc == False or  ma_100_asc == False or  ma_150_asc == False:
                if ma_100_asc == False or  ma_150_asc == False:
                    step = 0
                    df.loc[ind, self.step_counter] = 0
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = "-"
                 
            #ORDEN SHORT   
            if step == 1: #SHORT
                step = 2
                df.loc[ind, self.step_counter] = 2
                df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                soportes_resistencias =  TA.PIVOT(prices).iloc[-1]
                compra = soportes_resistencias['r1']
                df.loc[ind, DataFrameColum.NOTE.value] = compra
            
            if step == 2: #SHORT
                compra = df.loc[ind, DataFrameColum.NOTE.value]
                if actual_price > compra:
                    step = 3
                    df.loc[ind, self.step_counter] = 3
                
            if step == 3: #SHORT
                compra = df.loc[ind, DataFrameColum.NOTE.value]
                if actual_price < compra:
                    soportes_resistencias =  TA.PIVOT(prices).iloc[-1]
                    price_place = int(df.loc[ind,DataFrameColum.PRICEPLACE.value])
                    stop = soportes_resistencias['r4']
                    take = soportes_resistencias['s4']
                    stop_p =PriceUtil.porcentaje_valores_absolutos(compra, stop)
                    take_p = PriceUtil.porcentaje_valores_absolutos(compra, take)
                    limit = stop_p*2
                    if limit > take_p:
                        df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                        df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                        df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value       
            
            #ORDEN LONG   
            if step == -1: #LONG
                step = -2
                df.loc[ind, self.step_counter] = -2
                df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                soportes_resistencias =  TA.PIVOT(prices).iloc[-1]
                compra = soportes_resistencias['s1']
                df.loc[ind, DataFrameColum.NOTE.value] = compra
            
            if step == -2: #LONG
                compra = df.loc[ind, DataFrameColum.NOTE.value]
                if actual_price < compra:
                    step = -3
                    df.loc[ind, self.step_counter] = -3
               
            if step == -3: #LONG
                compra = df.loc[ind, DataFrameColum.NOTE.value]
                if actual_price > compra:
                    soportes_resistencias =  TA.PIVOT(prices).iloc[-1]
                    #limits =  [soportes_resistencias['r1'],soportes_resistencias['r2'],soportes_resistencias['r3'],soportes_resistencias['r4']]
                    stop = soportes_resistencias['r4']
                    take = soportes_resistencias['s4']
                    stop_p =PriceUtil.porcentaje_valores_absolutos(compra, stop)
                    take_p = PriceUtil.porcentaje_valores_absolutos(compra, take)
                    limit = stop_p*2
                    if limit > take_p:
                        df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                        df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                        df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value   
        
        sell_df = self.return_for_buy(df=df)
        
        if sell_df.empty:
            #self.print_data_frame(message="COMPRA ACTUALIZADA", data_frame=df)
            return df
        
        TelegramNotify.notify(message="Nueva compra BTC SHORT", settings=settings)
        return sell_df
    
    def calculate_stop(self, price, take, limits):

        # Calcula el porcentaje de diferencia de precio entre price y take
        take_percentage = (take - price) / price * 100

        # Encuentra el elemento de resistencias que tiene un porcentaje de diferencia de precio más cercano a la mitad de take_percentage
        stop = min(limits, key=lambda x: abs((x - price) / price * 100 - take_percentage / 2))

        # Se retorna el elemento de resistencias encontrado
        return stop

    
    def return_for_buy(self, df: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.READY_FOR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = df.query(state_query)
        
        df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
        df[DataFrameColum.LEVEREAGE.value] = self.leverage
        df[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
        df[self.step_counter] = 0
        
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
                    TelegramNotify.notify(message="Nueva venta BTC SHORT", settings=settings)
                    TelegramNotify.notify_df(settings=settings, dataframe=sell_df, message="Nueva venta ", colums=[DataFrameColum.SIDE_TYPE.value, 
                                                                                                   DataFrameColum.TAKE_PROFIT.value,
                                                                                                   DataFrameColum.STOP_LOSS.value,
                                                                                                   DataFrameColum.PERCENTAGE_PROFIT.value])
                    sell_df[DataFrameColum.ORDER_ID.value] = "-"
                    sell_df[self.step_counter] = 0
                    sell_df[DataFrameColum.TAKE_PROFIT.value] = 0.0
                    sell_df[DataFrameColum.STOP_LOSS.value] = 0.0
                    sell_df[DataFrameColum.NOTE.value] = 0.0
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
                DataFrameColum.NOTE.value,            
                self.step_counter
                ]])