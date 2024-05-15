import pandas
import time
from datetime import datetime, timedelta
import numpy
import pandas_ta
from src.botrading.utils import koncorde
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
from ta.trend import ADXIndicator

from configs.config import settings as settings

class Strategy:
    
    name:str
    first_iteration = True
    update_4h:datetime
    startTime:datetime
    levereage:int
    take_percentage:float
    stop_percentage:float
    update_oclock:bool
    only_one_coin:bool
    notify:bool
    
    def __init__(self, name:str):
        
        self.first_iteration = False
        self.name = name
        self.take_percentage = 1
        self.stop_percentage = 50
        self.leverage = 10
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        self.update_oclock = False
        self.only_one_coin = False
        self.notify = True
    
    def esperar_hasta_siguiente_hora(self):
        ahora = datetime.now()
        siguiente_hora = ahora.replace(minute=0, second=0, microsecond=0)
        siguiente_hora += timedelta(hours=1)
        tiempo_restante = (siguiente_hora - ahora).total_seconds()
        print(f"Esperando hasta la siguiente hora en punto ({siguiente_hora.strftime('%H:%M')})...")
        time.sleep(tiempo_restante)

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        df = data_frame
        
        if self.update_oclock:
            self.esperar_hasta_siguiente_hora()
        
        df = df.query(DataFrameColum.NOTE.value + " == '-'")
        
        #time_range = TimeRanges("DAY_1")
        #time_range = TimeRanges("HOUR_4")
        time_range = TimeRanges("HOUR_1")
        #time_range = TimeRanges("HOUR_1")
        #time_range = TimeRanges("MINUTES_15")
        #time_range = TimeRanges("MINUTES_5")

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range, limit=1000)
        
        comprar_long = []
        comprar_short = []
        
        for ind in df.index:
            
            try:

                long = False
                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                price_place = int(df.loc[ind,DataFrameColum.PRICEPLACE.value])
                data = prices_history[symbol]
                prices_high = data['High']
                prices_low = data['Low']
                prices_close = data['Close']
                actual_price = prices_close.iloc[-1]
                
                adx = numpy.array(ADXIndicator(high = prices_high, low = prices_low, close = prices_close, window= 14).adx())
                
                adx_2 = adx[-2]
                adx_1 = adx[-1]
                
                if adx_2 < adx_1:
                
                    koncorde_df = koncorde.calculate(data=data)

                    azul_1 = koncorde_df['azul'].iloc[-1]
                    verde_1 = koncorde_df['verde'].iloc[-1]
                    media_1 = koncorde_df['media'].iloc[-1]

                    # LONG
                    if (azul_1 > media_1):
                        if (verde_1 < media_1):
                            long = True
                    
                    if (verde_1 > media_1):
                        if (azul_1 < media_1):
                            long = True
                    
                    # SHORT
                    if (azul_1 < media_1) and (verde_1 < media_1):
                        long = False
                        
                    ao = numpy.array(pandas_ta.ao(high = prices_high, low = prices_low))
                    
                    ao_2 = ao[-2]
                    ao_1 = ao[-1]
                    
                    if long:
                        if ao_2 < 0 and ao_1 > 0 and adx_1 > 18:
                            df.loc[ind, DataFrameColum.NOTE.value] = "Ya comprada"
                            df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                            take = PriceUtil.plus_percentage_price(actual_price, self.take_percentage)
                            stop = PriceUtil.minus_percentage_price(actual_price, self.stop_percentage)
                            df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                            df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                            df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
                            df[DataFrameColum.LEVEREAGE.value] = self.leverage
                            comprar_long.append(symbol)
                            if self.notify:
                                TelegramNotify.notify(settings=settings, message="Se va a comprar LONG " + str(symbol))
                            if self.only_one_coin:
                                break
                    else:
                        if ao_2 > 0 and ao_1 < 0 and adx_1 > 18:
                            df.loc[ind, DataFrameColum.NOTE.value] = "Ya comprada"
                            df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                            take =  PriceUtil.minus_percentage_price(actual_price, self.take_percentage)
                            stop =  PriceUtil.plus_percentage_price(actual_price, self.stop_percentage)
                            df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                            df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                            df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
                            df[DataFrameColum.LEVEREAGE.value] = self.leverage
                            comprar_short.append(symbol)
                            if self.notify:
                                TelegramNotify.notify(settings=settings, message="Se va a comprar SHORT " + str(symbol))
                            if self.only_one_coin:
                                break
                    
            
            except Exception as e:
                print("Error calculando " + symbol)
                print(e)
                continue
            
        print("------- COMPRAR LONG -------")
        print(comprar_long)
        print("------- COMPRAR SHORT -------")
        print(comprar_short)
        
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
                    sell_df[DataFrameColum.STATE.value] = ColumStateValues.SELL.value
                    return sell_df
            
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
                        DataFrameColum.PERCENTAGE_PROFIT.value,
                        DataFrameColum.NOTE.value,
                        DataFrameColum.TAKE_PROFIT.value,
                        DataFrameColum.STOP_LOSS.value,
                        DataFrameColum.PERCENTAGE_PROFIT_FLAG.value
                    ]
                ]
            )