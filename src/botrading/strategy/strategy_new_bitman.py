import pandas
import time
from datetime import datetime, timedelta
import numpy
import pandas_ta
from src.botrading.utils import koncorde
from src.botrading.utils import bbwp
from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.price_util import PriceUtil
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.future_values import FutureValues
from ta.trend import ADXIndicator
import beepy as beep

from configs.config import settings as settings

# Nueva estrategia bitman
# Permite comprar multiples monedas, pero solo 1 moneda cada hora
# Reglas:
# AO cruza cero
# ADX ascendente mayor que 18
# Media de koncorde dentro o fuera del area de volumen
# Indicador de volatilidad, solo si es ascendente y es mayor que 25 y menor que 80
class Strategy:
    
    name:str
    first_iteration = True
    update_4h:datetime
    startTime:datetime
    levereage:int
    take_percentage:float
    stop_percentage:float
    percentage_profit_flag:bool
    update_oclock:bool
    adx_min:int
    volatility_min:int
    volatility_max:int
    last_execution:datetime
    
    def __init__(self, name:str):
        
        self.first_iteration = False
        self.name = name
        self.take_percentage = 1
        self.stop_percentage = 90
        self.leverage = 10
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        self.update_oclock = False
        self.adx_min = 20
        self.volatility_min = 40
        self.volatility_max = 60
        self.last_execution = datetime.now()
        self.percentage_profit_flag = False
        
    def esperar_hasta_min(self, minutes:int):
        ahora = self.last_execution
        siguiente_hora = ahora + timedelta(minutes=minutes)
        tiempo_restante = (siguiente_hora - ahora).total_seconds()
        print(f"Esperando hasta la siguiente hora en punto ({siguiente_hora.strftime('%H:%M')})...")
        time.sleep(tiempo_restante)
    
    def esperar_hasta_siguiente_hora(self):
        ahora = datetime.now()
        siguiente_hora = ahora.replace(minute=0, second=0, microsecond=0)
        siguiente_hora += timedelta(hours=1)
        tiempo_restante = (siguiente_hora - ahora).total_seconds()
        print(f"Esperando hasta la siguiente hora en punto ({siguiente_hora.strftime('%H:%M')})...")
        time.sleep(tiempo_restante)

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        df = data_frame
        
        if self.update_oclock and self.first_iteration == False:
            #self.esperar_hasta_siguiente_hora()
            self.esperar_hasta_min(minutes=5)
            self.last_execution = datetime.now()
        
        if self.first_iteration == False:
            self.first_iteration = False
            
        if not self.update_oclock:
            self.update_oclock = True
        
        df = df.query(DataFrameColum.NOTE.value + " == '-'")
        
        print("Obteniendo historial de monedas")
        time_range = TimeRanges("HOUR_1")
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range, limit=1000)
        
        comprar_long = []
        comprar_short = []
        
        print("Evaluando condiciones")
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
                
                volatility = bbwp.calculate(data)                
                volatility_2 = volatility.iloc[-2]
                volatility_1 = volatility.iloc[-1]
                
                if volatility_2 < volatility_1 and volatility_1 > self.volatility_min and volatility_1 < self.volatility_max:
                
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
                        
                        ao_3 = ao[-3]
                        ao_2 = ao[-2]
                        ao_1 = ao[-1]
                        
                        if long:
                            if ao_2 < 0 and ao_1 > 0 and adx_1 > self.adx_min:
                                df.loc[ind, DataFrameColum.NOTE.value] = "Ya comprada"
                                df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                                df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                                if self.percentage_profit_flag:
                                    take = PriceUtil.plus_percentage_price(actual_price, self.take_percentage)
                                    stop = PriceUtil.minus_percentage_price(actual_price, self.stop_percentage)
                                    df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                                    df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                                df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = self.percentage_profit_flag
                                df[DataFrameColum.LEVEREAGE.value] = self.leverage
                                comprar_long.append(symbol)

                        else:
                            if ao_2 > 0 and ao_1 < 0 and adx_1 > self.adx_min:
                                df.loc[ind, DataFrameColum.NOTE.value] = "Ya comprada"
                                df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                                df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                                if self.percentage_profit_flag:
                                    take =  PriceUtil.minus_percentage_price(actual_price, self.take_percentage)
                                    stop =  PriceUtil.plus_percentage_price(actual_price, self.stop_percentage)
                                    df.loc[ind, DataFrameColum.TAKE_PROFIT.value] =  round(take, price_place)
                                    df.loc[ind, DataFrameColum.STOP_LOSS.value] =  round(stop, price_place)
                                df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = self.percentage_profit_flag
                                df[DataFrameColum.LEVEREAGE.value] = self.leverage
                                comprar_short.append(symbol)
            
            except Exception as e:
                print("Error calculando " + symbol)
                print(e)
                continue
                
        rules = [ColumStateValues.READY_FOR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = df.query(state_query)
        
        if df.empty == False:
            beep.beep(sound=1)
            self.print_data_frame(message="COMPRAR", data_frame=df)
            #return pandas.DataFrame()
            return df
        else:
            print("No hay compras posibles ...")
            return pandas.DataFrame()
    
    def apply_sell(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        time.sleep(10)
        return pandas.DataFrame() 
    
    def print_data_frame(self, message: str="", data_frame: pandas.DataFrame=pandas.DataFrame()):

        if data_frame.empty == False:
            print(message)
            print("#####################################################################################################################")
            print(
                data_frame[
                    [
                        DataFrameColum.SYMBOL.value
                    ]
                ]
            )