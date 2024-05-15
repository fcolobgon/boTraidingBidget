import pandas
import time
import beepy
from finta import TA
from datetime import datetime, timedelta

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
        self.take_percentage = 0.35
        self.stop_percentage = 5
        self.levereage = 10

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        df = data_frame
        #time_range = TimeRanges("DAY_1")
        time_range = TimeRanges("HOUR_4")
        #time_range = TimeRanges("HOUR_1")
        #time_range = TimeRanges("MINUTES_15")
        #time_range = TimeRanges("MINUTES_5")
        

        mirar_bitman_long = []
        mirar_bitman_short = []
        mirar_media_zero = []
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range, limit=1000)
        #df = bitget_data_util.updating_koncorde(data_frame=df, prices_history_dict=prices_history)
        
        df = bitget_data_util.updating_adx(data_frame=df, prices_history_dict=prices_history)
        
        query = DataFrameColum.ADX_ASCENDING.value + " == True"
        df = df.query(query)
        
        query = DataFrameColum.ADX_LAST.value + " > 20"
        df = df.query(query)
        
        df = df.sort_values(by=[DataFrameColum.ADX_LAST.value,
                                DataFrameColum.ADX_ASCENDING.value], 
                            ascending=[False, 
                                       True])
        
        for ind in df.index:
            
            try:

                look = False
                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                data = prices_history[symbol]
                
                koncorde_df = koncorde.calculate(data=data)
                
                # Se muestran las primeras 5 filas del DataFrame original
                #print(koncorde_df.head())

                # 1. Limpiar koncorde_df para mantener únicamente las 5 últimas filas
                koncorde_df = koncorde_df.tail(4)
                #print(koncorde_df)
                
                #periodo_menos = True
                periodo_menos = False
                
                if periodo_menos:
                    koncorde_df = koncorde_df.drop(koncorde_df.index[-1])
                
                azul_3 = koncorde_df['azul'].iloc[-3]
                azul_2 = koncorde_df['azul'].iloc[-2]
                azul_1 = koncorde_df['azul'].iloc[-1]
                
                verde_3 = koncorde_df['verde'].iloc[-3]
                verde_2 = koncorde_df['verde'].iloc[-2]
                verde_1 = koncorde_df['verde'].iloc[-1]
                
                media_3 = koncorde_df['media'].iloc[-3]
                media_2 = koncorde_df['media'].iloc[-2]
                media_1 = koncorde_df['media'].iloc[-1]

                # LONG
                if (azul_3 < media_3 and azul_2 < media_2 and azul_1 > media_1) and azul_2 < azul_1:
                    if (verde_3 < media_3 and verde_2 < media_2 and verde_1 < media_1):
                        look = True
                        mirar_bitman_long.append(symbol)
                
                if (verde_3 < media_3 and verde_2 < media_2 and verde_1 > media_1) and verde_2 < verde_1:
                    if (azul_3 < media_3 and azul_2 < media_2 and azul_1 < media_1):
                        look = True
                        mirar_bitman_long.append(symbol)
                
                if (media_1 < 0):
                    look = True
                    mirar_media_zero.append(symbol)
                
                # SHORT
                if (azul_2 > media_2 and azul_1 < media_1) and (verde_1 < media_1) and azul_2 > azul_1:
                    look = True
                    mirar_bitman_short.append(symbol)
                
                if (azul_1 < media_1) and (verde_2 > media_2 and verde_1 < media_1) and verde_2 < verde_1:
                    look = True
                    mirar_bitman_short.append(symbol)
            
            except Exception as e:
                print("Error calculando " + symbol)
                print(e)
                continue 
            
        print("---------- SHORT ---------------")
        print(mirar_bitman_short)
        #print("MARRON")
        #print(mirar_marron_short)
        
        
        print("-------------------------------")
        
        print("---------- LONG ---------------")
        print(mirar_bitman_long)
        
        print("---------- LONG ---------------")
        print("MEDIA ZERO")
        print(mirar_media_zero)

        if mirar_bitman_long or mirar_bitman_short:
            beepy.beep(sound='ready')
        
        return pandas.DataFrame()
    
    def apply_sell(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
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