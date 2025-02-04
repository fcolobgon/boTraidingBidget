import time
import numpy
import pandas_ta 

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil

from src.botrading.utils import koncorde
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.dataframe_util import DataFrameUtil
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.colum_good_bad_values import ColumLineValues
from src.botrading.utils.enums.future_values import FutureValues
from src.botrading.telegram.telegram_notify import TelegramNotify
from src.botrading.utils.price_util import PriceUtil
from src.botrading.utils import excel_util
from datetime import datetime, timedelta

#from src.botrading.bit import BitgetClienManager
from src.botrading.utils import traiding_operations

from matplotlib import pyplot as plt
#from scipy.signal import savgol_filter


from configs.config import settings as settings

class Strategy:
    name:str

    def __init__(self, name:str):

        self.name = name
        
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL, ColumStateValues.ERR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame: pandas.DataFrame
        filtered_data_frame = data_frame.query(state_query)

        filtered_df_master = filtered_data_frame

        # NO SE COMPRA EN HORARIO DE FIN DE SEMANA (V - 15:00 a D - 19:00)
        if Strategy.is_weekend_schedule():
            return filtered_data_frame

        filtered_data_frame = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, 
                                                             time_range = TimeRanges("HOUR_4"), num_elements_wma=3)
        #excel_util.save_data_frame( data_frame=filtered_data_frame, exel_name="wma.xlsx")

        # -------------------------------- L O N G  ------------------------------------
        
        query = "(" + DataFrameColum.WMA_ASCENDING.value + " == True)"
        df_long_4H = filtered_data_frame.query(query)

        if df_long_4H.empty == False:
            df_long_4H = Strategy.decision_in_buy (bitget_data_util=bitget_data_util, buy_df = df_long_4H, side_type = "CHECK_LNG", num_elements_wma=3)
            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_long_4H)

        # -------------------------------- S H O R T  ------------------------------------

        query = "(" + DataFrameColum.WMA_ASCENDING.value + " == False)"
        df_short_4H = filtered_data_frame.query(query)

        if df_short_4H.empty == False:
            df_short_4H = Strategy.decision_in_buy (bitget_data_util=bitget_data_util, buy_df = df_short_4H, side_type = "CHECK_SHRT", num_elements_wma=3)
            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_short_4H)

        Strategy.print_data_frame(message="COMPRA ", data_frame=filtered_df_master)

        return filtered_df_master


    @staticmethod
    def apply_sell(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame = data_frame.query(state_query)

        if filtered_data_frame.empty:
            return pandas.DataFrame()
        else:

            startTime = datetime.now()
            startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)

            filtered_data_frame =  bitget_data_util.updating_pnl_roe_orders(data_frame=filtered_data_frame, startTime=startTime)
            #open_order = traiding_operations.get_open_positions(productType='umcbl')
            #excel_util.save_data_frame( data_frame=open_order, exel_name="order.xlsx")

        # -------------------------------- C O N T R O L  V E N T A  M A N U A L  ------------------------------------
            if filtered_data_frame.empty == False:
                query = DataFrameColum.ORDER_OPEN.value + " == False"      
                df_order = filtered_data_frame.query(query)

                if df_order.empty == False:
                    df_order = Strategy.clearing_fields_sell (clean_df = filtered_data_frame) 
                    df_order[DataFrameColum.STATE.value] = ColumStateValues.SELL.value
                    return df_order

        # -------------------------------- C O N T R O L  V E N T A  M A N U A L  ------------------------------------                    

            filtered_data_frame = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, 
                                                                time_range = TimeRanges("HOUR_4"), num_elements_wma=3)
            
            Strategy.print_data_frame(message="VENTA ", data_frame=filtered_data_frame)

            # Listas de EMA vs WMA
            sec_ema = filtered_data_frame[DataFrameColum.MA.value][0]
            sec_wma = filtered_data_frame[DataFrameColum.WMA.value][0]

        # -------------------------------- L O N G  ------------------------------------
        # Opc 0
            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG') and (" + DataFrameColum.WMA_ASCENDING.value + " == False)"
            df_4H_sell = filtered_data_frame.query(query)

            if df_4H_sell.empty == False:
                return Strategy.clearing_fields_sell (clean_df = filtered_data_frame) 

        # -------------------------------- S H O R T  ------------------------------------
        #Opc 0
            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT') and (" + DataFrameColum.WMA_ASCENDING.value + " == True)"
            df_4H_sell = filtered_data_frame.query(query)

            if df_4H_sell.empty == False:
                return Strategy.clearing_fields_sell (clean_df = filtered_data_frame) 


            return filtered_data_frame



# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def decision_in_buy (bitget_data_util: BitgetDataUtil, buy_df:pandas.DataFrame = pandas.DataFrame(), side_type:str = "", num_elements_wma=3):

        time_range = TimeRanges("HOUR_1")
        prices_history_dict = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = buy_df, time_range = time_range, limit=1000)
        buy_df = bitget_data_util.updating_wma(length=20, data_frame=buy_df, prices_history_dict=prices_history_dict, ascending_count=num_elements_wma)
        wma_asc_1h = buy_df.loc[0, DataFrameColum.WMA_ASCENDING.value]

        time_range = TimeRanges("HOUR_2")
        prices_history_dict = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = buy_df, time_range = time_range, limit=1000)
        buy_df = bitget_data_util.updating_wma(length=20, data_frame=buy_df, prices_history_dict=prices_history_dict, ascending_count=num_elements_wma)
        wma_asc_2h = buy_df.loc[0, DataFrameColum.WMA_ASCENDING.value]

        time_range = TimeRanges("DAY_1")
        prices_history_dict = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = buy_df, time_range = time_range, limit=1000)
        buy_df = bitget_data_util.updating_wma(length=20, data_frame=buy_df, prices_history_dict=prices_history_dict, ascending_count=num_elements_wma)
        wma_asc_1d = buy_df.loc[0, DataFrameColum.WMA_ASCENDING.value]

        time_range = TimeRanges("WEEK")
        prices_history_dict = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = buy_df, time_range = time_range, limit=1000)
        buy_df = bitget_data_util.updating_wma(length=20, data_frame=buy_df, prices_history_dict=prices_history_dict, ascending_count=num_elements_wma)
        wma_asc_1w = buy_df.loc[0, DataFrameColum.WMA_ASCENDING.value]

        if (side_type == "CHECK_LNG" and wma_asc_1h and wma_asc_2h and wma_asc_1d and wma_asc_1w) or (side_type == "CHECK_SHRT" and not wma_asc_1h and not wma_asc_2h and not wma_asc_1d and not wma_asc_1w):
            buy_df.loc[:, DataFrameColum.LEVEREAGE.value] = 15
        else:
            buy_df.loc[:, DataFrameColum.LEVEREAGE.value] = 15

        if side_type == "CHECK_LNG":                
            buy_df.loc[:, DataFrameColum.NOTE.value] = "CHECK_LNG"
            buy_df.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
            buy_df.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
        elif side_type == "CHECK_SHRT":   
            buy_df.loc[:, DataFrameColum.NOTE.value] = "CHECK_SHRT"
            buy_df.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            buy_df.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
        
        buy_df.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

        return buy_df

    @staticmethod
    def clearing_fields_sell (clean_df:pandas.DataFrame = pandas.DataFrame()):
        clean_df.loc[:, DataFrameColum.STOP_LOSS.value] = 0.0
        clean_df.loc[:, DataFrameColum.SIDE_TYPE] = "-"
        clean_df.loc[:, DataFrameColum.WMA_ASCENDING] = "-"
        clean_df.loc[:, DataFrameColum.MACD_CRUCE_LINE] = "-"
        clean_df.loc[:, DataFrameColum.MACD_ASCENDING] = "-"
        clean_df.loc[:, DataFrameColum.MACD_CHART_ASCENDING] = "-"
        clean_df.loc[:, DataFrameColum.ROE] = 0.0
        clean_df.loc[:, DataFrameColum.PNL] = 0.0
        clean_df.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value

        return clean_df

    @staticmethod
    def is_weekend_schedule():
        # Obtener la hora actual
        ahora = datetime.now()
        
        # Obtener el día de la semana (0 = lunes, 6 = domingo)
        dia_semana = ahora.weekday()
        
        # Reglas para el horario de trading de fin de semana
        if dia_semana == 4:  # Viernes
            return ahora.time() >= datetime.strptime('15:00', '%H:%M').time()
        elif dia_semana == 5:  # Sábado
            return True
        elif dia_semana == 6:  # Domingo
            return ahora.time() <= datetime.strptime('19:00', '%H:%M').time()
        
        return False

    @staticmethod
    def calculate_indicators (bitget_data_util: BitgetDataUtil, df_master:pandas.DataFrame = pandas.DataFrame(), time_range:TimeRanges=None, num_elements_wma:int=3, 
                              num_elements_macd:int=2, num_elements_chart_macd:int=2, num_elements_ema:int=2):
        
        prices_history_dict = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_master, time_range = time_range, limit=1000)

        filtered_data_frame = bitget_data_util.updating_wma(length=20, data_frame=df_master, prices_history_dict=prices_history_dict, ascending_count=num_elements_wma)
        
        config_macd = ConfigMACD(fast=12, slow=26, signal=9)
        #filtered_data_frame = bitget_data_util.updating_macd(config_macd = config_macd, data_frame = filtered_data_frame, prices_history_dict = prices_history_dict, ascending_count = 2)
        filtered_data_frame = Strategy.updating_macd_modif(bitget_data_util=bitget_data_util, config_macd = config_macd, data_frame = filtered_data_frame, 
                                                           prices_history_dict = prices_history_dict, ascending_count = num_elements_macd, asc_count_chart=num_elements_chart_macd)

        config_ma = ConfigMA(length = 5, type="ema")
        filtered_data_frame = bitget_data_util.updating_ma(config_ma = config_ma, data_frame = filtered_data_frame, prices_history_dict = prices_history_dict, ascending_count=num_elements_ema)
        filtered_data_frame['EMA_5'] = filtered_data_frame[DataFrameColum.MA.value]  
        filtered_data_frame['EMA_5_LST'] = filtered_data_frame[DataFrameColum.MA_LAST.value]  
        filtered_data_frame['EMA_5_ASC'] = filtered_data_frame[DataFrameColum.MA_ASCENDING.value]  

        return filtered_data_frame

    @staticmethod
    def updating_macd_modif(bitget_data_util: BitgetDataUtil, config_macd:ConfigMACD=ConfigMACD(), data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0, asc_count_chart:int = 3):
        
        fast=config_macd.fast
        slow=config_macd.slow
        signal=config_macd.signal

        if DataFrameColum.MACD_GOOD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_GOOD_LINE.value] = "-"

        if DataFrameColum.MACD_BAD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_BAR_CHART.value] = "-"

        if DataFrameColum.MACD_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_LAST.value] = "-"

        if DataFrameColum.MACD_LAST_CHART.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_LAST_CHART.value] = "-"
    
        if DataFrameColum.MACD_PREVIOUS_CHART.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_PREVIOUS_CHART.value] = "-"

        if DataFrameColum.MACD_CHART_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CHART_ASCENDING.value] = "-"

        if DataFrameColum.MACD_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_ASCENDING.value] = "-"

        if DataFrameColum.MACD_CRUCE_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CRUCE_LINE.value] = "-"

        if DataFrameColum.MACD_CRUCE_ZERO.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CRUCE_ZERO.value] = "-"


        chars = "MACDh_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        blue_line = "MACD_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        red_line = "MACDs_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        
        data_frame = DataFrameCheckUtil.create_macd_columns(data_frame=data_frame)
                        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                prices_history = prices_history_dict[symbol]
                
                prices_close = prices_history['Close'].astype(float)

                macd =  pandas_ta.macd(close = prices_close, fast=fast, slow=slow, signal=signal)
                
                macd_numpy = numpy.array(macd[blue_line])
                macd_h_numpy = numpy.array(macd[chars])
                macd_s_numpy = numpy.array(macd[red_line])
                
                macd_numpy = macd_numpy[~numpy.isnan(macd_numpy)]
                macd_h_numpy = macd_h_numpy[~numpy.isnan(macd_h_numpy)]
                macd_s_numpy = macd_s_numpy[~numpy.isnan(macd_s_numpy)]
        
                #data_frame[DataFrameColum.MACD_BAR_CHART.value][ind] = macd_h_numpy
                data_frame[DataFrameColum.MACD_GOOD_LINE.value][ind] = macd_numpy
                data_frame[DataFrameColum.MACD_BAD_LINE.value][ind] = macd_s_numpy
                data_frame.loc[ind, DataFrameColum.MACD_LAST.value] = bitget_data_util.get_last_element(element_list = macd_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_LAST_CHART.value] = bitget_data_util.get_last_element(element_list = macd_h_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_PREVIOUS_CHART.value] = macd_h_numpy[-2]
                data_frame.loc[ind, DataFrameColum.MACD_CHART_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = macd_h_numpy, ascending_count = asc_count_chart)
                data_frame.loc[ind, DataFrameColum.MACD_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = macd_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_LINE.value] = bitget_data_util.good_indicator_on_top_of_bad(macd_numpy, macd_s_numpy)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_ZERO.value] = bitget_data_util.cruce_zero(macd_h_numpy)
                
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "MACD", e)
                continue
        
        return data_frame 

    @staticmethod
    def analyze_trend(bitget_data_util: BitgetDataUtil, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None):
            """Analiza y confirma la tendencia actual"""
            
            # Calcular Supertrend
            config_st = ConfigSupertrend(length = 10, factor=3)
            df = bitget_data_util.updating_supertrend(config_st=config_st, data_frame=data_frame, prices_history_dict=prices_history_dict,)

           
            # Calcular ADX
            config_adx = ConfigADX(series=14)
            adx, plus_di, minus_di = bitget_data_util.updating_adx()
            
            # Calcular EMA de largo plazo
            df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
            
            # Análisis de tendencia
            current_close = df['close'].iloc[-1]
            current_supertrend = supertrend.iloc[-1]
            current_adx = adx.iloc[-1]
            current_plus_di = plus_di.iloc[-1]
            current_minus_di = minus_di.iloc[-1]
            current_ema200 = df['ema200'].iloc[-1]
            
            # Lógica de confirmación de tendencia
            trend = {
                'direction': 'undefined',
                'strength': 'weak',
                'confirmation': False,
                'signals': []
            }
            
            # Confirmar dirección de tendencia
            if current_close > current_supertrend and current_close > current_ema200:
                trend['direction'] = 'bullish'
                trend['signals'].append('Precio por encima del Supertrend y EMA200')
            elif current_close < current_supertrend and current_close < current_ema200:
                trend['direction'] = 'bearish'
                trend['signals'].append('Precio por debajo del Supertrend y EMA200')
                
            # Confirmar fuerza de tendencia
            if current_adx > 25:
                trend['strength'] = 'strong' if current_adx > 35 else 'moderate'
                trend['signals'].append(f'ADX indica tendencia {trend["strength"]} ({current_adx:.2f})')
                
                if current_plus_di > current_minus_di and trend['direction'] == 'bullish':
                    trend['confirmation'] = True
                    trend['signals'].append('DI+ > DI- confirma tendencia alcista')
                elif current_minus_di > current_plus_di and trend['direction'] == 'bearish':
                    trend['confirmation'] = True
                    trend['signals'].append('DI- > DI+ confirma tendencia bajista')
            
            return trend

    @staticmethod
    def print_data_frame(message: str, data_frame: pandas.DataFrame):

        if data_frame.empty == False:
            print("#####################################################################################################################")
            print(message)
            print(
                data_frame[[DataFrameColum.SYMBOL.value,
                            DataFrameColum.SIDE_TYPE.value,
                            DataFrameColum.ROE.value, 
                            DataFrameColum.PNL.value, 
                            DataFrameColum.NOTE_2.value
                            ]])
            print("#####################################################################################################################")
        else:
            print("#####################################################################################################################")
            print(message + " SIN DATOS")
            print("#####################################################################################################################")


    def prediction_next(bitget_data_util: BitgetDataUtil, df_master:pandas.DataFrame = pandas.DataFrame(), time_range:TimeRanges=None, num_elements:int=3):
                # IA

        #Devuelve los datos en una dataframe para trabaja r la IA
        data = bitget_data_util.generate_data_all_crypto(df_master = df_master, time_range = time_range, limit=1000)

        """Prepara características adicionales para el modelo"""
        df = data.copy()
        
        # Características de precio
        df['returns'] = df['Close'].pct_change()
        df['price_range'] = df['High'] - df['Low']
        df['price_position'] = (df['Close'] - df['Low']) / (df['High'] - df['Low'])
        
        # Medias móviles
        df['sma_10'] = df['Close'].rolling(window=10).mean()
        df['sma_20'] = df['Close'].rolling(window=20).mean()
        
        # Volatilidad
        df['volatility'] = df['returns'].rolling(window=10).std()
        
        # Volumen
        df['volume_ma'] = df['Volume'].rolling(window=10).mean()
        df['volume_ratio'] = df['Volume'] / df['volume_ma']
        
        # Momentum
        df['momentum'] = df['Close'] - df['Close'].shift(4)
        
        # Tendencia
        df['trend'] = numpy.where(df['Close'] > df['Close'].shift(1), 1, -1)

        excel_util.save_data_frame( data_frame=df, exel_name="data_ia.xlsx")

        return df.dropna()

    def create_sequences(self, df):
        """Crea secuencias para el modelo LSTM"""
        # Características a usar
        features = [
            'returns', 'price_range', 'price_position', 
            'volatility', 'volume_ratio', 'momentum',
            'Open', 'High', 'Low', 'Close', 'Volume'
        ]
        
        # Normalizar datos
        data_scaled = self.scaler.fit_transform(df[features])
        
        X, y = [], []
        for i in range(len(data_scaled) - self.lookback_periods):
            X.append(data_scaled[i:(i + self.lookback_periods)])
            # 1 si el siguiente cierre es mayor, 0 si es menor
            next_return = df['Close'].iloc[i + self.lookback_periods] > df['Close'].iloc[i + self.lookback_periods - 1]
            y.append(1 if next_return else 0)
            
        return numpy.array(X), numpy.array(y)
        
        # IA


