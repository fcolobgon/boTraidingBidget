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

        filtered_data_frame = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, time_range = TimeRanges("HOUR_4"), 
                                                             num_elements_wma=2, num_elements_macd=2, num_elements_chart_macd=2)

        #excel_util.save_data_frame( data_frame=filtered_data_frame, exel_name="wma.xlsx")

        # -------------------------------- L O N G  ------------------------------------
        query = "(" + DataFrameColum.WMA_ASCENDING.value + " == True) and (" + DataFrameColum.MACD_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_TOP.value + "')"
        df_long_4H = filtered_data_frame.query(query)

        if df_long_4H.empty == False:

            filtered_data_frame = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, time_range = TimeRanges("HOUR_2"), 
                                                             num_elements_wma=4, num_elements_macd=4, num_elements_chart_macd=2)

            query = "(" + DataFrameColum.WMA_ASCENDING.value + " == True) and (" + DataFrameColum.MACD_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_TOP.value + "')"
            df_long_2H = filtered_data_frame.query(query)

            if df_long_2H.empty == False:
                df_long_4H.loc[:, DataFrameColum.NOTE.value] = "CHECK_LNG"
                df_long_4H.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                df_long_4H.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                df_long_4H.loc[:, DataFrameColum.LEVEREAGE.value] = 15
                df_long_4H.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

                filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_long_4H)

        # -------------------------------- S H O R T  ------------------------------------

        #query = "((" + DataFrameColum.WMA_ASCENDING.value + " == False) and (" + DataFrameColum.MACD_ASCENDING.value + " == False))"
        query = "(" + DataFrameColum.WMA_ASCENDING.value + " == False)  and (" + DataFrameColum.MACD_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_DOWN.value + "')"
        df_short_4H = filtered_data_frame.query(query)

        if df_short_4H.empty == False:

            filtered_data_frame = Strategy.calculate_indicators (bitget_data_util=bitget_data_util, df_master = filtered_data_frame, time_range = TimeRanges("HOUR_2"), 
                                                             num_elements_wma=4, num_elements_macd=4, num_elements_chart_macd=2)

            query = "(" + DataFrameColum.WMA_ASCENDING.value + " == True) and (" + DataFrameColum.MACD_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_DOWN.value + "')"
            df_short_2H = filtered_data_frame.query(query)

            if df_short_2H.empty == False:
                df_short_4H.loc[:, DataFrameColum.NOTE.value] = "CHECK_SHRT"
                df_short_4H.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                df_short_4H.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                df_short_4H.loc[:, DataFrameColum.LEVEREAGE.value] = 15
                df_short_4H.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

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
                    df_order[DataFrameColum.ORDER_ID.value] = "-"
                    df_order[DataFrameColum.TAKE_PROFIT.value] = 0.0
                    df_order[DataFrameColum.STOP_LOSS.value] = 0.0
                    df_order.loc[:, DataFrameColum.NOTE.value] = "-"
                    df_order.loc[:, DataFrameColum.NOTE_3.value] = "-"
                    df_order[DataFrameColum.STATE.value] = ColumStateValues.SELL.value

                    return df_order
        # -------------------------------- C O N T R O L  V E N T A  M A N U A L  ------------------------------------                    

            Strategy.print_data_frame(message="VENTA ", data_frame=filtered_data_frame)

            time_range = TimeRanges("HOUR_4")  #DAY_1  HOUR_4  MINUTES_1

            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range, limit=1000)
            filtered_data_frame = Strategy.updating_wma(bitget_data_util=bitget_data_util, length=20, data_frame=filtered_data_frame, prices_history_dict=prices_history, ascending_count=3)

        # -------------------------------- L O N G  ------------------------------------

            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG') and (" + DataFrameColum.WMA_ASCENDING.value + " == False) "
            df_long_step_1 = filtered_data_frame.query(query)

            if df_long_step_1.empty == False:
                df_long_step_1.loc[:, DataFrameColum.STOP_LOSS.value] = 0.0
                df_long_step_1.loc[:, DataFrameColum.NOTE.value] = "-"
                df_long_step_1.loc[:, DataFrameColum.NOTE_3.value] = "-"
                df_long_step_1.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value

                return df_long_step_1 

        # -------------------------------- S H O R T  ------------------------------------

            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT') and (" + DataFrameColum.WMA_ASCENDING.value + " == True)"
            df_short_step_1 = filtered_data_frame.query(query)

            if df_short_step_1.empty == False:
                df_short_step_1.loc[:, DataFrameColum.STOP_LOSS.value] = 0.0
                df_short_step_1.loc[:, DataFrameColum.NOTE.value] = "-"
                df_short_step_1.loc[:, DataFrameColum.NOTE_3.value] = "-"
                df_short_step_1.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value

                return df_short_step_1 

        # ********************************************* OPCION 1 **********************************************
            query = "(" + DataFrameColum.ROE.value + " > " + str(15) + ")"  
            df_op1 = filtered_data_frame.query(query)

            if df_op1.empty == False:
                df_op1.loc[:, DataFrameColum.STOP_LOSS.value] = 0.0
                df_op1.loc[:, DataFrameColum.NOTE.value] = "-"
                df_op1.loc[:, DataFrameColum.NOTE_3.value] = "-"
                df_op1.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
                
                return df_op1

        # -------------------------------- M O V E  S T O P L O S S   ------------------------------------

            #filtered_data_frame.loc[:,DataFrameColum.PRESET_STOP_LOSS_PRICE.value] = 0.5215
            """
            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_LNG')"
            df_lng_sl = filtered_data_frame.query(query)

            if df_lng_sl.empty == False:
            
                for ind in df_lng_sl.index:
                    sl = df_lng_sl.loc[ind, DataFrameColum.STOP_LOSS.value]
            
                    #INFORMAR STOPLOSS

                    return df_lng_sl


            query = "(" + DataFrameColum.NOTE.value + " == 'CHECK_SHRT')"
            df_shrt_sl = filtered_data_frame.query(query)

            if df_shrt_sl.empty == False:
                df_shrt_sl.loc[:, DataFrameColum.STOP_LOSS.value] = 0
                df_shrt_sl.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            """
        
            return filtered_data_frame



# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def calculate_indicators (bitget_data_util: BitgetDataUtil, df_master:pandas.DataFrame = pandas.DataFrame(), time_range:TimeRanges=None, num_elements_wma:int=3, num_elements_macd:int=2, num_elements_chart_macd:int=2):
        prices_history_dict = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_master, time_range = time_range, limit=1000)
        #filtered_data_frame = bitget_data_util.updating_rsi(length=9, data_frame=filtered_data_frame, prices_history_dict=prices_history_dict, ascending_count=2)
        filtered_data_frame = Strategy.updating_wma(bitget_data_util=bitget_data_util, length=20, data_frame=df_master, prices_history_dict=prices_history_dict, ascending_count=num_elements_wma)
        
        config_macd = ConfigMACD(fast=12, slow=26, signal=9)
        #filtered_data_frame = bitget_data_util.updating_macd(config_macd = config_macd, data_frame = filtered_data_frame, prices_history_dict = prices_history_dict, ascending_count = 2)
        filtered_data_frame = Strategy.updating_macd_modif(bitget_data_util=bitget_data_util, config_macd = config_macd, data_frame = filtered_data_frame, prices_history_dict = prices_history_dict, ascending_count = num_elements_macd, asc_count_chart=num_elements_chart_macd)
        
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


    def analizar_wma(valores_wma, ventana_analisis=3, umbral_caida=0.1):
        """
        Analiza los valores WMA para detectar señales de venta
        
        Args:
        valores_wma: Lista de valores WMA
        ventana_analisis: Número de períodos para analizar la tendencia
        umbral_caida: Porcentaje de caída que se considera significativo
        
        Returns:
        dict: Diccionario con el análisis y recomendaciones
        """
        if len(valores_wma) < ventana_analisis:
            return {"error": "No hay suficientes datos para el análisis"}
        
        # Obtener los últimos n valores
        ultimos_valores = valores_wma[-ventana_analisis:]
        
        # Calcular las diferencias porcentuales
        diferencias = [((ultimos_valores[i] - ultimos_valores[i-1])/ultimos_valores[i-1])*100 
                    for i in range(1, len(ultimos_valores))]
        
        # Analizar la tendencia
        tendencia_bajista = all(diff < 0 for diff in diferencias)
        caida_significativa = any(abs(diff) > umbral_caida for diff in diferencias)
        
        # Calcular la velocidad de la caída
        velocidad_caida = sum(diferencias) / len(diferencias)
        
        # Determinar el porcentaje recomendado de venta
        porcentaje_venta = 0
        if tendencia_bajista:
            if caida_significativa:
                porcentaje_venta = 75  # Venta fuerte si hay caída significativa
            else:
                porcentaje_venta = 50  # Venta moderada si la tendencia es bajista pero no significativa
                
        resultado = {
            "tendencia_bajista": tendencia_bajista,
            "caida_significativa": caida_significativa,
            "velocidad_caida": velocidad_caida,
            "porcentaje_venta_recomendado": porcentaje_venta,
            "ultimos_valores": ultimos_valores,
            "diferencias_porcentuales": diferencias
        }
        
        return resultado


    def analizar_wma_avanzado(valores_wma, ventana_analisis=3, ventana_tendencia=10):
        """Análisis más avanzado incluyendo tendencia a largo plazo"""
        analisis_basico = Strategy.analizar_wma(valores_wma, ventana_analisis)
        
        if len(valores_wma) >= ventana_tendencia:
            # Calcular tendencia a largo plazo
            tendencia_largo_plazo = numpy.polyfit(range(ventana_tendencia), 
                                            valores_wma[-ventana_tendencia:], 1)[0]
            
            # Calcular volatilidad
            volatilidad = numpy.std(valores_wma[-ventana_tendencia:])
            
            # Ajustar recomendación basada en tendencia largo plazo
            if tendencia_largo_plazo > 0:
                analisis_basico['porcentaje_venta_recomendado'] *= 0.8  # Más conservador en tendencia alcista
                
            analisis_basico.update({
                'tendencia_largo_plazo': tendencia_largo_plazo,
                'volatilidad': volatilidad
            })
        
        return analisis_basico


    def analizar_plus_wma_avanzado(valores_wma, ventana_analisis=3, ventana_tendencia=10, ventana_suavizado=5, umbral_caida=0.1):
        """
        Análisis avanzado de los valores WMA para detectar señales de venta

        Args:
        valores_wma: Lista de valores WMA
        ventana_analisis: Número de períodos para analizar la tendencia a corto plazo
        ventana_tendencia: Número de períodos para analizar la tendencia a largo plazo
        ventana_suavizado: Número de períodos para suavizar los valores WMA
        umbral_caida: Porcentaje de caída que se considera significativo

        Returns:
        dict: Diccionario con el análisis y recomendaciones
        """
        if len(valores_wma) < ventana_tendencia:
            return {"error": "No hay suficientes datos para el análisis"}

        # Suavizar los valores WMA usando Savitzky-Golay
        valores_wma_suavizados = savgol_filter(valores_wma, ventana_suavizado, 2)

        # Calcular la tendencia a corto plazo
        analisis_basico = Strategy.analizar_wma(valores_wma_suavizados, ventana_analisis, umbral_caida)

        # Calcular la tendencia a largo plazo
        valores_wma_recientes = valores_wma_suavizados[-ventana_tendencia:]
        tendencia_largo_plazo = numpy.polyfit(range(ventana_tendencia), valores_wma_recientes, 1)[0]

        # Calcular la volatilidad a largo plazo
        volatilidad = numpy.std(valores_wma_recientes)

        # Determinar la estrategia de venta basada en la tendencia a corto y largo plazo
        porcentaje_venta = analisis_basico['porcentaje_venta_recomendado']
        if tendencia_largo_plazo > 0:
            if analisis_basico['tendencia_bajista']:
                porcentaje_venta = 75  # Venta fuerte si hay tendencia bajista a corto plazo
            else:
                porcentaje_venta = 25  # Venta moderada si la tendencia a corto plazo es alcista
        else:
            porcentaje_venta = 100  # Venta total si la tendencia a largo plazo es bajista

        # Actualizar el resultado
        analisis_basico.update({
            'tendencia_largo_plazo': tendencia_largo_plazo,
            'volatilidad': volatilidad,
            'porcentaje_venta_recomendado': porcentaje_venta
        })

        return analisis_basico


    def calcular_stop_loss(precio_actual, valores_wma, multiplicador_atr=2):
        """Calcula un stop loss dinámico basado en ATR"""
        atr = calcular_atr(valores_wma)  # Función que deberías implementar
        return precio_actual - (atr * multiplicador_atr)


    def niveles_venta_parcial(precio_entrada, precio_actual, niveles=[0.25, 0.5, 0.75]):
        """Define niveles de venta parcial basados en ganancias"""
        ganancia = (precio_actual - precio_entrada) / precio_entrada
        for nivel in niveles:
            if ganancia >= nivel and nivel not in ventas_realizadas:
                return nivel
        return None


    @staticmethod
    def mark_price_exceeds_limit(data_frame: pandas.DataFrame, value_limit: float = 1) -> pandas.DataFrame:
        """Marcamos las cons con el profit superior al limite marcado"""
        data_frame.loc[data_frame[DataFrameColum.ROE.value] >= value_limit, DataFrameColum.LOOK.value] = 'headdress!'
        return data_frame
    

    def updating_wma(bitget_data_util: BitgetDataUtil, length:int = 14, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3):        
        if DataFrameColum.WMA.value not in data_frame.columns:
            data_frame[DataFrameColum.WMA.value] = "-"

        if DataFrameColum.WMA_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.WMA_LAST.value] = 0.0

        if DataFrameColum.WMA_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.WMA_ASCENDING.value] = "-"
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            try: 
                prices_history = prices_history_dict[symbol]
                close = prices_history['Close'].astype(float)   

                wma = pandas_ta.wma(pandas.Series(close), length = length)

                wma_numpy = numpy.array(wma)
                wma_numpy = wma_numpy[~numpy.isnan(wma_numpy)]
                                
                data_frame[DataFrameColum.WMA.value][ind] = wma_numpy

                print (symbol)

                data_frame.loc[ind, DataFrameColum.WMA_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = wma_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.WMA_LAST.value] = bitget_data_util.get_last_element(element_list = wma_numpy)
               
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "WMA", e)
                continue
        
        return data_frame


    def updating_vwma(bitget_data_util: BitgetDataUtil, length:int = 14, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3):        
        if DataFrameColum.VWMA.value not in data_frame.columns:
            data_frame[DataFrameColum.VWMA.value] = "-"

        if DataFrameColum.VWMA_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.VWMA_LAST.value] = 0.0

        if DataFrameColum.VWMA_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.VWMA_ASCENDING.value] = "-"
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            try: 
                prices_history = prices_history_dict[symbol]
                close = prices_history['Close'].astype(float)   
                volume = prices_history['Volume'].astype(float) 

                vwma = pandas_ta.vwma(close=pandas.Series(close), volume=pandas.Series(volume), length = length)

                vwma_numpy = numpy.array(vwma)
                vwma_numpy = vwma_numpy[~numpy.isnan(vwma_numpy)]
                                
                data_frame[DataFrameColum.VWMA.value][ind] = vwma_numpy

                data_frame.loc[ind, DataFrameColum.VWMA_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = vwma_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.VWMA_LAST.value] = bitget_data_util.get_last_element(element_list = vwma_numpy)
               
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "VWMA", e)
                continue
        
        return data_frame


    def sum_hour(hours: int = 1):
        """
        Devuelve la hora futura ajustada, sumando un número de horas al momento actual.
        
        :param hours: Número de horas a sumar. Por defecto es 1.
        :return: Fecha y hora ajustada con la suma de las horas indicadas.
        """
        now = datetime.now()  # Obtenemos la hora actual
        fecha_proximo_periodo = now + timedelta(hours=hours)  # Sumamos las horas

        return fecha_proximo_periodo

    @staticmethod
    def next_hour_up(hours: int = 1):
        now = datetime.now()
        start_time = now.replace(hour=2, minute=0, second=0, microsecond=0)
        time_difference = now - start_time
        tiempo_restante = timedelta(hours=hours) + (time_difference % timedelta(hours=hours))
        fecha_proximo_periodo = now + tiempo_restante

        return (fecha_proximo_periodo)

    @staticmethod
    def next_hour_down(hours: int = 1):
        now = datetime.now()
        start_time = now.replace(hour=2, minute=0, second=0, microsecond=0)
        time_difference = now - start_time
        tiempo_restante = timedelta(hours=hours) - (time_difference % timedelta(hours=hours))
        fecha_proximo_periodo = now + tiempo_restante

        return (fecha_proximo_periodo)

    @staticmethod
    def print_data_frame(message: str, data_frame: pandas.DataFrame):

        if data_frame.empty == False:
            print("#####################################################################################################################")
            print(message)
            print(
                data_frame[[DataFrameColum.SYMBOL.value,
                            DataFrameColum.SIDE_TYPE.value,
                            DataFrameColum.NOTE.value,
                            DataFrameColum.NOTE_3.value,
                            DataFrameColum.WMA_ASCENDING.value,
                            DataFrameColum.ROE.value, 
                            DataFrameColum.PNL.value, 
                            DataFrameColum.STOP_LOSS.value
                            ]])
            print("#####################################################################################################################")
        else:
            print("#####################################################################################################################")
            print(message + " SIN DATOS")
            print("#####################################################################################################################")
