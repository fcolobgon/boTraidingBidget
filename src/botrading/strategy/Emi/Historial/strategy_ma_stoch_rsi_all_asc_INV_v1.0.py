import time
from finta import TA
import numpy

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.bit import BitgetClienManager

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

from configs.config import settings as settings

class Strategy:
    name:str

    def __init__(self, name:str):
        
        self.name = name
        self.step_counter = "STEP_COUNTER"
        self.ma_50_colum = "MA_50"
        self.ma_50_ascending_colum = "MA_50_ASCENDING"
        self.ma_50_angle_colum = "MA_50_ANGLE"
        self.ma_100_colum = "MA_100"
        self.ma_100_ascending_colum = "MA_100_ASCENDING"
        self.ma_150_colum = "MA_150"
        self.ma_150_ascending_colum = "MA_150_ASCENDING"
        self.time_range_colum = "TIME_RANGE"
        self.diferencia_percentual_column = "DIFERENCIA_PERCENTUAL"
        self.parallel_limit_column = "PARALLEL_LIMIT"
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL, ColumStateValues.ERR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame: pandas.DataFrame
        filtered_data_frame = data_frame.query(state_query)

        #Inicializar NOTE_2 como limit profit para el SELL
        filtered_data_frame.loc[:, DataFrameColum.NOTE_2.value] = 0.0
        filtered_data_frame[DataFrameColum.NOTE_2.value] = filtered_data_frame[DataFrameColum.NOTE_2.value].astype(float)

        filtered_df_master = filtered_data_frame

        # ----------------- CLASIFICACIÓN DE  COINS POR TIEMPO -----------------
        # Actualizamos fecha de la siguiente ejecución
        query = DataFrameColum.NOTE_3.value + " == '-'"
        filtered_data_frame.loc[filtered_data_frame.query(query).index, DataFrameColum.NOTE_3.value] = datetime.now()
        filtered_data_frame[DataFrameColum.NOTE_3.value] = pandas.to_datetime(filtered_data_frame[DataFrameColum.NOTE_3.value], format='%d-%m-%Y %H:%M:%S')
        
        #Copiamos la inf en el DF bckp
        filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = filtered_data_frame)
        filtered_data_frame = Strategy.unlocking_time_locked_crypto (data_frame = filtered_data_frame, time_column = DataFrameColum.NOTE_3.value)
        # -----------------  CLASIFICACIÓN DE  COINS POR TIEMPO  -----------------

        lista_coins = ["BTCUSDT_UMCBL","ETHUSDT_UMCBL","SUIUSDT_UMCBL","ZETAUSDT_UMCBL","BCHUSDT_UMCBL","XRPUSDT_UMCBL"]
        df_adx = self.apply_buy_long_giro_adx(bitget_data_util = bitget_data_util, df_slave = filtered_data_frame, lista_coins = lista_coins)
        filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_adx)
        
        if df_adx.empty == False: 
            return filtered_df_master
        
        time_range = TimeRanges("MINUTES_15")  # MINUTES_5   HOUR_1
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range)

        df_long = self.apply_buy_long_short_macd_ma_stochrsi(bitget_data_util=bitget_data_util, prices_history = prices_history, df_slave=filtered_data_frame,time_range = time_range)
        filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_long)
        
        if df_long.empty == False:
            return filtered_df_master
        
        #--------------------------------------------------------- Step_2 - Waiting Short ------------------------------------------------------
        """
        # Contar cuántas veces aparece "casa"
        count_waiting_shorts = filtered_data_frame[DataFrameColum.NOTE_4.value].value_counts().get('Step_2 - Waiting Short', 0)

        query = DataFrameColum.NOTE_4.value + " == 'Step_2 - Waiting Short'"
        df_ws = filtered_data_frame.query(query)

        if not df_ws.empty:
            for ind in df_ws.index:

                time_range = df_ws.loc[ind, DataFrameColum.NOTE_5.value]

                symbol = df_ws.loc[ind,DataFrameColum.SYMBOL.value]

                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_ws, time_range = TimeRanges(time_range))

                df_stp2 = bitget_data_util.updating_stochrsi(data_frame=df_ws, prices_history_dict=prices_history)

                config_ma = ConfigMA(length=1, type="sma")
                df_stp2 = bitget_data_util.updating_ma(config_ma= config_ma, data_frame=df_stp2, prices_history_dict=prices_history)

                # LONG
                query = "(" + DataFrameColum.MA_ASCENDING.value + " == False)"
                query = query + " and (" + DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == False)"
                query = query + " and (" + DataFrameColum.SYMBOL.value + " == '" + symbol + "')"
                df_stp2 = df_stp2.query(query)

                if not df_stp2.empty:
                    df_stp2.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                    df_stp2.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                    df_stp2.loc[:, DataFrameColum.LEVEREAGE.value] = 10
                    df_stp2.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                    df_stp2.loc[:,DataFrameColum.NOTE.value] = "-"
                    df_stp2.loc[:,DataFrameColum.NOTE_4.value] = "Step_3 - Short BUY"

                    #Copiamos la inf en el DF bckp
                    filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_stp2)

                    return filtered_df_master

        #--------------------------------------------------------- Step_1 - Long BUY ------------------------------------------------------
                
        #Reservo las compras de waiting shorts, para completar el cirtuito
        free_slots = settings.MAX_COIN_BUY - count_waiting_shorts

        if free_slots > 0:

            for temporalidad in time_ranges:

                time_range = TimeRanges(temporalidad)
                cut_temp = int(temporalidad[-1])
                filtered_data_frame.loc[:, DataFrameColum.NOTE_5.value] = time_range.name

                # LIMIT SELL
                if time_range.name == "MINUTES_1":
                    filtered_data_frame.loc[:, DataFrameColum.NOTE_2.value] = 0.1
                elif time_range.name == "MINUTES_5":
                    filtered_data_frame.loc[:, DataFrameColum.NOTE_2.value] = 0.2
                elif time_range.name == "MINUTES_15":
                    filtered_data_frame.loc[:, DataFrameColum.NOTE_2.value] = 0.3
                elif time_range.name == "HOUR_1":
                    filtered_data_frame.loc[:, DataFrameColum.NOTE_2.value] = 0.35
                elif time_range.name == "HOUR_4":
                    filtered_data_frame.loc[:, DataFrameColum.NOTE_2.value] = 0.4

                # ----------------- CLASIFICACIÓN DE  COINS POR TIEMPO -----------------
                # Actualizamos fecha de la siguiente ejecución
                query = DataFrameColum.NOTE_3.value + " == '-'"
                filtered_data_frame.loc[filtered_data_frame.query(query).index, DataFrameColum.NOTE_3.value] = datetime.now()
                filtered_data_frame[DataFrameColum.NOTE_3.value] = pandas.to_datetime(filtered_data_frame[DataFrameColum.NOTE_3.value], format='%d-%m-%Y %H:%M:%S')
                
                #Copiamos la inf en el DF bckp
                filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = filtered_data_frame)
                filtered_data_frame = Strategy.unlocking_time_locked_crypto (data_frame = filtered_data_frame, time_column = DataFrameColum.NOTE_3.value)
                # -----------------  CLASIFICACIÓN DE  COINS POR TIEMPO  -----------------

                rules = DataFrameColum.NOTE_4.value + " == '-' or " + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY'"
                df_stp1 = filtered_data_frame.query(state_query)

                #Copiamos la inf en el DF bckp
                filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_stp1)

                if not df_stp1.empty:

                    #·············································································································································
                    #··························································  ESTRATEGIA DE GIRO ADX ··························································  
                    #·············································································································································

                    prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_stp1, time_range = time_range)
                    df_stp1 = Strategy.calculate_ao_adx_v (bitget_data_util=bitget_data_util, data_frame =  df_stp1, prices_history= prices_history)
                    df_stp1 = Strategy.calculate_percentaje_close_respect_to_the_High_of_a_candle (data_frame = df_stp1, prices_history_dict = prices_history, last_position = 1)

                    print(df_stp1[[DataFrameColum.SYMBOL.value,DataFrameColum.SIDE_TYPE.value,DataFrameColum.NOTE_5.value,'prctj_close_of_a_candle']])

                        #·············································································································································
                        #··························································  ESTRATEGIA DE GIRO ADX ··························································  
                        #·············································································································································

                    #Copiamos la inf en el DF bckp
                    filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_stp1)

                    #! Antes de hacer la compra de las coins marcadas con el giro, es conveniente ver el estado de la vela. Ver el close con respeto al hight 

                    if not df_stp1.empty: 

                        df_stp1.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                        df_stp1.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                        df_stp1.loc[:, DataFrameColum.LEVEREAGE.value] = 10
                        df_stp1.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                        df_stp1.loc[:, DataFrameColum.NOTE.value] = "-"
                        df_stp1.loc[:, DataFrameColum.NOTE_4.value] = "Step_1 - Long BUY"
                        df_stp1.loc[:, DataFrameColum.NOTE_5.value] = time_range.name
                        
                        cut_temp = int(temporalidad[-1])
                        df_stp1.loc[:, DataFrameColum.NOTE_3.value] = Strategy.get_next_period(number_hours = cut_temp)

                        #Copiamos la inf en el DF bckp
                        filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_stp1)

                        return filtered_df_master
        """
#--------------------------------------------------------- Step - Stoch_RSI ------------------------------------------------------

        return filtered_df_master
    
    @staticmethod
    def apply_sell(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)

        df[DataFrameColum.NOTE_2.value] = df[DataFrameColum.NOTE_2.value].astype(float)

        df = bitget_data_util.updating_open_orders(data_frame=df)
        df = Strategy.mark_price_exceeds_limit(data_frame = df)
        df = Strategy.mark_step_long_sell(bitget_data_util=bitget_data_util, data_frame = df)
        Strategy.print_data_frame(message = 'COMPRA', data_frame = df)

        for ind in df.index:
            # LIMIT SELL
            time_range = df.loc[ind, DataFrameColum.NOTE_5.value]
            if time_range == "MINUTES_1":
                df.loc[ind, DataFrameColum.NOTE_2.value] = 0.15
            elif time_range == "MINUTES_5":
                df.loc[ind, DataFrameColum.NOTE_2.value] = 0.2
            elif time_range == "MINUTES_15":
                df.loc[ind, DataFrameColum.NOTE_2.value] = 0.25
            elif time_range == "HOUR_1":
                df.loc[ind, DataFrameColum.NOTE_2.value] = 0.3
            elif time_range == "HOUR_4":
                df.loc[ind, DataFrameColum.NOTE_2.value] = 0.5

            if df.loc[ind, DataFrameColum.NOTE_4.value] == 'Step - Stoch_RSI':
                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                query = DataFrameColum.SYMBOL.value + " == '" + symbol + "'"
                df_only_coin = df.query(query)

                """********************************************** OPCION 0 **********************************************"""

                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_only_coin, time_range = TimeRanges("HOUR_1"))

                ascending_count = 2
                config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
                df_stch_rsi = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = df_only_coin, prices_history_dict = prices_history, ascending_count = ascending_count)

                if df.loc[ind, DataFrameColum.SIDE_TYPE.value] == 'long':
                    query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == False"
                else:
                    query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == True"
                df_stch_rsi = df_stch_rsi.query(query)

                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_stch_rsi, time_range = TimeRanges("MINUTES_15"))
                
                ascending_count = 2
                config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
                df_stch_rsi = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = df_stch_rsi, prices_history_dict = prices_history, ascending_count = ascending_count)

                if df.loc[ind, DataFrameColum.SIDE_TYPE.value] == 'long':
                    query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == False"
                else:
                    query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == True"
                df_stch_rsi = df_stch_rsi.query(query)

                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_stch_rsi, time_range = TimeRanges("MINUTES_5"))
                
                ascending_count = 2
                config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
                df_stch_rsi = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = df_stch_rsi, prices_history_dict = prices_history, ascending_count = ascending_count)

                if df.loc[ind, DataFrameColum.SIDE_TYPE.value] == 'long':
                    query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == False"
                else:
                    query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == True"
                df_op0 = df_stch_rsi.query(query)

                if df_op0.empty == False:
                    df_op0.loc[:,DataFrameColum.ORDER_ID.value] = "-"
                    df_op0.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
                    df_op0.loc[:, DataFrameColum.LOOK.value] = "-"
                    df_op0.loc[:, DataFrameColum.NOTE_4.value] = "-"        
                    df_op0.loc[:,DataFrameColum.NOTE_2.value] = 0.0
                    df_op0.loc[:, DataFrameColum.STOP_LOSS.value] = 0

                    return df_op0
            elif df.loc[ind, DataFrameColum.NOTE_4.value] == 'Step - MACD' or df.loc[ind, DataFrameColum.NOTE_4.value] == 'Step - every trend' or df.loc[ind, DataFrameColum.NOTE_4.value] == 'Step - Giro ADX':

                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = TimeRanges(df.loc[ind, DataFrameColum.NOTE_5.value]))

                ascending_count = 2
                config_macd = ConfigMACD(fast=12, slow=26, signal=9)
                df = bitget_data_util.updating_macd(config_macd = config_macd, data_frame = df, prices_history_dict = prices_history, ascending_count = ascending_count)

                """********************************************** OPCION MACD ********************************************** """

                if df.loc[ind, DataFrameColum.SIDE_TYPE.value] == 'short': #! PRUEBA INVERSA
                    query = "(MACD_GOOD_LINE.str[-1] < MACD_BAD_LINE.str[-1])"
                else:
                    query = "(MACD_GOOD_LINE.str[-1] > MACD_BAD_LINE.str[-1])"

                df_macd = df.query(query)

                if df_macd.empty == False:
                    df_macd.loc[:,DataFrameColum.ORDER_ID.value] = "-"
                    df_macd.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
                    df_macd.loc[:, DataFrameColum.LOOK.value] = "-"
                    df_macd.loc[:, DataFrameColum.NOTE_4.value] = "-"        
                    df_macd.loc[:,DataFrameColum.NOTE_2.value] = 0.0
                    df_macd.loc[:, DataFrameColum.STOP_LOSS.value] = 0

                    return df_macd   
            elif df.loc[ind, DataFrameColum.NOTE_4.value] == 'Step - Long Sell' and df.loc[ind, DataFrameColum.NOTE_4.value] != 'headdress!' :

                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = TimeRanges(df.loc[ind, DataFrameColum.NOTE_5.value]))

                ascending_count = 2
                config_macd = ConfigMACD(fast=12, slow=26, signal=9)
                df = bitget_data_util.updating_macd(config_macd = config_macd, data_frame = df, prices_history_dict = prices_history, ascending_count = ascending_count)

                ascending_count = 3 #! CONTROL
                config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
                df_slave = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = df, prices_history_dict = prices_history, ascending_count = ascending_count)
                
                if df.loc[ind, DataFrameColum.SIDE_TYPE.value] == 'short': #! PRUEBA INVERSA
                    query = "(" + DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " != '" + ColumLineValues.RED_TOP.value + "')"
                else:
                    query = "(" + DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " != '" + ColumLineValues.BLUE_TOP.value + "')"

                df_long_venta= df_slave.query(query)

                if df_long_venta.empty == False:
                    df_long_venta.loc[:,DataFrameColum.ORDER_ID.value] = "-"
                    df_long_venta.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
                    df_long_venta.loc[:, DataFrameColum.LOOK.value] = "-"
                    df_long_venta.loc[:, DataFrameColum.NOTE_4.value] = "-"        
                    df_long_venta.loc[:,DataFrameColum.NOTE_2.value] = 0.0
                    df_long_venta.loc[:, DataFrameColum.STOP_LOSS.value] = 0

                    return df_long_venta   

            """********************************************** OPCION 1 **********************************************
            Si tiene el valor headdress, es que ya ha superado el value_limit. Se vende en el momento que baja el value_limit 

            query = DataFrameColum.PERCENTAGE_PROFIT.value + " < " + DataFrameColum.NOTE_2.value + " and " + DataFrameColum.LOOK.value + " == 'headdress!'" 
            #query = query + " and (" + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY' or " + DataFrameColum.NOTE_4.value + " == 'Step - Stoch_RSI')"
            df_op1 = df.query(query)

            for ind in df_op1.index:

                df_op1.loc[ind,DataFrameColum.ORDER_ID.value] = "-"
                df_op1.loc[ind,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
                df_op1.loc[ind, DataFrameColum.LOOK.value] = "-"

                query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY')"
                df_op1.loc[df_op1.query(query).index, DataFrameColum.NOTE_4.value] = "Step_2 - Waiting Short"

                query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_3 - Short BUY')"
                df_op1.loc[df_op1.query(query).index, DataFrameColum.NOTE_4.value] = "-"

                query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step - every trend')"
                df_op1.loc[df_op1.query(query).index, DataFrameColum.NOTE_4.value] = "-"

                df_op1.loc[ind,DataFrameColum.NOTE_2.value] = 0.0
                
                time_range = df_op1.loc[ind, DataFrameColum.NOTE_5.value]
                cut_temp = int(time_range[-1])
                df_op1.loc[ind,DataFrameColum.NOTE_3.value] = Strategy.next_hour(how_hours=cut_temp)
                df_op1.loc[ind, DataFrameColum.STOP_LOSS.value] = 0

                return df_op1
            """

            """********************************************** OPCION 2 **********************************************
            Si tiene el valor headdress, es que ya ha superado el value_limit. Se le deja subir hasta que toca stoploss""" 
            
            query = "(" + DataFrameColum.PERCENTAGE_PROFIT.value + " > " + str(DataFrameColum.NOTE_2.value) + ") and (" + DataFrameColum.PERCENTAGE_PROFIT.value + " < " + DataFrameColum.STOP_LOSS.value + ")" 
            df_op2 = df.query(query)

            if df_op2.empty == False:
                df_op2.loc[ind,DataFrameColum.ORDER_ID.value] = "-"
                df_op2.loc[ind,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
                df_op2.loc[ind, DataFrameColum.LOOK.value] = "-"
                df_op2.loc[ind,DataFrameColum.NOTE_2.value] = "-"
                df_op2.loc[ind,DataFrameColum.NOTE_4.value] = "-"
                df_op2.loc[ind, DataFrameColum.STOP_LOSS.value] = 0

                return df_op2

        """********************************************** OPCION 0 **********************************************"""
        """Vender si llega al profit marcado"""
        
        """ 
        query = "(" + DataFrameColum.PERCENTAGE_PROFIT.value + " < -1.5)" 
        df_op0 = df.query(query)

        for ind in df_op0.index:
            df_op0.loc[ind,DataFrameColum.ORDER_ID.value] = "-"
            df_op0.loc[ind,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            df_op0.loc[ind, DataFrameColum.LOOK.value] = "-"
            df_op0.loc[ind,DataFrameColum.NOTE_2.value] = 0.0

            time_range = df_op0.loc[ind, DataFrameColum.NOTE_5.value]
            cut_temp = int(time_range[-1])
            df_op0.loc[ind,DataFrameColum.NOTE_3.value] = Strategy.get_next_period(number_hours=cut_temp)

            df_op0.loc[ind, DataFrameColum.NOTE_4.value] = "-"
            df_op0.loc[ind,DataFrameColum.NOTE_5.value] = "-"
            df_op0.loc[ind, DataFrameColum.STOP_LOSS.value] = 0
            
            return df_op0
        """

        """********************************************** OPCION 00 **********************************************
        for ind in df.index:

            time_range = df.loc[ind, DataFrameColum.NOTE_5.value]

            query = DataFrameColum.NOTE_5.value + " == '" + str(time_range) + "'"
            df_temp = df.query(query)

            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_temp, time_range = TimeRanges(time_range))

            config_ma = ConfigMA(length=2, type="sma")
            ascending_count = 2
            df_temp = bitget_data_util.updating_ma(config_ma= config_ma, data_frame=df_temp, prices_history_dict=prices_history, ascending_count = ascending_count)

            symbol = df_temp.loc[ind,DataFrameColum.SYMBOL.value]

            if df_temp.loc[ind,DataFrameColum.SIDE_TYPE.value] == FutureValues.SIDE_TYPE_LONG.value:
                query = "(" + DataFrameColum.MA_ASCENDING.value + " == False) and (" + DataFrameColum.SYMBOL.value + " == '" + symbol + "')"
                df_op00 = df_temp.query(query)
            else:
                query = "(" + DataFrameColum.MA_ASCENDING.value + " == True) and (" + DataFrameColum.SYMBOL.value + " == '" + symbol + "')"
                df_op00 = df_temp.query(query)

            if df_op00.empty == False:
                df_op00.loc[:,DataFrameColum.ORDER_ID.value] = "-"
                df_op00.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
                df_op00.loc[:, DataFrameColum.LOOK.value] = "-"

                query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY')"
                df_op00.loc[df_op00.query(query).index, DataFrameColum.NOTE_4.value] = "Step_2 - Waiting Short"

                query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_3 - Short BUY')"
                df_op00.loc[df_op00.query(query).index, DataFrameColum.NOTE_4.value] = "-"

                df_op00.loc[:,DataFrameColum.NOTE_2.value] = 0.0
                df_op00.loc[:,DataFrameColum.NOTE_3.value] = "-"
                df_op00.loc[ind, DataFrameColum.STOP_LOSS.value] = 0

                return df_op00
        """

        """********************************************** OPCION 1 **********************************************
        Si tiene el valor headdress, es que ya ha superado el value_limit. Se vende en el momento que baja el value_limit 

        query = DataFrameColum.PERCENTAGE_PROFIT.value + " < " + DataFrameColum.NOTE_2.value + " and " + DataFrameColum.LOOK.value + " == 'headdress!'" 
        #query = query + " and (" + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY' or " + DataFrameColum.NOTE_4.value + " == 'Step - Stoch_RSI')"
        df_op1 = df.query(query)

        for ind in df_op1.index:

            df_op1.loc[ind,DataFrameColum.ORDER_ID.value] = "-"
            df_op1.loc[ind,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            df_op1.loc[ind, DataFrameColum.LOOK.value] = "-"

            query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY')"
            df_op1.loc[df_op1.query(query).index, DataFrameColum.NOTE_4.value] = "Step_2 - Waiting Short"

            query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_3 - Short BUY')"
            df_op1.loc[df_op1.query(query).index, DataFrameColum.NOTE_4.value] = "-"

            query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step - Stoch_RSI')"
            df_op1.loc[df_op1.query(query).index, DataFrameColum.NOTE_4.value] = "-"

            df_op1.loc[ind,DataFrameColum.NOTE_2.value] = 0.0
            
            time_range = df_op1.loc[ind, DataFrameColum.NOTE_5.value]
            cut_temp = int(time_range[-1])
            df_op1.loc[ind,DataFrameColum.NOTE_3.value] = Strategy.next_hour(how_hours=cut_temp)
            df_op1.loc[ind, DataFrameColum.STOP_LOSS.value] = 0

            return df_op1
        """

        """********************************************** OPCION 2 **********************************************
        Si tiene el valor headdress, es que ya ha superado el value_limit. Se le deja subir hasta que toca stoploss 
        
        query = "(" + DataFrameColum.PERCENTAGE_PROFIT.value + " > " + str(DataFrameColum.NOTE_2.value) + ") and (" + DataFrameColum.PERCENTAGE_PROFIT.value + " < " + DataFrameColum.STOP_LOSS.value + ")" 
        query = query + " and " + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY'"
        df_op2 = df.query(query)

        for ind in df_op2.index:
            df_op2.loc[ind,DataFrameColum.ORDER_ID.value] = "-"
            df_op2.loc[ind,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            df_op2.loc[ind, DataFrameColum.LOOK.value] = "-"

            query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_1 - Long BUY')"
            df_op2.loc[df_op2.query(query).index, DataFrameColum.NOTE_4.value] = "Step_2 - Waiting Short"

            query =  "(" + DataFrameColum.NOTE_4.value + " == 'Step_3 - Short BUY')"
            df_op2.loc[df_op2.query(query).index, DataFrameColum.NOTE_4.value] = "-"

            df_op2.loc[ind,DataFrameColum.NOTE_2.value] = "-"

            time_range = df_op2.loc[ind, DataFrameColum.NOTE_5.value]
            cut_temp = int(time_range[-1])
            df_op2.loc[ind,DataFrameColum.NOTE_3.value] = Strategy.next_hour(how_hours=cut_temp)
            df_op2.loc[ind, DataFrameColum.STOP_LOSS.value] = 0

            return df_op2
        """
    
        for ind in df.index:
            time_range = df.loc[ind, DataFrameColum.NOTE_5.value]
            
            if time_range == "MINUTES_1":
                limit_sl = 0.05
            elif time_range == "MINUTES_5":
                limit_sl = 0.075
            elif time_range == "MINUTES_15":
                limit_sl = 0.1
            elif time_range == "HOUR_1":
                limit_sl = 0.5
            elif time_range == "HOUR_4":
                limit_sl = 0.9

            cntrl_profit_crrnt = float(df.loc[ind,DataFrameColum.PERCENTAGE_PROFIT.value]) - limit_sl

            if float(df.loc[ind,DataFrameColum.STOP_LOSS.value]) < cntrl_profit_crrnt:
                df.loc[ind,DataFrameColum.STOP_LOSS.value] = cntrl_profit_crrnt
        
        return df

    def apply_buy_long_short_macd_ma_stochrsi(self, bitget_data_util: BitgetDataUtil,  df_slave: pandas.DataFrame, prices_history:dict=None, time_range:TimeRanges=None) -> pandas.DataFrame:

        ascending_count = 2
        config_macd = ConfigMACD(fast=12, slow=26, signal=9)
        df_slave = bitget_data_util.updating_macd(config_macd = config_macd, data_frame = df_slave, prices_history_dict = prices_history, ascending_count = ascending_count)

        config_ma_50 = ConfigMA(length=50, type="sma")
        df_ma = bitget_data_util.updating_ma(config_ma= config_ma_50, data_frame=df_slave, prices_history_dict=prices_history)

        df_ma.loc[:,self.ma_50_colum] = df_ma[DataFrameColum.MA_LAST.value]
        df_ma.loc[:,self.ma_50_ascending_colum] = df_ma[DataFrameColum.MA_ASCENDING.value]
        df_ma.loc[:,self.ma_50_angle_colum] = df_ma[DataFrameColum.MA_LAST_ANGLE.value]
        
        config_ma_100 = ConfigMA(length=100, type="sma")
        df_ma = bitget_data_util.updating_ma(config_ma= config_ma_100, data_frame=df_ma, prices_history_dict=prices_history)
        
        df_ma.loc[:,self.ma_100_colum] = df_ma[DataFrameColum.MA_LAST.value]
        df_ma.loc[:,self.ma_100_ascending_colum] = df_ma[DataFrameColum.MA_ASCENDING.value]
        
        ascending_count = 3 #! CONTROL
        config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
        df_slave = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = df_ma, prices_history_dict = prices_history, ascending_count = ascending_count)
        
        query = DataFrameColum.MA_ASCENDING.value + " == True"
        df_asc = df_slave.query(query)

        if not df_asc.empty: 
            #MACD
            query = "(MACD_GOOD_LINE.str[-3] < MACD_BAD_LINE.str[-3]) and (MACD_GOOD_LINE.str[-2] > MACD_BAD_LINE.str[-2]) and (MACD_GOOD_LINE.str[-1] > MACD_BAD_LINE.str[-1])"
            query = query + " and (" + DataFrameColum.MACD_LAST.value + " > 0)"

            df_asc= df_asc.query(query)

            #STOCHRSI
            query = query + " and ((" + DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_TOP.value +"') or (" +DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_TOP.value + "'))"
            #query = query + " and (" + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " < 50)" #! Original
            query = query + " and (" + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " > 70)" #! PRUEBA INVERSA

            df_asc= df_asc.query(query)
            
            #MA
            query = query + " and (" + self.ma_50_colum + " > " + self.ma_100_colum + ")"
            #query = query + " and (MA_50.str[-2] < MA_50.str[-1]"

            df_long= df_asc.query(query)

        
        query = DataFrameColum.MA_ASCENDING.value + " == False"
        df_des = df_slave.query(query)

        if not df_des.empty: 
            #MACD
            query = "(MACD_GOOD_LINE.str[-3] > MACD_BAD_LINE.str[-3]) and (MACD_GOOD_LINE.str[-2] < MACD_BAD_LINE.str[-2]) and (MACD_GOOD_LINE.str[-1] < MACD_BAD_LINE.str[-1])"
            query = query + " and (" + DataFrameColum.MACD_LAST.value + " < 0)"

            df_des= df_des.query(query)

            #STOCHRSI
            query = query + " and ((" + DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_DOWN.value +"') or (" +DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.RED_TOP.value + "'))"
            #query = query + " and (" + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " > 50)" #! Original
            query = query + " and (" + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " < 30)" #! PRUEBA INVERSA

            df_des= df_des.query(query)
            
            #MA
            query = query + " and (" + self.ma_50_colum + " < " + self.ma_100_colum + ")"
            #query = query + " and (MA_50.str[-2] > MA_50.str[-1]"

            df_short = df_des.query(query)

        if not df_long.empty or not df_short.empty:
            list_df = [df_long, df_short]
            df_buy = pandas.concat(list_df)
        else:
            df_buy = pandas.DataFrame()

        if not df_buy.empty: 

            df_buy.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False

            for ind in df_buy.index:
                if df_buy.loc[ind, self.ma_50_ascending_colum] == False: #! PRUEBA INVERSA
                    df_buy.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                else:
                    df_buy.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value

            df_buy.loc[:, DataFrameColum.LEVEREAGE.value] = 20
            df_buy.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df_buy.loc[:, DataFrameColum.NOTE.value] = "-"
            df_buy.loc[:, DataFrameColum.NOTE_3.value] = Strategy.get_next_period(number_hours = 1)
            df_buy.loc[:, DataFrameColum.NOTE_4.value] = "Step - every trend"
            df_buy.loc[:, DataFrameColum.NOTE_5.value] = time_range.name

            
        return df_buy

    def apply_buy_long_short_stochrsi(self, bitget_data_util: BitgetDataUtil,df_slave: pandas.DataFrame, is_long:bool, prices_history:dict=None, time_range:TimeRanges=None) -> pandas.DataFrame:

        config_ma_50 = ConfigMA(length=50, type="sma")
        df_ma = bitget_data_util.updating_ma(config_ma= config_ma_50, data_frame=df_slave, prices_history_dict=prices_history)

        df_ma.loc[:,self.ma_50_colum] = df_ma[DataFrameColum.MA_LAST.value]
        df_ma.loc[:,self.ma_50_ascending_colum] = df_ma[DataFrameColum.MA_ASCENDING.value]
        df_ma.loc[:,self.ma_50_angle_colum] = df_ma[DataFrameColum.MA_LAST_ANGLE.value]
        
        config_ma_100 = ConfigMA(length=100, type="sma")
        df_ma = bitget_data_util.updating_ma(config_ma= config_ma_100, data_frame=df_ma, prices_history_dict=prices_history)
        
        df_ma.loc[:,self.ma_100_colum] = df_ma[DataFrameColum.MA_LAST.value]
        df_ma.loc[:,self.ma_100_ascending_colum] = df_ma[DataFrameColum.MA_ASCENDING.value]

        config_ma_150 = ConfigMA(length=150, type="sma")
        df_ma = bitget_data_util.updating_ma(config_ma= config_ma_150, data_frame=df_ma, prices_history_dict=prices_history)
                
        df_ma.loc[:,self.ma_150_colum] = df_ma[DataFrameColum.MA_LAST.value]
        df_ma.loc[:,self.ma_150_ascending_colum] = df_ma[DataFrameColum.MA_ASCENDING.value]
        
        if is_long:
            query = self.ma_50_colum + " > " + self.ma_100_colum 
            #query = self.ma_50_colum + " > " + self.ma_100_colum + " and " + self.ma_100_colum + " > " + self.ma_150_colum 
        else:
            query = self.ma_50_colum + " < " + self.ma_100_colum 

        filtered_data_frame = df_ma.query(query)
        
        ascending_count = 2 #! Cambiado
        config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
        df_stch_rsi = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = filtered_data_frame, 
        prices_history_dict = prices_history, ascending_count = ascending_count,)

        if is_long:
            query = DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == 'BLUE_CRUCE_TOP' and (" + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " < 50 and " + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " > 20)"
        else:
            query = DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == 'BLUE_CRUCE_DOWN' and (" + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " > 50 and " + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " < 80)"
            
        df_stch_rsi = df_stch_rsi.query(query)

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_stch_rsi, time_range = TimeRanges("MINUTES_15"))
        
        ascending_count = 2
        config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
        df_stch_rsi = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = df_stch_rsi, 
        prices_history_dict = prices_history, ascending_count = ascending_count)

        if is_long:
            query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == True and " + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " < 30"
        else:
            query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == False and " + DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value + " > 70"

        df_stch_rsi = df_stch_rsi.query(query)

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_stch_rsi, time_range = TimeRanges("MINUTES_5"))
        
        ascending_count = 2
        config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
        df_stch_rsi = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = df_stch_rsi, 
        prices_history_dict = prices_history, ascending_count = ascending_count)

        if is_long:
            query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == True" 
        else:
            query = DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value + " == False" 
        df_stch_rsi = df_stch_rsi.query(query)
        
        if not df_stch_rsi.empty: 

            df_stch_rsi.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
            if is_long:
                df_stch_rsi.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
            else:
                df_stch_rsi.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            df_stch_rsi.loc[:, DataFrameColum.LEVEREAGE.value] = 10
            df_stch_rsi.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df_stch_rsi.loc[:, DataFrameColum.NOTE.value] = "-"
            df_stch_rsi.loc[:, DataFrameColum.NOTE_4.value] = "Step - Stoch_RSI"
            df_stch_rsi.loc[:, DataFrameColum.NOTE_5.value] = 'HOUR_1'
            df_stch_rsi.loc[:, DataFrameColum.NOTE_2.value] = 0.3
            
        return df_stch_rsi

    def apply_buy_long_giro_adx(self, bitget_data_util: BitgetDataUtil, df_slave: pandas.DataFrame, lista_coins:list) -> pandas.DataFrame:
    
        # Crear la consulta dinámica
        query = df_slave[DataFrameColum.SYMBOL.value].isin(lista_coins)
        df_btc = df_slave[query]

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_btc, time_range = TimeRanges("HOUR_4"))
        df_stp1 = Strategy.calculate_ao_adx_v (bitget_data_util=bitget_data_util, data_frame =  df_btc, prices_history= prices_history)
        df_stp1 = Strategy.calculate_percentaje_close_respect_to_the_High_of_a_candle (data_frame = df_stp1, prices_history_dict = prices_history, last_position = 1)

        #! Antes de hacer la compra de las coins marcadas con el giro, es conveniente ver el estado de la vela. Ver el close con respeto al hight 

        if not df_stp1.empty: 
            df_stp1.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
            df_stp1.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
            df_stp1.loc[:, DataFrameColum.LEVEREAGE.value] = 10
            df_stp1.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df_stp1.loc[:, DataFrameColum.NOTE.value] = "-"
            df_stp1.loc[:, DataFrameColum.NOTE_3.value] = Strategy.get_next_period(number_hours = 4)
            df_stp1.loc[:, DataFrameColum.NOTE_4.value] = "Step - Giro ADX"
            df_stp1.loc[:, DataFrameColum.NOTE_5.value] = TimeRanges("HOUR_4")
            
            #cut_temp = int(temporalidad[-1])
            #df_stp1.loc[:, DataFrameColum.NOTE_3.value] = Strategy.get_next_period(number_hours = cut_temp)

            #Copiamos la inf en el DF bckp
            #filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_stp1)
        return df_stp1



#----------------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def calculate_percentaje_close_respect_to_the_High_of_a_candle (data_frame: pandas.DataFrame, prices_history_dict:dict=None, last_position:int=1):
        #percentage of close with respect to the High of a Japanese candle

        if 'prctj_close_of_a_candle' not in data_frame.columns:
            data_frame['prctj_close_of_a_candle'] = None

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            prices_history = prices_history_dict[symbol]
            
            prices_high = prices_history['High'].astype(float)[:-last_position]
            prices_low = prices_history['Low'].astype(float)[:-last_position]
            prices_close = prices_history['Close'].astype(float)[:-last_position]

            data_frame['prctj_close_of_a_candle'] = ((prices_close - prices_high) / prices_high) * 100
        
        return data_frame

    @staticmethod
    def mark_step_long_sell(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        for ind in data_frame.index:
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = data_frame, time_range = TimeRanges(data_frame.loc[ind, DataFrameColum.NOTE_5.value]))

            ascending_count = 2
            config_macd = ConfigMACD(fast=12, slow=26, signal=9)
            data_frame = bitget_data_util.updating_macd(config_macd = config_macd, data_frame = data_frame, prices_history_dict = prices_history, ascending_count = ascending_count)

            ascending_count = 2 #! CONTROL
            config_stoch_rsi = ConfigSTOCHrsi(longitud_stoch= 14, longitud_rsi= 14, smooth_k = 4, smooth_d = 3)
            df_slave = bitget_data_util.updating_stochrsi(config_stoch_rsi= config_stoch_rsi, data_frame = data_frame, prices_history_dict = prices_history, ascending_count = ascending_count)
            
            if df_slave.loc[ind, DataFrameColum.SIDE_TYPE.value] == 'short': #! PRUEBA INVERSA
                query = "(" +DataFrameColum.MACD_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_TOP.value + "')"
                query = query + " and ((" +DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.RED_TOP.value + "') or (" +DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_TOP.value + "'))"
            else:
                query = "(" +DataFrameColum.MACD_CRUCE_LINE.value + " == '" + ColumLineValues.RED_TOP.value + "')"
                query = query + " and ((" +DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_TOP.value + "') or (" +DataFrameColum.RSI_STOCH_CRUCE_LINE.value + " == '" + ColumLineValues.BLUE_CRUCE_DOWN.value + "'))"

            df_long_venta= df_slave.query(query)

            if df_long_venta.empty == False:
                df_long_venta.loc[:, DataFrameColum.NOTE_4.value] == 'Step - Long Sell'
        
        data_frame = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = data_frame, df_slave = df_long_venta)
        return data_frame

    @staticmethod
    def mark_price_exceeds_limit(data_frame: pandas.DataFrame) -> pandas.DataFrame:
        """Marcamos las cons con el profit superior al limite marcado"""
        data_frame.loc[data_frame[DataFrameColum.PERCENTAGE_PROFIT.value] >= data_frame[DataFrameColum.NOTE_2.value], DataFrameColum.LOOK.value] = 'headdress!'
        return data_frame

    @staticmethod
    def calculate_ao_adx_v (bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame, prices_history:dict=None, positions_back: float = 0):
        
        ######## INICIALIZACIION ##########
        """
        NOTE = time_range que vamos a ejecutar 
        NOTE_2 = Se guarda 'OK' Si cumple el objetivo
        NOTE_3 = GUardamos hasta que hora se deja la ventana de ejecuión
        """

        # AO
        ascending_count = 2
        data_frame = bitget_data_util.updating_ao(data_frame = data_frame, prices_history_dict = prices_history, ascending_count = ascending_count)

        query = DataFrameColum.AO_ASCENDING.value + " == True and " + DataFrameColum.AO_LAST.value + " > 0"
        data_frame = data_frame.query(query)

        if data_frame.empty: return data_frame

        #ADX
        adx_ascending_count = 2
        config_adx = ConfigADX(series= 14)
        data_frame = bitget_data_util.updating_adx(config_adx=config_adx, data_frame = data_frame, prices_history_dict = prices_history, ascending_count = adx_ascending_count)

        query = DataFrameColum.ADX_LAST.value + " > 25"
        data_frame = data_frame.query(query)

        data_frame = Strategy.seach_v (filtered_data_frame = data_frame, positions_back = positions_back)
                
        return data_frame

    @staticmethod
    def seach_v (filtered_data_frame: pandas.DataFrame, positions_back:float = 0):

        filtered_data_frame['SUM_ANGLES'] = None

        for ind in filtered_data_frame.index:
            list_adx = filtered_data_frame.loc[ind, DataFrameColum.ADX.value]

            pos_adx_3 = list_adx[-3 + positions_back]
            pos_adx_2 = list_adx[-2 + positions_back]
            pos_adx_1 = list_adx[-1 + positions_back]

            if (pos_adx_3 > pos_adx_2) and (pos_adx_2 < pos_adx_1): #buscando pico "v"

                list_data_prev = [pos_adx_3, pos_adx_2]
                angle_prev = float(Strategy.calculate_angle(list_data_prev, int_eje_x = 1))

                list_data_crrnt = [pos_adx_2, pos_adx_1]
                angle_crrnt = float(Strategy.calculate_angle(list_data_crrnt, int_eje_x = 1))

                sum_angles = 180 - (abs(angle_prev) + abs(angle_crrnt))

                print ("ANGLE (" + str(filtered_data_frame.loc[ind, DataFrameColum.NOTE_5.value]) + ") de : " + str(filtered_data_frame.loc[ind, DataFrameColum.SYMBOL.value]) + " es "+ str(sum_angles))

                filtered_data_frame.loc[ind, 'SUM_ANGLES'] = sum_angles

        query = "SUM_ANGLES < 110"
        filtered_data_frame = filtered_data_frame.query(query)

        return filtered_data_frame

    @staticmethod
    def calculate_angle(list_values, int_eje_x):

        # BTC: int_eje_x = 100 para 1h ; BTC: int_eje_x = 3 para 5m
        # Crear un array de valores X con números enteros consecutivos
        X = numpy.arange(0, len(list_values)*int_eje_x, int_eje_x)

        # Ajustar una línea recta a los datos de precios
        coeffs = numpy.polyfit(X, list_values, 1)

        # Obtener la pendiente de la línea recta
        slope = coeffs[0]

        # Calcular el ángulo de la pendiente en grados
        angle = numpy.degrees(numpy.arctan(slope))

        return angle
    
    @staticmethod
    def unlocking_time_locked_crypto (data_frame: pandas.DataFrame, time_column : str):
        """
        La función utiliza la biblioteca datetime de Python para obtener la hora y fecha actuales con datetime.now(). 
        Luego, crea una máscara booleana mask que contiene True para todas las filas donde el valor en la columna de tiempo es anterior a la hora y fecha actuales.

        Por último, la función devuelve las filas del DataFrame que cumplen con la máscara booleana utilizando el método loc de Pandas.
        """
        filtered_rows = data_frame.loc[data_frame[time_column] < datetime.now()]
                
        return filtered_rows
 
    @staticmethod
    def next_hour(how_hours:int = 1):
        now = datetime.now()
        next_hour = now + timedelta(hours=how_hours)
        return next_hour.replace(minute=0, second=0, microsecond=0)

    @staticmethod
    def get_next_period(number_hours):
        # Obtener la hora actual
        now = datetime.now()
        start_time = now.replace(hour=1, minute=0, second=0, microsecond=0)
        time_difference = now - start_time
        tiempo_restante = timedelta(hours=number_hours) - (time_difference % timedelta(hours=number_hours))
        next_period = now + tiempo_restante

        return next_period.strftime('%d-%m-%Y %H:%M:%S')

    # Ejecuta el ciclo principal del programa
    @staticmethod
    def print_data_frame(message: str, data_frame: pandas.DataFrame):

        if data_frame.empty == False:
            print("#####################################################################################################################")
            print(message)
            print(
                data_frame[[DataFrameColum.SYMBOL.value,
                            DataFrameColum.SIDE_TYPE.value,
                            DataFrameColum.PERCENTAGE_PROFIT.value,
                            DataFrameColum.STOP_LOSS.value,
                            DataFrameColum.NOTE_2.value, 
                            DataFrameColum.NOTE_5.value,
                            DataFrameColum.NOTE_4.value
                            ]])
            print("#####################################################################################################################")
        else:
            print("#####################################################################################################################")
            print(message + " SIN DATOS")
            print("#####################################################################################################################")
