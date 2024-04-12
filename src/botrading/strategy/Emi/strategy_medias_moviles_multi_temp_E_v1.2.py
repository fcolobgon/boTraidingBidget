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
from src.botrading.utils.enums.future_values import FutureValues
from src.botrading.telegram.telegram_notify import TelegramNotify
from src.botrading.utils.price_util import PriceUtil
from src.botrading.utils import excel_util
from src.botrading.utils import traiding_operations

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
    ma_200_colum = "MA_150"
    ma_200_ascending_colum = "MA_150_ASCENDING"
    diferencia_percentual_column = "DIFERENCIA_PERCENTUAL"
    parallel_limit_column = "PARALLEL_LIMIT"
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
        self.time_range_colum = "TIME_RANGE"
        self.diferencia_percentual_column = "DIFERENCIA_PERCENTUAL"
        self.parallel_limit_column = "PARALLEL_LIMIT"
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        
    def get_time_ranges(self) -> []:
        return [ "HOUR_1", "HOUR_4"] #, "MINUTES_15", "HOUR_1", "HOUR_4"


    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL, ColumStateValues.ERR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        if self.first_iteration and settings.LOAD_FROM_PREVIOUS_EXECUTION == False:
            
            df = DataFrameCheckUtil.add_columns_to_dataframe(
                column_names=[self.step_counter,
                            self.ma_50_colum,
                            self.ma_50_ascending_colum,
                            self.ma_100_colum,
                            self.ma_100_ascending_colum,
                            self.ma_150_colum,
                            self.ma_150_ascending_colum,
                            self.time_range_colum,
                            self.diferencia_percentual_column,
                            self.parallel_limit_column], 
                            df=df)
            
            list_time_range = self.get_time_ranges()
            #df = pandas.concat([df.assign(TIME_RANGE=time_range) for time_range in nuevos_valores_tipo])

            if settings.LOAD_FROM_PREVIOUS_EXECUTION == False:
                df = Strategy.concatenates_copies_of_dataframes_at_different_temporalities(df, list_time_range)

            query = self.time_range_colum + " == 'MINUTES_1'"
            df.loc[df.query(query).index, self.parallel_limit_column] = 0.7

            query = self.time_range_colum + " == 'MINUTES_5'"
            df.loc[df.query(query).index, self.parallel_limit_column] = 1.5

            query = self.time_range_colum + " == 'MINUTES_15'"
            df.loc[df.query(query).index, self.parallel_limit_column] = 2

            query = self.time_range_colum + " == 'HOUR_1'"
            df.loc[df.query(query).index, self.parallel_limit_column] = 2.5

            query = self.time_range_colum + " == 'HOUR_4'"
            df.loc[df.query(query).index, self.parallel_limit_column] = 3

            df[self.step_counter] = 0
            self.first_iteration = False

            return df
        
        #! TEST
        #return self.return_for_buy_test(bitget_data_util=bitget_data_util, df=df)
        
        time_ranges = self.get_time_ranges()
        
        for t in time_ranges:
            
            time_range = TimeRanges.get_time_range_by_name(t)
            df_t = df.loc[df[self.time_range_colum] == t]

            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_t, time_range = time_range)
            df_t = Strategy.save_close_and_open_in_dataframe (df = df_t, currency_data_dictionary = prices_history)
            df_t = Strategy.diferencia_porcentual(df = df_t, name_column_values = DataFrameColum.NOTE.value, name_column_save = self.diferencia_percentual_column, number_of_samples = 50)

            config_ma_50 = ConfigMA(length=50, type="sma")
            df_t = bitget_data_util.updating_ma(config_ma= config_ma_50, data_frame=df_t, prices_history_dict=prices_history)

            df_t.loc[:,self.ma_50_colum] = df_t[DataFrameColum.MA_LAST.value]
            df_t.loc[:,self.ma_50_ascending_colum] = df_t[DataFrameColum.MA_ASCENDING.value]
            
            config_ma_100 = ConfigMA(length=100, type="sma")
            df_t = bitget_data_util.updating_ma(config_ma= config_ma_100, data_frame=df_t, prices_history_dict=prices_history)
            
            df_t.loc[:,self.ma_100_colum] = df_t[DataFrameColum.MA_LAST.value]
            df_t.loc[:,self.ma_100_ascending_colum] = df_t[DataFrameColum.MA_ASCENDING.value]
            
            config_ma_150 = ConfigMA(length=150, type="sma")
            df_t = bitget_data_util.updating_ma(config_ma= config_ma_150, data_frame=df_t, prices_history_dict=prices_history)
                    
            df_t.loc[:,self.ma_150_colum] = df_t[DataFrameColum.MA_LAST.value]
            df_t.loc[:,self.ma_150_ascending_colum] = df_t[DataFrameColum.MA_ASCENDING.value]

            df = DataFrameUtil.replace_rows_df_backup_with_df_for_index(df, df_t)
            
        df_big_data = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history)

        df_big_data = df_big_data.sort_values(by=self.step_counter, ascending=False)
        self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)

        # LONG
        query = self.ma_50_colum + " > " + self.ma_100_colum + " and " + self.ma_100_colum + " > " + self.ma_150_colum + " and " + DataFrameColum.PRICE_CLOSE.value + " > " + self.ma_50_colum + " and " + self.step_counter + " <= 0" + " and " + self.ma_50_ascending_colum + " == True"
        df_big_data.loc[df_big_data.query(query).index, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 1

        # SHORT
        query = self.ma_50_colum + " < " + self.ma_100_colum + " and " + self.ma_100_colum + " < " + self.ma_150_colum + " and " + DataFrameColum.PRICE_CLOSE.value + " < " + self.ma_50_colum  + " and " + self.step_counter + " <= 0" + " and " + self.ma_50_ascending_colum + " == False"
        df_big_data.loc[df_big_data.query(query).index, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 2

        # SI NO CUMPLE LAS 2 QUERYS ANTERIORES RESETEAMOS CAMPOS
        query =  " not (" + self.ma_50_colum + " > " + self.ma_100_colum + " and " + self.ma_100_colum + " > " + self.ma_150_colum + ")"
        query = query + " and not (" + self.ma_50_colum + " < " + self.ma_100_colum + " and " + self.ma_100_colum + " < " + self.ma_150_colum + ")"
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 0
        df_big_data.loc[df_big_data.query(query).index, DataFrameColum.SIDE_TYPE.value] = "-"
        df_big_data.loc[df_big_data.query(query).index, DataFrameColum.NOTE_5.value] = str(datetime.now()) + " - Resetar"

        # LONG - PASO 2
        query = "(" + self.step_counter + " == 1) and (" + DataFrameColum.PRICE_CLOSE.value + " > " + self.ma_100_colum + ") and (" + DataFrameColum.PRICE_CLOSE.value + " < " + self.ma_50_colum + ")"
        query = query + " and (NOTE.str[-2] < " + self.ma_50_colum + ")" #! NOTE = Lista de close y cojo el penultimo
        query = query + " and (NOTE_2.str[-2] < " + self.ma_50_colum + ")" #! NOTE = Lista de open y cojo el penultimo
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 3

        self.print_data_frame(message="INICIO COMPRA", data_frame=df_big_data)

        # SHORT - PASO 2
        query = "(" + self.step_counter + " == 2) and (" + DataFrameColum.PRICE_CLOSE.value + " < " + self.ma_100_colum + ") and (" + DataFrameColum.PRICE_CLOSE.value + " > " + self.ma_50_colum + ")"
        query = query + " and (NOTE.str[-2] > " + self.ma_50_colum + ")" #! NOTE = Lista de close y cojo el penultimo
        query = query + " and (NOTE_2.str[-2] > " + self.ma_50_colum + ")" #! NOTE = Lista de open y cojo el penultimo
        df_big_data.loc[df_big_data.query(query).index, self.step_counter] = 4

        self.print_data_frame(message="INICIO COMPRA", data_frame=df_big_data)

        # Obtener valores únicos de la columna 'columna_deseada'
        list_time_range = df_big_data[self.time_range_colum].unique()

        for time_range_name in list_time_range:

            time_range = TimeRanges.get_time_range_by_name(time_range_name)

            query = self.time_range_colum + " == '" + time_range_name + "'"
            df_time = df_big_data.query(query)

            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df_time, time_range = time_range)
            df_time = bitget_data_util.updating_price_indicators(data_frame=df_time, prices_history_dict=prices_history, previous_period=1) #! PREVIOUS

            # LONG - PASO COMPRA
            query = "(" + self.step_counter + " == 3) and (" + DataFrameColum.PRICE_CLOSE.value + " > " + self.ma_50_colum + ") and (" + self.diferencia_percentual_column + " > " + self.parallel_limit_column + ")" + " and " + self.ma_50_ascending_colum + " == True"
            df_stp_3 = df_time.query(query)

            if df_stp_3.empty == False:
                TelegramNotify.notify_buy(settings=settings, dataframe=df_stp_3)

                for ind in df_stp_3.index:
                    df_stp_3.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                    df_stp_3.loc[ind, DataFrameColum.LEVEREAGE.value] = 15
                    df_stp_3.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                    df_stp_3.loc[ind, self.step_counter] = 0

                    """
                    symbol = df_stp_3.loc[ind, DataFrameColum.SYMBOL.value]

                    query = "(" + DataFrameColum.SYMBOL.value + " == " + symbol + ") and (" + DataFrameColum.STATE.value + " != " + ColumStateValues.READY_FOR_BUY.value + ")"
                    df_big_data.loc[df_big_data.query(query).index, DataFrameColum.STATE.value] = ColumStateValues.BLOCKED.value
                    """
                    
                # Verificar columnas diferentes
                columnas_nuevas = set(df_stp_3.columns) - set(df_big_data)

                # Añadir columnas nuevas al DataFrame master basándonos en el índice
                self.data_frame = pandas.concat([df_big_data, df_stp_3[columnas_nuevas]], axis=1)
                DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = df_big_data, df_slave = df_stp_3)

            # SHORT - PASO COMPRA
            query = "(" + self.step_counter + " == 4) and (" + DataFrameColum.PRICE_CLOSE.value + " < " + self.ma_50_colum + ") and (" + self.diferencia_percentual_column + " > " + self.parallel_limit_column + ")" + " and " + self.ma_50_ascending_colum + " == False"
            df_stp_4 = df_time.query(query)

            if df_stp_4.empty == False:
                TelegramNotify.notify_buy(settings=settings, dataframe=df_stp_4)
                
                for ind in df_stp_4.index:
                    df_stp_4.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
                    df_stp_4.loc[ind, DataFrameColum.LEVEREAGE.value] = 15
                    df_stp_4.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
                    df_stp_4.loc[ind, self.step_counter] = 0

                    """
                    symbol = df_stp_4.loc[ind, DataFrameColum.SYMBOL.value]
                                        
                    query = "(" + DataFrameColum.SYMBOL.value + " == " + symbol + ") and (" + DataFrameColum.STATE.value + " != " + ColumStateValues.READY_FOR_BUY.value + ")"
                    df_big_data.loc[df_big_data.query(query).index, DataFrameColum.STATE.value] = ColumStateValues.BLOCKED.value
                    """

                # Verificar columnas diferentes
                columnas_nuevas = set(df_stp_4.columns) - set(df_big_data)

                # Añadir columnas nuevas al DataFrame master basándonos en el índice
                self.data_frame = pandas.concat([df_big_data, df_stp_4[columnas_nuevas]], axis=1)

                DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = df_big_data, df_slave = df_stp_4)
            
                
        return df_big_data
    

    def apply_sell(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)

        if df.empty:
            return pandas.DataFrame()
        else:
            df =  bitget_data_util.updating_open_orders(data_frame=df, startTime=self.startTime)


            self.print_data_frame(message="VENTA ", data_frame=df)

            value_limit = 1
            Strategy.mark_price_exceeds_limit(data_frame = df, value_limit= value_limit)

            """********************************************** OPCION 1 **********************************************
            Si tiene el valor headdress, es que ya ha superado el value_limit. Se vende en el momento que baja el value_limit """

            query = "((" + DataFrameColum.PERCENTAGE_PROFIT.value + " < " + str(value_limit) + ") and (" + DataFrameColum.LOOK.value + " == 'headdress!'))" 
            df_op1 = df.query(query)

            if df_op1.empty == False:
                df_op1.loc[:, DataFrameColum.LOOK.value] = "-"
                df_op1[self.step_counter] = 0

                return Strategy.return_for_sell(data_frame=df_op1)
            
            """********************************************** OPCION 2 **********************************************"""
            query = "((" + DataFrameColum.PERCENTAGE_PROFIT.value + " > " + value_limit + ") and (" + DataFrameColum.PERCENTAGE_PROFIT.value + " < " + DataFrameColum.STOP_LOSS.value + "))"  #! Es muy buena 
            df_op2 = df.query(query)

            if df_op2.empty == False:
                df_op2.loc[:, DataFrameColum.LOOK.value] = "-"
                df_op2[self.step_counter] = 0
                
                return Strategy.return_for_sell(data_frame=df_op2)


            """********************************************** OPCION 3 **********************************************"""
            for ind in df.index:
                time_range = df.loc[ind,self.time_range_colum]
                time_range = TimeRanges(time_range)
                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
                
                config_ma_100 = ConfigMA(length=100, type="sma")
                df = bitget_data_util.updating_ma(config_ma= config_ma_100, data_frame=df, prices_history_dict=prices_history)

                symbol = data_frame.loc[ind,DataFrameColum.SYMBOL.value]
                prices_history = prices_history[symbol]

                price_coin = prices_history['Close'].astype(float).iloc[-1]

            query = "(" + str(price_coin) + " < " + DataFrameColum.MA_LAST.value + ")" 
            df_op3 = df.query(query)

            if df_op3.empty == False:
                df_op3.loc[:, DataFrameColum.LOOK.value] = "-"
                df_op3[self.step_counter] = 0
                
                return Strategy.return_for_sell(data_frame=df_op3)

            """********************************************** OPCION 4 **********************************************
            Solo vendemos cuando el precio está por debajo del ma50_last y el ma50_asc es false"""
            for ind in df.index:
                time_range = df.loc[ind,self.time_range_colum]
                time_range = TimeRanges(time_range)
                prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
                
                config_ma_50 = ConfigMA(length=50, type="sma")
                df = bitget_data_util.updating_ma(config_ma= config_ma_50, data_frame=df, prices_history_dict=prices_history)

                symbol = data_frame.loc[ind,DataFrameColum.SYMBOL.value]
                prices_history = prices_history[symbol]

                price_coin = prices_history['Close'].astype(float).iloc[-1]

            query = "(" + str(price_coin) + " < " + DataFrameColum.MA_LAST.value + ") and (" + DataFrameColum.MA_ASCENDING.value == False + ")"
            df_op4 = df.query(query)

            if df_op4.empty == False:
                df_op4.loc[:, DataFrameColum.LOOK.value] = "-"
                df_op4[self.step_counter] = 0
                
                return Strategy.return_for_sell(data_frame=df_op4)

        for ind in df.index:
            cntrl_profit_crrnt = float(df.loc[ind,DataFrameColum.PERCENTAGE_PROFIT.value]) - 0.3

            if float(df.loc[ind,DataFrameColum.STOP_LOSS.value]) < cntrl_profit_crrnt:
                df.loc[ind,DataFrameColum.STOP_LOSS.value] = cntrl_profit_crrnt
            
        return df
    
    @staticmethod
    def mark_price_exceeds_limit(data_frame: pandas.DataFrame, value_limit: float = 1) -> pandas.DataFrame:
        """Marcamos las cons con el profit superior al limite marcado"""
        data_frame.loc[data_frame[DataFrameColum.PERCENTAGE_PROFIT.value] >= value_limit, DataFrameColum.LOOK.value] = 'headdress!'
        return data_frame


    @staticmethod
    def return_for_sell(data_frame: pandas.DataFrame) -> pandas.DataFrame:

        data_frame[DataFrameColum.ORDER_ID.value] = "-"
        #data_frame[DataFrameColum.SIDE_TYPE.value] = "-"        
        data_frame[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
        data_frame[DataFrameColum.STOP_LOSS_LEVEL.value] = 0
        data_frame[DataFrameColum.LOOK.value] = False
        TelegramNotify.notify_sell(settings=settings, dataframe=data_frame)
        return data_frame

    def calcular_pivote_soporte_resistencia(df: pandas.DataFrame, currency_data_dictionary:dict=None):
        # Calcular el punto de pivote y los niveles de soporte y resistencia

        for ind in df.index:
            symbol = df[DataFrameColum.SYMBOL.value][ind]    
            prices_history_dict = currency_data_dictionary[symbol]

            h = prices_history_dict['High'].astype(float)
            l = prices_history_dict['Low'].astype(float)
            c = prices_history_dict['Close'].astype(float)

            pivot_point = (h + l + c) / 3

            soporte_1 = (2 * pivot_point) - prices_history_dict['High'].min()
            resistencia_1 = (2 * pivot_point) - prices_history_dict['Low'].min()

            soporte_2 = pivot_point - (prices_history_dict['High'].max() - prices_history_dict['Low'].min())
            resistencia_2 = pivot_point + (prices_history_dict['High'].max() - prices_history_dict['Low'].min())

            soporte_3 = soporte_1 - (prices_history_dict['High'].max() - prices_history_dict['Low'].min())
            resistencia_3 = resistencia_1 + (prices_history_dict['High'].max() - prices_history_dict['Low'].min())

        # Crear DataFrame con niveles de soporte y resistencia
        niveles = pandas.DataFrame({
            'Pivot Point': pivot_point,
            'sp_1': soporte_1,
            'rs_1': resistencia_1,
            'sp_2': soporte_2,
            'rs_2': resistencia_2,
            'sp_3': soporte_3,
            'rs_3': resistencia_3
        })

        sp1 = niveles['sp_1'].iloc[-1]
        sp2 = niveles['sp_2'].iloc[-1]
        rs1 = niveles['rs_1'].iloc[-1]
        rs2 = niveles['rs_2'].iloc[-1]

        rango = 5

        # Contar coincidencias en cada nivel
        coincidencias_sp1 = numpy.sum(prices_history_dict['Close'].between(sp1 - rango, sp1 + rango))
        niveles['coincidencias_sp1'] = coincidencias_sp1
        # Contar coincidencias en cada nivel
        coincidencias_sp2 = numpy.sum(prices_history_dict['Close'].between(sp2 - rango, sp2 + rango))
        niveles['coincidencias_sp2'] = coincidencias_sp2
        # Contar coincidencias en cada nivel
        coincidencias_rs1 = numpy.sum(prices_history_dict['Close'].between(rs1 - rango, rs1 + rango))
        niveles['coincidencias_rs1'] = coincidencias_rs1
        # Contar coincidencias en cada nivel
        coincidencias_rs2 = numpy.sum(prices_history_dict['Close'].between(rs2 - rango, rs2 + rango))
        niveles['coincidencias_rs2'] = coincidencias_rs2

        return niveles

    def calculate_differential_percentage(valor1, valor2):
        """
        Calcula el porcentaje de diferencia entre dos valores.

        :param valor1: Primer valor.
        :param valor2: Segundo valor.
        :return: Porcentaje de diferencia.
        """
        try:
            # Calcular la diferencia entre los dos valores
            diferencia = abs(valor1 - valor2)

            # Calcular el porcentaje de diferencia
            porcentaje_diferencia = (diferencia / max(abs(valor1), abs(valor2))) * 100

            return porcentaje_diferencia

        except ZeroDivisionError:
            # Manejar el caso en que ambos valores sean cero
            return 0.0

    def calculate_percentage_to_price(price:float, percentage, over_or_under_the_price:str):

        """ Esta es la formula para calcular el take_profit en base a un """

        if over_or_under_the_price == 'over':
            return price * (1 + percentage/100)
        elif over_or_under_the_price == 'under':
            return price * (1 - percentage/100)

    def calculate_support_and_resistance (df: pandas.DataFrame, currency_data_dictionary:dict=None):
        
        for ind in df.index:
            symbol = df[DataFrameColum.SYMBOL.value][ind]    
            prices_history_dict = currency_data_dictionary[symbol]

            list_res_sup = TA.PIVOT(prices_history_dict)

            df.loc[ind, "R1"] = list_res_sup['r1'].iloc[-1]
            df.loc[ind, "R2"] = list_res_sup['r2'].iloc[-1]
            df.loc[ind, "R3"] = list_res_sup['r3'].iloc[-1]
            df.loc[ind, "R4"] = list_res_sup['r4'].iloc[-1]

            df.loc[ind, "S1"] = list_res_sup['s1'].iloc[-1]
            df.loc[ind, "S2"] = list_res_sup['s2'].iloc[-1]
            df.loc[ind, "S3"] = list_res_sup['s3'].iloc[-1]
            df.loc[ind, "S4"] = list_res_sup['s4'].iloc[-1]
        
        return df
    
    def search_for_price_matches_at_support_and_resistance (df: pandas.DataFrame, currency_data_dictionary:dict=None, range_prctg:float = 1):
        for ind in df.index:
            symbol = df[DataFrameColum.SYMBOL.value][ind]    
            prices_history_dict = currency_data_dictionary[symbol]
    
            r1 = df.loc[ind, "R1"]
            range_r1 = PriceUtil.percentage_number ( value= r1, percentage=range_prctg)
            r2 = df.loc[ind, "R2"]
            range_r2 = PriceUtil.percentage_number ( value= r2, percentage=range_prctg)
            r3 = df.loc[ind, "R3"]
            range_r3 = PriceUtil.percentage_number ( value= r3, percentage=range_prctg)
            r4 = df.loc[ind, "R4"]
            range_r4 = PriceUtil.percentage_number ( value= r4, percentage=range_prctg)

            # Contar coincidencias en cada nivel
            df.loc[ind, 'matches_R1'] = numpy.sum(prices_history_dict['Close'].between(r1 - range_r1, r1 + range_r1))
            df.loc[ind, 'matches_R2'] = numpy.sum(prices_history_dict['Close'].between(r2 - range_r2, r2 + range_r2))
            df.loc[ind, 'matches_R3'] = numpy.sum(prices_history_dict['Close'].between(r3 - range_r3, r3 + range_r3))
            df.loc[ind, 'matches_R4'] = numpy.sum(prices_history_dict['Close'].between(r4 - range_r4, r4 + range_r4))

            s1 = df.loc[ind, "S1"]
            range_s1 = PriceUtil.percentage_number ( value= s1, percentage=range_prctg)
            s2 = df.loc[ind, "S2"]
            range_s2 = PriceUtil.percentage_number ( value= s2, percentage=range_prctg)
            s3 = df.loc[ind, "S3"]
            range_s3 = PriceUtil.percentage_number ( value= s3, percentage=range_prctg)
            s4 = df.loc[ind, "S4"]
            range_s4 = PriceUtil.percentage_number ( value= s4, percentage=range_prctg)

            df.loc[ind, 'matches_S1'] = numpy.sum(prices_history_dict['Close'].between(s1 - range_s1, s1 + range_s1))
            df.loc[ind, 'matches_S2'] = numpy.sum(prices_history_dict['Close'].between(s2 - range_s2, s2 + range_s2))
            df.loc[ind, 'matches_S3'] = numpy.sum(prices_history_dict['Close'].between(s3 - range_s3, s3 + range_s3))
            df.loc[ind, 'matches_S4'] = numpy.sum(prices_history_dict['Close'].between(s4 - range_s4, s4 + range_s4))
        
        return df

    def save_close_and_open_in_dataframe (df: pandas.DataFrame, currency_data_dictionary:dict=None):
       
        for ind in df.index:
            symbol = df[DataFrameColum.SYMBOL.value][ind]    
            prices_history_dict = currency_data_dictionary[symbol]

            close_numpy = numpy.array(prices_history_dict['Close'].astype(float).values)
            close_numpy = close_numpy[~numpy.isnan(close_numpy)]

            open_numpy = numpy.array(prices_history_dict['Open'].astype(float).values)
            open_numpy = open_numpy[~numpy.isnan(open_numpy)]

            df[DataFrameColum.NOTE.value][ind] = close_numpy
            df[DataFrameColum.NOTE_2.value][ind] = open_numpy

        
        return df
    
    def diferencia_porcentual(df: pandas.DataFrame, name_column_values:str, name_column_save:str, number_of_samples:int = 20, porcentual:float = 1):
        # Una forma de ver si lateraliza es con una lista de valores (10 elementos), coger el maximo y minimo
        # Y saber la diferencia porcentua. Si no varia mucho en el tiempo es que el precio lateraliza
        
        
        for ind in df.index:
            # Calcular el máximo y el mínimo
            values = df.loc[ind,name_column_values][-number_of_samples:]
            maximo = max(values)
            minimo = min(values)

            # Calcular la diferencia porcentual
            dif_porcentual = ((maximo - minimo) / minimo) * 100
            df.loc[ind, name_column_save] = dif_porcentual
       
        return df

    def return_for_buy(self, bitget_data_util: BitgetDataUtil, df: pandas.DataFrame) -> pandas.DataFrame:
        
        profit_percentage = 0.5

        for ind in df.index:
            
            t = df.loc[ind, self.time_range_colum]
            time_range = TimeRanges.get_time_range_by_name(t)
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

            actual_price = df.loc[ind, DataFrameColum.PRICE_CLOSE.value]   
            step = df.loc[ind, self.step_counter]
            
            percentage = actual_price * profit_percentage # / 100
            symbol = df[DataFrameColum.SYMBOL.value][ind]
            
            prices_history_dict = prices_history[symbol]

            if step == 3: #LONG
                value_S4 =  TA.PIVOT(prices_history_dict)['s4'].iloc[-1]
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_S4
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = actual_price + percentage
            elif step == 4: #SHORT
                value_R4 =  TA.PIVOT(prices_history_dict)['r4'].iloc[-1]
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_R4
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = actual_price - percentage
            
            df.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            df.loc[ind, DataFrameColum.LEVEREAGE.value] = 5
            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df.loc[ind, self.step_counter] = 5
            
        self.print_data_frame(message="EJECUTAR COMPRA", data_frame=df)
        return df
    
    def concatenates_copies_of_dataframes_at_different_temporalities(dataframe, list_time_range):
        # Lista para almacenar las copias
        copias = []
        first_iteration = True
        start_position = dataframe.index.max() + 1
        end_position = start_position + len(dataframe)

        # Generar copias y añadirlas a la lista
        for value_time in list_time_range:

            dataframe['TIME_RANGE'] = value_time
            if not first_iteration:
                dataframe = dataframe.reset_index(drop=True).set_index(pandas.Index(range(start_position, end_position)))
                start_position = end_position
                end_position = start_position + len(dataframe)
            else:
                first_iteration = False
                
            copias.append(dataframe.copy())

        # Concatenar las copias verticalmente
        df = pandas.concat(copias)

        return df

    def return_for_buy_test(self, bitget_data_util: BitgetDataUtil, df: pandas.DataFrame) -> pandas.DataFrame:
        
        profit_percentage = 0.5

        for ind in df.index:
            
            t = df.loc[ind, self.time_range_colum]
            time_range = TimeRanges.get_time_range_by_name(t)
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
           
            symbol = df[DataFrameColum.SYMBOL.value][ind]
            
            prices_history_dict = prices_history[symbol]

            value_R4 =  TA.PIVOT(prices_history_dict)['r4'].iloc[-1]
            df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_R4
            df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = TA.PIVOT(prices_history_dict)['s4'].iloc[-1]
            df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            
            df.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            df.loc[ind, DataFrameColum.LEVEREAGE.value] = 5
            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df.loc[ind, self.step_counter] = 5
            break
            
        self.print_data_frame(message="EJECUTAR COMPRA", data_frame=df)
        return df
    
    def print_data_frame(self, message: str, data_frame: pandas.DataFrame, print_empty:bool=True):

        if data_frame.empty == False:
            print(message)
            print("#####################################################################################################################")
            print(data_frame[[
                DataFrameColum.ORDER_ID.value,
                DataFrameColum.BASE.value,
                DataFrameColum.PERCENTAGE_PROFIT.value,
                DataFrameColum.STOP_LOSS.value,
                DataFrameColum.SIDE_TYPE.value,            
                self.step_counter,
                #DataFrameColum.PRICE_CLOSE.value,
                self.time_range_colum,
                self.parallel_limit_column, 
                self.diferencia_percentual_column,
                DataFrameColum.LOOK.value,
                DataFrameColum.MA_LAST_ANGLE.value,
                ]])
            