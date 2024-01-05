import pandas
import time
from finta import TA

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.colum_good_bad_values import ColumLineValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.future_values import FutureValues

from configs.config import settings as settings

class Strategy:
    
    name:str
    first_iteration = True
    step_counter = "STEP_COUNTER"
    
    def __init__(self, name:str):
        
        self.name = name
        self.step_counter = "STEP_COUNTER"

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        if self.first_iteration:
            
            df = DataFrameCheckUtil.add_columns_to_dataframe(
                column_names=[self.step_counter], df=df)
            
            df[self.step_counter] = 0
            self.first_iteration = False
            self.print_data_frame(message="CREADO DATAFRAME", data_frame=df)
            return df
        
        self.print_data_frame(message="INICIO COMPRA", data_frame=df)
        time_range = TimeRanges("HOUR_4")
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

        df = bitget_data_util.updating_adx(data_frame=df, prices_history_dict=prices_history)
        df = bitget_data_util.updating_ao(data_frame=df, prices_history_dict=prices_history)
        df = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history)
        
        time_range = TimeRanges("HOUR_1")
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)
        df = bitget_data_util.updating_stochrsi(data_frame=df, prices_history_dict=prices_history)

        df = df.sort_values(by=self.step_counter, ascending=False)
        self.print_data_frame(message="DATOS COMPRA ACTUALIZADO", data_frame=df)
        
        for ind in df.index:

            symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
            ao_ascending = df.loc[ind, DataFrameColum.AO_ASCENDING.value]
            adx_angle = df.loc[ind, DataFrameColum.ADX_ANGLE.value]
            step = df.loc[ind, self.step_counter]
            
            if adx_angle > 0 and adx_angle < 100:
                if ao_ascending:
                    df.loc[ind, self.step_counter] = 1
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
                    
                    self.print_data_frame(message=symbol + " -> PASO 0 FINALIZADO", data_frame=df)
                    return df
                
                if not ao_ascending:
                    df.loc[ind, self.step_counter] = 2
                    df.loc[ind, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
                
                    self.print_data_frame(message=symbol + " -> PASO 0 FINALIZADO", data_frame=df)
                    return df
                
            if step == 3: #LONG 
                
                cruce_rsi = df.loc[ind,DataFrameColum.RSI_STOCH_CRUCE_LINE.value]
                
                if cruce_rsi == ColumLineValues.BLUE_CRUCE_TOP.value:
                    return self.return_for_buy(bitget_data_util=bitget_data_util, df=df)
            
            if step == 4: #SHORT
                
                cruce_rsi = df.loc[ind,DataFrameColum.RSI_STOCH_CRUCE_LINE.value]
                
                if cruce_rsi == ColumLineValues.BLUE_CRUCE_DOWN.value:
                    return self.return_for_buy(bitget_data_util=bitget_data_util, df=df)
                
        return pandas.DataFrame()
    
    def return_for_buy(self, bitget_data_util: BitgetDataUtil, df: pandas.DataFrame) -> pandas.DataFrame:
        
        profit_percentage = 0.5
        time_range = TimeRanges("HOUR_1")
        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

        df = bitget_data_util.updating_price_indicators(data_frame=df, prices_history_dict=prices_history)
        
        for ind in df.index:

            actual_price = df.loc[ind, DataFrameColum.PRICE_CLOSE.value]   
            step = df.loc[ind, self.step_counter]
            
            percentage = actual_price * profit_percentage / 100
            symbol = df[DataFrameColum.SYMBOL.value][ind]
            
            prices_history_dict = prices_history[symbol]

            if step == 3: #LONG
                value_S4 =  TA.PIVOT(prices_history_dict)['s4'].iloc[-1]
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_S4
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = actual_price + percentage
            else: #SHORT
                value_R4 =  TA.PIVOT(prices_history_dict)['r4'].iloc[-1]
                df.loc[ind, DataFrameColum.STOP_LOSS.value] =  value_R4
                df.loc[ind, DataFrameColum.TAKE_PROFIT.value] = actual_price - percentage
            
            df.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            df.loc[ind, DataFrameColum.LEVEREAGE.value] = 5
            df.loc[ind, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
            df.loc[ind, self.step_counter] = 5
            
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
            
            df = bitget_data_util.updating_open_orders(data_frame=df)
            df.loc[~df[DataFrameColum.ORDER_OPEN.value], DataFrameColum.STATE.value] = ColumStateValues.SELL.value
            return df
    
    
    def print_data_frame(self, message: str, data_frame: pandas.DataFrame, print_empty:bool=True):

        if data_frame.empty == False:
            print(message)
            print("#####################################################################################################################")
            print(data_frame[[
                DataFrameColum.BASE.value,
                DataFrameColum.TAKE_PROFIT.value,
                DataFrameColum.STOP_LOSS.value,
                DataFrameColum.SIDE_TYPE.value,            
                DataFrameColum.AO_ASCENDING,
                DataFrameColum.ADX_ANGLE,
                DataFrameColum.STOCH_CRUCE_LINE
                ]])