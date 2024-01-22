import pandas
import time
from finta import TA
import pandas_ta 

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.utils.price_util import PriceUtil
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
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
    time_range_colum = "TIME_RANGE"
    startTime:datetime
    
    def __init__(self, name:str):
        
        self.name = name
        self.step_counter = "STEP_COUNTER"
        self.time_range_colum = "TIME_RANGE"
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        
        
    def get_time_ranges(self) -> []:
        return ["MINUTES_5", "MINUTES_15","MINUTES_30", "HOUR_1"]
        #return ["MINUTES_5"]


    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = data_frame.query(state_query)
        
        if self.first_iteration:
            
            from_prev = settings.LOAD_FROM_PREVIOUS_EXECUTION
            
            if from_prev:
                return df
            
            df = DataFrameCheckUtil.add_columns_to_dataframe(
                column_names=[self.step_counter,
                              self.time_range_colum], df=df)
            
            nuevos_valores_tipo = self.get_time_ranges()
            df = pandas.concat([df.assign(TIME_RANGE=time_range) for time_range in nuevos_valores_tipo], ignore_index=True)
            
            for ind in df.index:
                
                time = df.loc[ind, self.time_range_colum]
                
                if "MINUTES_5" == time:
                    p = 0.4
                if "MINUTES_15" == time:
                    p = 0.8
                if "MINUTES_30" == time:
                    p = 1.2
                if "HOUR_1" == time:
                    p = 1.6
                
                df.loc[ind, DataFrameColum.NOTE.value] = p
                df.loc[ind, DataFrameColum.ID_DF.value] = datetime.now().microsecond
            
            df[self.step_counter] = 0
            self.first_iteration = False
            self.print_data_frame(message="CREADO DATAFRAME", data_frame=df)
            
            return df
        
        #return self.return_for_buy_test(bitget_data_util=bitget_data_util, df=df)
        
        time_ranges = self.get_time_ranges()
        
        for t in time_ranges:
        
            time_range = TimeRanges.get_time_range_by_name(t)
            prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = df, time_range = time_range)

            for ind in df.index:
                
                time = df.loc[ind, self.time_range_colum]
                
                if t != time:
                    continue

                symbol = df.loc[ind, DataFrameColum.SYMBOL.value]
                step = df.loc[ind, self.step_counter]
                
                # Implementar a partir de medias moviles v2
        
        sell_df = self.return_for_buy(df=df)
        
        if sell_df.empty:
            self.print_data_frame(message="COMPRA ACTUALIZADA", data_frame=df)
            return df
        
        return sell_df

    
    def return_for_buy(self, df: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.READY_FOR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        df = df.query(state_query)
        
        df[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
        df[DataFrameColum.LEVEREAGE.value] = 5
        df[DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value
        df[self.step_counter] = 5
        
        TelegramNotify.notify_df(settings=settings, dataframe=df, message="Nueva compra ")
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
                    TelegramNotify.notify_df(settings=settings, dataframe=sell_df, message="Nueva venta ")
                    sell_df[DataFrameColum.ORDER_ID.value] = "-"
                    sell_df[self.step_counter] = 0
                    sell_df[DataFrameColum.TAKE_PROFIT.value] = 0.0
                    sell_df[DataFrameColum.STOP_LOSS.value] = 0.0
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
                self.step_counter,
                self.time_range_colum,
                DataFrameColum.ID_DF.value
                ]])