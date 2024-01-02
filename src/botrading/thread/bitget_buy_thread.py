import threading
from typing import List
import pandas
import time

from src.botrading.model.time_ranges import *
from src.botrading.bit import BitgetClienManager
from src.botrading.strategy.strategy import Strategy
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils import traiding_operations
from src.botrading.utils import excel_util
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.dataframe_util import DataFrameUtil

class BitgetBuyThreed(threading.Thread):

    thread_name: str

    client_bit: BitgetClienManager

    bitget_data_util: BitgetDataUtil

    max_coin_buy: int
    
    quantity_buy_order : int

    data_frame: pandas.DataFrame

    data_frame_name: str = "data_frame_file.xlsx"
    
    stop_thread = False
    
    pause_thread = False

    buy_thread_readry: bool
    
    strategy:Strategy

    on_colum_dict = {}

    def __init__(
        self,
        client_bit: BitgetClienManager,
        max_coin_buy: int = 5,
        quantity_buy_order: int = 100,
        observe_coin_list: List[str] = [],
        remove_coin_list: List[str] = [],
        load_from_previous_execution: bool = False,
        strategy:Strategy = None
    ):
        threading.Thread.__init__(self)
        self.buy_thread_readry = False
        self.max_coin_buy = max_coin_buy
        self.quantity_buy_order = quantity_buy_order
        self.thread_name = strategy.name + " BUY"
        self.strategy = strategy
        self.client_bit = client_bit
        
        self.bitget_data_util = BitgetDataUtil(
            client_bit=client_bit,
            crypto_observe_default_list=observe_coin_list,
            load_from_previous_execution=load_from_previous_execution,
            crypto_remove_list=remove_coin_list
        )
        if load_from_previous_execution == False:
            self.data_frame = self.bitget_data_util.data_frame_bkp
            excel_util.save_data_frame(data_frame = self.data_frame, exel_name=self.data_frame_name)
        else:
            self.data_frame = self.bitget_data_util.data_frame_bkp

        self.buy_thread_readry = True

    def lock_buy_thread(self):
        self.buy_thread_readry = False

    def unlock_buy_thread(self):
        self.buy_thread_readry = True

    def merge_dataframes(self, update_data_frame: pandas.DataFrame):

        if self.buy_thread_readry:
            self.lock_buy_thread()

            #Sustituye las lineas Slave con Master
            #self.data_frame = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = self.data_frame, df_slave = update_data_frame)
            self.data_frame = update_data_frame
            
            excel_util.save_data_frame(data_frame=self.data_frame, exel_name=self.data_frame_name)

            self.unlock_buy_thread()
            return self.data_frame
        else:
            self.wait_buy_thread_ready()
            #Sustituye las lineas Slave con Master
            self.merge_dataframes(update_data_frame=update_data_frame)
            return self.data_frame

    def wait_buy_thread_ready(self):

        while True:
            # logger.debug("########################### ESPERANDO ACTUALIZACION DE DATOS PARA " + wait_for + " ###########################")
            if self.buy_thread_readry:
                break
            time.sleep(0.1)

    def get_data_frame(self) -> pandas.DataFrame:
        return self.data_frame

    def set_data_frame(self, data_frame: pandas.DataFrame):
        self.data_frame = data_frame

    def get_bitget_data_util(self) -> BitgetDataUtil:
        return self.bitget_data_util

    def run(self):

        continue_buy = True

        while continue_buy:
            
            if self.stop_thread:
                print("Hilo de compras parado")
                break
            
            if self.pause_thread:
                while self.pause_thread:
                    print("Hilo de compras pausado")
                    time.sleep(1)

            self.wait_buy_thread_ready()

            # logger.info("########################### DATOS PARA EJECUCION DE LA PROXIMA COMPRA ########################### ")

            data_frame_for_buy = self.update_data_strategy_for_buy(data_frame=self.data_frame)

            if data_frame_for_buy.empty == False:

                rules = [ColumStateValues.READY_FOR_BUY]
                state_query = RuleUtils.get_rules_search_by_states(rules)
                data_frame_buy_now = data_frame_for_buy.query(state_query)

                if data_frame_buy_now.empty:
                    print("No hay monedas para comprar ahora!! ")
                    self.data_frame = self.merge_dataframes(update_data_frame=data_frame_for_buy)
                else:
                    data_frame_buy_now = self.apply_max_coin_buy(data_frame_buy_now)

                    df_buyed = traiding_operations.logic_buy(clnt_bit=self.client_bit, df_buy=data_frame_buy_now, quantity_usdt = self.quantity_buy_order)
                    # logger.info("########################### COMPRA REALIZADA ########################### ")

                    self.data_frame = self.merge_dataframes(update_data_frame=df_buyed)

            time.sleep(1)

    def update_data_strategy_for_buy(self, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        if data_frame is None:
            print("... sin datos de compra ...")
            return pandas.DataFrame()
        elif data_frame.empty:
            print("... sin datos de compra ...")
            return pandas.DataFrame()

        final_data_frame = pandas.DataFrame

        if self.check_max_coin_buy(data_frame=data_frame) == False:
            return final_data_frame

        final_data_frame = self.strategy.apply_buy(bitget_data_util=self.bitget_data_util, data_frame=data_frame)

        return final_data_frame

    def check_max_coin_buy(self, data_frame: pandas.DataFrame) -> bool:

        max_buy_data = self.apply_max_coin_buy(data_frame)

        if max_buy_data.empty == True:
            return False
        else:
            return True

    def apply_max_coin_buy(self, final_data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        rule_state_buy = RuleUtils.get_rules_search_by_states(rules)
        df_buyed = self.data_frame.query(rule_state_buy)

        count = len(df_buyed)

        if self.max_coin_buy >= count:
            buy_size: int
            buy_size = self.max_coin_buy - count
            return final_data_frame.head(buy_size)
        else:
            print("No puedes comprar monedas según el máximo establecido " + str(self.max_coin_buy))
            return pandas.DataFrame()
