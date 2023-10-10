import threading
from typing import List
import pandas
import time

from src.botrading.model.time_ranges import *
from src.botrading.bnb import BinanceClienManager
from src.botrading.strategy.strategy import Strategy
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils import traiding_operations
from src.botrading.utils import excel_util
from src.botrading.utils.binance_data_util import BinanceDataUtil
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.enums.data_frame_colum import ColumStateValues

class BinanceBuyThreed(threading.Thread):

    thread_name: str

    bnb_client: BinanceClienManager

    binance_data_util: BinanceDataUtil

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
        bnb_client: BinanceClienManager,
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
        self.bnb_client = bnb_client
        
        self.binance_data_util = BinanceDataUtil(
            bnb_client=bnb_client,
            crypto_observe_default_list=observe_coin_list,
            load_from_previous_execution=load_from_previous_execution,
            crypto_remove_list=remove_coin_list
        )
        if load_from_previous_execution == False:
            self.data_frame = self.binance_data_util.create_data_frame()
            excel_util.save_data_frame(data_frame = self.data_frame, exel_name=self.data_frame_name)
        else:
            self.data_frame = self.binance_data_util.data_frame_bkp

        self.buy_thread_readry = True

    def lock_buy_thread(self):
        self.buy_thread_readry = False

    def unlock_buy_thread(self):
        self.buy_thread_readry = True

    def merge_dataframes(self, update_data_frame: pandas.DataFrame):

        if self.buy_thread_readry:
            self.lock_buy_thread()

            for ind in update_data_frame.index:

                symbol = update_data_frame[DataFrameColum.SYMBOL.value][ind]

                if symbol in set(self.data_frame[DataFrameColum.SYMBOL.value]):
                    
                    self.data_frame.loc[self.data_frame[DataFrameColum.SYMBOL.value] == symbol] = update_data_frame.loc[update_data_frame[DataFrameColum.SYMBOL.value] == symbol]
                else:
                    new_crypto_data_frame = update_data_frame.loc[update_data_frame[DataFrameColum.SYMBOL.value] == symbol]
                    self.data_frame = pandas.concat([self.data_frame, new_crypto_data_frame], ignore_index=True)
            
            excel_util.save_data_frame(data_frame=self.data_frame, exel_name=self.data_frame_name)
            self.unlock_buy_thread()
            return self.data_frame
        else:
            self.wait_buy_thread_ready()
            self.merge_dataframes(update_data_frame=update_data_frame)
    
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

    def get_binance_data_util(self) -> BinanceDataUtil:
        return self.binance_data_util

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
                    print("No hay monedas para comprar ahora!!")
                    self.merge_dataframes(update_data_frame=data_frame_for_buy)
                else:
                    data_frame_buy_now = self.apply_max_coin_buy(data_frame_buy_now)

                    df_buyed = traiding_operations.logic_buy(clnt_bnb=self.bnb_client, df_buy=data_frame_buy_now, quantity_buy = self.quantity_buy_order)
                    # logger.info("########################### COMPRA REALIZADA ########################### ")

                    self.merge_dataframes(update_data_frame=df_buyed)

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

        final_data_frame = self.strategy.apply_buy(binance_data_util=self.binance_data_util, data_frame=data_frame)

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
