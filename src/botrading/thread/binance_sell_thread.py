import threading
import time


from src.botrading.thread.binance_buy_thread import BinanceBuyThreed
from src.botrading.model.time_ranges import *
from src.botrading.utils.binance_data_util import BinanceDataUtil
from src.botrading.bnb import BinanceClienManager
from src.botrading.strategy.strategy import Strategy
from src.botrading.utils import traiding_operations
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.enums.data_frame_colum import ColumStateValues


class BinanceSellThreed(threading.Thread):

    thread_name: str

    strategy:Strategy

    bnb_client: BinanceClienManager

    binance_data_util: BinanceDataUtil
    
    stop_thread = False
    
    pause_thread = False

    def __init__(
        self,
        bnb_client: BinanceClienManager,
        buy_thread: BinanceBuyThreed = None,
        strategy: Strategy = None
    ):
        buy_thread.wait_buy_thread_ready()
        threading.Thread.__init__(self)
        self.thread_name = strategy.name + " SELL"
        self.strategy = strategy
        self.bnb_client = bnb_client
        self.buy_thread = buy_thread
        self.binance_data_util = buy_thread.get_binance_data_util()

    def run(self):

        continue_sell = True

        while continue_sell:
            
            if self.stop_thread:
                print("Hilo de ventas parado")
                break
            
            if self.pause_thread:
                while self.pause_thread:
                    print("Hilo de compras pausado")
                    time.sleep(1)

            self.buy_thread.wait_buy_thread_ready()

            if self.check_dataframe_contains_buy_coins():
                
                data_frame_for_sell = self.strategy.apply_sell(binance_data_util=self.binance_data_util, data_frame=self.buy_thread.get_data_frame())
                                      
                if data_frame_for_sell.empty == False:

                    rules = [ColumStateValues.READY_FOR_SELL]
                    state_query = RuleUtils.get_rules_search_by_states(rules)
                    data_frame_sell_now = data_frame_for_sell.query(state_query)

                    if data_frame_sell_now.empty:
                        print("No hay monedas para vender ahora!!")
                        self.buy_thread.merge_dataframes(update_data_frame=data_frame_for_sell)

                    else:

                        df_sell = traiding_operations.logic_sell(clnt_bnb=self.bnb_client, df_sell=data_frame_sell_now)

                        self.buy_thread.merge_dataframes(update_data_frame=df_sell)

            time.sleep(1)
    
    def check_dataframe_contains_buy_coins(self) -> bool:
        
        data_frame = self.buy_thread.get_data_frame()
        
        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        data_frame = data_frame.query(state_query)
        
        if data_frame.empty:
            return False
        else:
            return True
        