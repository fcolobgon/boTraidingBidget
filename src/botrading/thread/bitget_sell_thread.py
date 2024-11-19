import threading
import pandas
import time


from src.botrading.thread.bitget_buy_thread import BitgetBuyThreed
from src.botrading.model.time_ranges import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.bit import BitgetClienManager
from src.botrading.strategy.strategy import Strategy
from src.botrading.utils import traiding_operations
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.enums.data_frame_colum import ColumStateValues


class BitgetSellThreed(threading.Thread):

    thread_name: str

    strategy:Strategy

    client_bit: BitgetClienManager

    bitget_data_util: BitgetDataUtil
    
    stop_thread = False
    
    pause_thread = False

    def __init__(
        self,
        client_bit: BitgetClienManager,
        buy_thread: BitgetBuyThreed = None,
        strategy: Strategy = None
    ):
        buy_thread.wait_buy_thread_ready()
        threading.Thread.__init__(self)
        self.thread_name = strategy.name + " SELL"
        self.strategy = strategy
        self.client_bit = client_bit
        self.buy_thread = buy_thread
        self.bitget_data_util = buy_thread.get_bitget_data_util()

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
                
                data_frame_for_sell = self.strategy.apply_sell(bitget_data_util=self.bitget_data_util, data_frame=self.buy_thread.get_data_frame())

                if data_frame_for_sell is not None and not data_frame_for_sell.empty:

                    rules = [ColumStateValues.READY_FOR_SELL]
                    state_query = RuleUtils.get_rules_search_by_states(rules)
                    data_frame_sell_now = data_frame_for_sell.query(state_query)

                    if data_frame_sell_now.empty:
                        #print("No hay monedas para vender ahora!!")
#! INI 240919 - Nueva función que permite modificar el TakeProfit y el StopLoss
                        traiding_operations.logic_modify_TPSL(clnt_bit=self.client_bit, df_modify=data_frame_for_sell)
#! FIn 240919 - Nueva función que permite modificar el TakeProfit y el StopLoss
                        self.buy_thread.merge_dataframes(update_data_frame=data_frame_for_sell)
                    else:
                        df_sell = traiding_operations.logic_sell(clnt_bit=self.client_bit, df_sell=data_frame_sell_now)

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
        