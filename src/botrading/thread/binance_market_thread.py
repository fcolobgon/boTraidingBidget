import threading
from typing import List
import pandas
import time
#import logging

from src.botrading.model.time_ranges import *
from src.botrading.bnb import BinanceClienManager
from src.botrading.strategy.market.market_strategy_lobo import MarketStrategyLobo
from src.botrading.strategy.market.market_strategy_emi import MarketStrategyEmi
from src.botrading.strategy.market.market_strategy_gabri import MarketStrategyGabri

from src.botrading.utils import logger_util
from src.botrading.utils import excel_util
from src.botrading.utils.binance_data_util import BinanceDataUtil

from src.botrading.thread.enums.binance_market_status import BinanceMarketStatus
from src.botrading.thread.enums.binance_market_strategy import BinanceMarketStrategy


#logger = logging.getLogger(__name__)
#logger = logger_util.definition_logger(logger)


class BinanceMarketThreed(threading.Thread):

    strategy: BinanceMarketStrategy

    bnb_client: BinanceClienManager

    binance_data_util: BinanceDataUtil

    data_frame: pandas.DataFrame

    data_frame_name: str = "market_data_frame_file.xlsx"
    
    market_status_name: str = "market_status.txt"
    
    market_status:BinanceMarketStatus
    
    sleep_seconds:int

    on_colum_dict = {}

    def __init__(
        self,
        bnb_client: BinanceClienManager,
        strategy: BinanceMarketStrategy = None,
        sleep_seconds: int = 1
    ):
        threading.Thread.__init__(self)

        self.bnb_client = bnb_client
        self.strategy = strategy
        self.sleep_seconds = sleep_seconds
        self.binance_data_util = BinanceDataUtil(
            bnb_client=bnb_client
        )
        
        self.data_frame = self.binance_data_util.create_data_frame()
        excel_util.save_data_frame(data_frame = self.data_frame, exel_name=self.data_frame_name)
        excel_util.save_market_status_file(BinanceMarketStatus.UNKNOW)

    def run(self):
        
        try:
            
            while True:
                
                self.data_frame = self.update_market_strategy(strategy=self.strategy, data_frame=self.data_frame)

                excel_util.save_data_frame(data_frame=self.data_frame, exel_name=self.data_frame_name)
                
                self.market_status = self.update_market_status(strategy=self.strategy, data_frame=self.data_frame)

                excel_util.save_market_status_file(self.market_status)
                
                time.sleep(self.sleep_seconds)
            
        except:
            print("#####################################################################################################################")
            print("HILO DE ESTADO DE MERCADO A PETAOOOO!!!")
            print("#####################################################################################################################")
            excel_util.save_market_status_file(BinanceMarketStatus.UNKNOW)


    def update_market_strategy(self, strategy: BinanceMarketStrategy, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        if BinanceMarketStrategy.STRATEGY_LOBO == strategy:

            data_frame = MarketStrategyLobo.apply_market(binance_data_util=self.binance_data_util, data_frame=data_frame)

        if BinanceMarketStrategy.STRATEGY_EMI == strategy:

            data_frame = MarketStrategyEmi.apply_market(binance_data_util=self.binance_data_util, data_frame=data_frame)

        if BinanceMarketStrategy.STRATEGY_GABRI == strategy:

            data_frame = MarketStrategyGabri.apply_market(binance_data_util=self.binance_data_util, data_frame=data_frame)


        return data_frame
    
    def update_market_status(self, strategy: BinanceMarketStrategy, data_frame: pandas.DataFrame) -> BinanceMarketStatus:

        market_status:BinanceMarketStatus
        
        if BinanceMarketStrategy.STRATEGY_LOBO == strategy:

            market_status = MarketStrategyLobo.apply_market_status(binance_data_util=self.binance_data_util, data_frame=data_frame)

        if BinanceMarketStrategy.STRATEGY_EMI == strategy:

            market_status = MarketStrategyEmi.apply_market_status(binance_data_util=self.binance_data_util, data_frame=data_frame)

        if BinanceMarketStrategy.STRATEGY_GABRI == strategy:

            market_status = MarketStrategyGabri.apply_market_status(binance_data_util=self.binance_data_util, data_frame=data_frame)


        return market_status