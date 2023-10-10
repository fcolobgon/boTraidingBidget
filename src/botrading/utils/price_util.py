import time
import pandas

from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.binance_data_util import BinanceDataUtil
from src.botrading.thread.enums.binance_market_status import BinanceMarketStatus

class PriceUtil:
    
    @staticmethod
    def wait_good_price_df(binance_data_util: BinanceDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        df = PriceUtil.wait_ascending_price(binance_data_util=binance_data_util, data_frame=data_frame)
        return PriceUtil.wait_descending_price(binance_data_util=binance_data_util, data_frame=df)
    
    @staticmethod
    def wait_good_price(binance_data_util: BinanceDataUtil, data_frame: pandas.DataFrame) -> bool:
        
        PriceUtil.wait_ascending_price(binance_data_util=binance_data_util, data_frame=data_frame)
        PriceUtil.wait_descending_price(binance_data_util=binance_data_util, data_frame=data_frame)
        
        return True
    
    @staticmethod
    def wait_ascending_price(binance_data_util: BinanceDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        aux_data_frame = data_frame.head(1)
        aux_data_frame = binance_data_util.updating_price(data_frame = aux_data_frame)
        exit = False
        for ind in data_frame.index:

            while(exit == False):
                time.sleep(0.5)
                symbol = aux_data_frame.loc[ind, DataFrameColum.SYMBOL.value]
                print("Esperando fin de subida de precio de " + str(symbol))
                previous_price = aux_data_frame.loc[ind, DataFrameColum.PRICE_BUY.value]
                previous_price = float(previous_price)
                aux_data_frame = binance_data_util.updating_price(data_frame = aux_data_frame)
                actual_price = aux_data_frame.loc[ind, DataFrameColum.PRICE_BUY.value]
                actual_price = float(actual_price)
                if previous_price > actual_price:
                    exit = True
                
        return aux_data_frame   
    
    @staticmethod
    def wait_descending_price(binance_data_util: BinanceDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        aux_data_frame = data_frame.head(1)
        aux_data_frame = binance_data_util.updating_price(data_frame = aux_data_frame)
        exit = False
        for ind in data_frame.index:

            while(exit == False):
                time.sleep(0.5)
                symbol = aux_data_frame.loc[ind, DataFrameColum.SYMBOL.value]
                print("Esperando fin de bajada de " + str(symbol))
                previous_price = aux_data_frame.loc[ind, DataFrameColum.PRICE_BUY.value]
                previous_price = float(previous_price)
                aux_data_frame = binance_data_util.updating_price(data_frame = aux_data_frame)
                actual_price = aux_data_frame.loc[ind, DataFrameColum.PRICE_BUY.value]
                actual_price = float(actual_price)
                if previous_price < actual_price:
                    exit = True
               
        return aux_data_frame

    @staticmethod
    def check_accumulated_take_profit(data_frame: pandas.DataFrame, profit:int) -> bool:
        
        if len(data_frame) < 2:
            return False 
        
        #if (data_frame[DataFrameColum.PERCENTAGE_PROFIT.value] < 0).any():
        #    profit = 3
        
        actual_profit = data_frame[DataFrameColum.PERCENTAGE_PROFIT.value].sum().astype(float)
        
        if actual_profit > profit:
            return True
        else:
            return False
    
    @staticmethod
    def check_accumulated_take_profit_by_market_status(data_frame: pandas.DataFrame, sell_all:float, very_bad:float, bad:float, good:float, very_good:float, on_fire:float) -> bool:
    
        if data_frame.empty == True:
            return False 
        
        profit = 1
        status_market = excel_util.read_market_status_file()
        
        if BinanceMarketStatus.SELL_ALL == status_market:
            profit = sell_all
        
        if BinanceMarketStatus.VERY_BAD == status_market:
            profit = very_bad
            
        if BinanceMarketStatus.BAD == status_market:
            profit = bad
        
        if BinanceMarketStatus.GOOD == status_market:
            profit = good
        
        if BinanceMarketStatus.VERY_GOOD == status_market:
            profit = very_good
        
        if BinanceMarketStatus.ON_FIRE == status_market:
            profit= on_fire
        
        if BinanceMarketStatus.UNKNOW == status_market:
            profit= 0.4 
                

        return PriceUtil.check_accumulated_take_profit(data_frame=data_frame, profit=profit)
    
    @staticmethod
    def update_take_profit_by_market_status(data_frame: pandas.DataFrame, sell_all:float, very_bad:float, bad:float, good:float, very_good:float, on_fire:float) -> pandas.DataFrame:
        
        if data_frame.empty == True:
            return data_frame 
        
        status_market = excel_util.read_market_status_file()
        
        if BinanceMarketStatus.SELL_ALL == status_market:
            data_frame[DataFrameColum.TAKE_PROFIT.value] = sell_all
        
        if BinanceMarketStatus.VERY_BAD == status_market:
            data_frame[DataFrameColum.TAKE_PROFIT.value] = very_bad
            
        if BinanceMarketStatus.BAD == status_market:
            data_frame[DataFrameColum.TAKE_PROFIT.value] = bad
        
        if BinanceMarketStatus.GOOD == status_market:
            data_frame[DataFrameColum.TAKE_PROFIT.value] = good
        
        if BinanceMarketStatus.VERY_GOOD == status_market:
            data_frame.loc[:,DataFrameColum.TAKE_PROFIT.value] = very_good
        
        if BinanceMarketStatus.ON_FIRE == status_market:
            data_frame[DataFrameColum.TAKE_PROFIT.value] = on_fire 
                    
        return data_frame
