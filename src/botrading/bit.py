from multiprocessing.connection import wait
import platform
import pandas
from datetime import datetime, timedelta
from datetime import datetime

from tenacity import retry, stop_after_attempt, stop_after_delay
from pybitget import Client



class BitgetClienManager:

    retry_delay = 60
    retry_delay_attempt = 5
    client_bit: Client
    buy_sell: bool

    # OJO! NO TOCAR ESTE CONSTRUCTOR
    def __init__(self, test_mode: bool = True, api_key:str = "", api_secret:str = "", api_passphrase:str = ""):

        if test_mode:
            self.buy_sell = False
        else:
            self.buy_sell = True
        self.client_bit = Client(api_key=api_key, api_secret_key=api_secret, passphrase=api_passphrase,use_server_time=False)

        # Check OS
        my_os = platform.system()

        """
        if my_os == "Windows":
            from src.botrading.utils.binance_client_util import BinanceClientUtil
            
            #Synchronise Time for Win
            BinanceClientUtil.synchronise_times(self.client_bit)
        """
    
    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_all_coins_filter_contract(self, productType):

        dict_symbol_inf = self.client_bit.mix_get_symbols_info(productType = productType )['data']
        df_symbol_inf = pandas.DataFrame(dict_symbol_inf)

        return df_symbol_inf
    
    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_historial_x_day_ago(self, symbol, x_days, interval, retry = 0, limit:int = 500) -> pandas.DataFrame:
        
        endTime = datetime.now()
        startTime = endTime - timedelta(days=x_days)
        
        #startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)
        #endTime = endTime.replace(hour=0, minute=0, second=0, microsecond=0)
        startTime_ms = int(startTime.timestamp() * 1000)
        endTime_ms = int(endTime.timestamp() * 1000)

        temp_data = self.client_bit.mix_get_candles(symbol=symbol, startTime=startTime_ms, endTime=endTime_ms, granularity=interval, kLineType='market', limit=limit)
            
        df_history_symbol = pandas.DataFrame(
            temp_data,
            columns=[
                "Open time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Close time"
            ],
        )

        # df_history_symbol.insert(column="isNew", value=isNew,  loc=1, allow_duplicates=False)

        return df_history_symbol
    
    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_price_for_symbol(self, symbol: str) -> float:
        """Devuelve el precio de una moneda asociada a su par

        Args:
            symbol (str): Simbolo de la moneda y par, ej, para saber el precio de BTC en USDT -> BTCUSDT

        Returns:
            float: Precio actual de la moneda en formato FLOAT
        """
        return float(self.client_bit.mix_get_market_price(symbol=symbol)["data"]["markPrice"])
    
    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_open_orders(self, marginCoin, productType) -> pandas.DataFrame:
    
        data = self.client_bit.mix_get_all_positions(marginCoin=marginCoin,productType=productType)
        
        orders = data["data"]
        
        return pandas.DataFrame(orders)
    
    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_orders_history(self, productType:str, startTime:datetime) -> pandas.DataFrame:
        
        startTime_ms = int(startTime.timestamp() * 1000)
        endTime_ms = int(datetime.now().timestamp() * 1000)
        
        #end_time_formatted = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        
        productType = productType.upper()
        
        data = self.client_bit.mix_get_productType_history_orders(productType=productType, startTime=startTime_ms, endTime=endTime_ms, pageSize=100)
        dataOrder = data["data"]
        orders = dataOrder["orderList"]
        
        if not orders:
            return pandas.DataFrame()
                
        return pandas.DataFrame(orders)
    
    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_open_positions(self, productType:str) -> pandas.DataFrame:
                
        productType = productType.upper()
        
        data = self.client_bit.mix_get_all_positions(productType=productType)
        positions = data["data"]
        
        if not positions:
            return pandas.DataFrame()
                
        return pandas.DataFrame(positions)
    

