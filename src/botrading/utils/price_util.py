import time
import pandas

from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.thread.enums.binance_market_status import BinanceMarketStatus

class PriceUtil:
    
    @staticmethod
    def plus_percentage_price(price, percentage):
        
        percentage = percentage/100
        qty = price * percentage
        return price + qty
    
    @staticmethod
    def minus_percentage_price(price, percentage):
        
        percentage = percentage/100
        qty = price * percentage
        return price - qty
    
    @staticmethod
    def porcentaje_valores_absolutos(valor_1, valor_2):

        porcentaje = abs(valor_1 - valor_2) / valor_2
        return porcentaje * 100

    @staticmethod
    def calculate_valid_price(price, price_place, price_end_step, volume_place):    

            adjusted_btc_amount = round(price, volume_place)

            return adjusted_btc_amount
