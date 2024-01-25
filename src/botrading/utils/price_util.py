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

    def create_multiple (price_place, price_end_step:int = 0):

        try:
            # Verificar que pricePlace sea un entero positivo
            if isinstance(price_place, int) and price_place > 0:
                return price_end_step / (10 ** price_place)  # Ejemplo: 0.001 para price_place=3

            else:
                return None  # En caso de que pricePlace no sea válido

        except ZeroDivisionError:
            # Manejar el caso en que el divisor sea cero
            return None
        

    def calculate_proximate_multiple (value, price_place, price_end_step):
        """
        Calcula el próximo múltiplo a partir de un valor dado y un multiplicador.

        :param valor: Valor base.
        :param multiplicador: Múltiplo a utilizar.
        :return: Próximo múltiplo.
        """
        try:
            multiplicator = PriceUtil.create_multiple (price_place, price_end_step)
            # Calcular el próximo múltiplo
            proximo_multiplo = (value / multiplicator) * multiplicator

            return proximo_multiplo

        except ZeroDivisionError:
            # Manejar el caso en que el multiplicador sea cero
            return value