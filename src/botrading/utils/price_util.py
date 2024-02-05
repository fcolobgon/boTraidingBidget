import time
import pandas

from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.thread.enums.binance_market_status import BinanceMarketStatus
from src.botrading.bit import BitgetClienManager
import math

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



    def calculate_size_with_leverage(clnt_bit: BitgetClienManager, symbol, quantity_usdt, volume_place, leverage):

        price_coin = float(clnt_bit.client_bit.mix_get_single_symbol_ticker(symbol=symbol)['data']['last'])
        size = (quantity_usdt / price_coin) * leverage
        size = round (size, volume_place)

        return size, price_coin
    
    @staticmethod
    def formatting_the_price(size, price_place, price_end_step, volume_place):

        buy_quantity = "{:0.0{}f}".format(size, price_end_step)
        fquantity = float(buy_quantity)
        formatted_price = round(fquantity, volume_place)

        return formatted_price
    

    def create_multiple (price_place, price_end_step:int = 0):

        value_format = "{:0.0{}f}".format(0,price_place)
        value_format = value_format[:-1] + str(price_end_step)

        return float(value_format)
        

    def multiple_next_price (value, price_place, price_end_step, volume_place):
        """
        Calcula el próximo múltiplo a partir de un valor dado y un multiplicador.

        :param valor: Valor base.
        :param multiplicador: Múltiplo a utilizar.
        :return: Próximo múltiplo.
        """
        try:
            if volume_place == 0: 
                formatted_value = int(value)
            else:
                formatted_value = round(value, volume_place)

            multiplicator = PriceUtil.create_multiple (price_place, price_end_step)
            # Calcular el próximo múltiplo
            next_multiple = (formatted_value / multiplicator) * multiplicator

            if volume_place == 0: 
                formatted_value = int(next_multiple)
            else:
                formatted_value = round(next_multiple, volume_place)

            return formatted_value

        except ZeroDivisionError:
            # Manejar el caso en que el multiplicador sea cero
            return value
        

    def multiple_next_limit (value, price_place, price_end_step):
        """
        Calcula el próximo múltiplo a partir de un valor dado y un multiplicador.

        :param valor: Valor base.
        :param multiplicador: Múltiplo a utilizar.
        :return: Próximo múltiplo.
        """
        try:
            formatted_value = str(round(value, price_place))

            if price_end_step != 1:
                formatted_value = formatted_value[:-1] + str(price_end_step)

            return float(formatted_value)

        except ZeroDivisionError:
            # Manejar el caso en que el multiplicador sea cero
            return value
    
    def longitud_parte_decimal(numero):
        # Convertir el número a cadena de texto
        cadena_numero = str(numero)

        # Verificar si hay un punto decimal en la cadena
        if '.' in cadena_numero:
            # Encontrar la posición del punto decimal
            indice_punto_decimal = cadena_numero.index('.')

            # Calcular la longitud de la parte decimal
            longitud_decimal = len(cadena_numero) - indice_punto_decimal - 1

            return longitud_decimal
        else:
            # Si no hay punto decimal, la longitud es 0
            return 0
