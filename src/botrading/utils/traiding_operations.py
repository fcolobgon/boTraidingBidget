import pandas
from pybitget import Client
from binance import helpers
from datetime import datetime
from datetime import timedelta

from src.botrading.bit import BitgetClienManager
from src.botrading.constants import botrading_constant
from src.botrading.utils.price_util import PriceUtil
from src.botrading.telegram.telegram_notify import TelegramNotify

def get_history(clnt_bit: Client, symbol, interval):
    
    kLineType='market'
    limit=1000
    
    endTime = datetime.now()
    startTime = endTime - timedelta(days=100)
    startTime_ms = int(startTime.timestamp() * 1000)
    endTime_ms = int(endTime.timestamp() * 1000)
    
    candels = clnt_bit.mix_get_candles(symbol=symbol, startTime=startTime_ms, endTime=endTime_ms, granularity=interval, kLineType=kLineType, limit=limit)
    
    data = pandas.DataFrame(
        candels,
        columns=[
            "Open Time",
            "Open",
            "High", 
            "Low",
            "Close",
            "Volume",
            "Close time"])
    
    data['Open'] = data['Open'].astype(float)
    data['High'] = data['High'].astype(float)
    data['Low'] = data['Low'].astype(float)
    data['Close'] = data['Close'].astype(float)
    data['Volume'] = data['Volume'].astype(float)
    return data
    

def get_open_positions(clnt_bit: Client):
    
    productType = botrading_constant.FUTURE_CONTRACT_USDT_UMCBL
    marginCoin = botrading_constant.MARIN_COIN
    data = clnt_bit.mix_get_all_positions(marginCoin=marginCoin,productType=productType)

    return data["data"]

def logic_buy(clnt_bit:Client, symbol, sideType, quantity_usdt, levereage, takeProfit, stopLoss, price_place, price_end_step, volume_place):

    margin_coin = botrading_constant.MARIN_COIN
        
    try:
        size, price_coin_buy = PriceUtil.calculate_size_with_leverage(clnt_bit=clnt_bit, symbol = symbol, quantity_usdt = quantity_usdt, volume_place= volume_place, leverage = int(levereage))
        formatted_price = PriceUtil.multiple_next_price (value = size, price_place = price_place, price_end_step = price_end_step , volume_place = volume_place) #! BORRAR son PRUEBAS

        symbol = str(symbol).upper()
        print("------------------- INICIO COMPRA " + str(symbol) + "-------------------")
            
        if int(levereage) > 0:

            try:
                clnt_bit.mix_adjust_margintype(symbol=symbol, marginCoin=margin_coin, marginMode=botrading_constant.MARGIN_MODE)
                order_leverage = clnt_bit.mix_adjust_leverage(symbol=symbol, marginCoin=margin_coin, leverage=levereage, holdSide=sideType) 
                print(order_leverage)
            except Exception as e:
                print(f"Error al realizar apalancamiento {symbol}: {e}")
                #continue

        print ('symbol: ' + str(symbol))
        print ('marginCoin: ' + str(margin_coin))
        print ('size: ' + str(formatted_price))
        print ('sideType: ' + str(sideType))
        
        if takeProfit and stopLoss:
            takeProfit = PriceUtil.multiple_next_limit (takeProfit, price_place, price_end_step)
            stopLoss = PriceUtil.multiple_next_limit (stopLoss, price_place, price_end_step)
            print ('takeProfit: ' + str(takeProfit))
            print ('stopLoss: ' + str(stopLoss))
            order = clnt_bit.mix_place_order(symbol, marginCoin = margin_coin, size = formatted_price, side = 'open_' + sideType, orderType = 'market', presetTakeProfitPrice = takeProfit, presetStopLossPrice = stopLoss)
        else:
            order = clnt_bit.mix_place_order(symbol, marginCoin = margin_coin, size = formatted_price, side = 'open_' + sideType, orderType = 'market') 
                
        if order['msg'] == 'success':
            print(order)
            return order['data']
        else:
            print("------------------- ERROR AL COMPRAR "   + str(symbol) + "-------------------")

    except Exception as e:
        print(f"Error al realizar la orden de compra para {symbol}: {e}")
        print(f"------------------- ERROR AL COMPRAR {symbol} -------------------")
        TelegramNotify.notify(f"Error al realizar la orden de compra para {symbol}: {e}")
                    
    return None


def logic_sell(clnt_bit: Client, symbol, sideType, levereage, price_place, price_end_step, volume_place) -> pandas.DataFrame:

    print("------------------- INICIO VENTA  -------------------")

    quantity_usdt=None #Calcular???
    margin_coin = botrading_constant.MARIN_COIN

    size, price_coin_sell = PriceUtil.calculate_size_with_leverage(clnt_bit=clnt_bit, symbol = symbol, quantity_usdt = quantity_usdt, volume_place= volume_place, leverage = int(levereage))
    formatted_price = PriceUtil.multiple_next_price (value = size, price_place = price_place, price_end_step = price_end_step , volume_place = volume_place) #! BORRAR son PRUEBAS
                        
    symbol = str(symbol).upper()
        
    order = clnt_bit.mix_place_order(symbol, marginCoin = margin_coin, size = formatted_price, side = 'close_' + sideType, orderType = 'market') 

    if order['msg'] == 'success':
       print(order)

    else:
        print("------------------- ERROR AL VENDER "   + str(symbol) + "-------------------")

    return None


class TradingUtil:

    @staticmethod
    def precision_decimal(number):
        """
        Usage:
            NumberUtils().precision_decimal(1.101) returns '0.001'
        """
        result = 1
        decimal_part_length = TradingUtil.length_decimal_part(number)
        for cont in range(0, decimal_part_length):
            result = result / 10
        return round(result, decimal_part_length)

    @staticmethod
    def diff_precision_decimal(number):
        # ¿si number es 0, devolver valor negativo?
        if TradingUtil.extract_decimal_part(number) > 0:
            return number - TradingUtil.precision_decimal(number)
        else:
            return number - 1

    @staticmethod
    def sell_with_retries(clnt_bit: BitgetClienManager, symbol, margin_coin, price_convert_coin_to_usdt, sideType):
        max_retries = 5
        order = None

        cont = 1
        #!Reintentos
        while order is None and cont < max_retries:
            print("-------------------" + str(cont) + " REINTENTO VENTA " + symbol + " CANTIDAD " + str(price_convert_coin_to_usdt) + "-------------------")
            
            order = clnt_bit.client_bit.mix_place_order(symbol, marginCoin = margin_coin, size = price_convert_coin_to_usdt, side = 'close_' + sideType, orderType = 'market')
            print("INTENTO DE VENTA NUMERO " + str(cont) + " ORDEN " + str(order))

            if order['msg'] == 'success':
                return order

            #qty_diff = TradingUtil.diff_precision_decimal(qty)
            #qty = TradingUtil.format_qty_for_sell(clnt_bit, symbol + base_asset, price, qty_diff)
            cont += 1

        print("LA VENTA NO SE HA PODIDO REALIZAR PARA " + str(symbol))

        return None

    @staticmethod
    def format_qty_for_sell(clnt_bit: BitgetClienManager, symbol, price, qty: float):

        coin_inf = clnt_bit.get_inf_coin(symbol)

        for filter in coin_inf.filters:
            if filter.filterType == "LOT_SIZE":
                step_size = float(filter.stepSize)
                min_qty = float(filter.minQty)
            if filter.filterType == "MIN_NOTIONAL":
                min_notional = float(filter.minNotional)
            if filter.filterType == "NOTIONAL":
                min_notional = float(filter.minNotional)

        if qty < min_qty:
            qty = min_qty

        if price * qty < min_notional:
            qty = min_notional / price

        return helpers.round_step_size(qty, step_size)


    @staticmethod
    def extract_whole_part(number):
        """
        Usage:
            NumberUtils().extract_whole_part(1.101) returns '1'
        """
        if number is None:
            return 0
        else:
            return int(number)

    @staticmethod
    def extract_decimal_part(number):
        """
        Usage:
            NumberUtils().extract_decimal_part(1.101) returns '0.101'
        """
        if number is None:
            raise Exception("number is invalid")
        else:
            return round(
                number - TradingUtil.extract_whole_part(number),
                TradingUtil.length_decimal_part(number),
            )

    @staticmethod
    def length_decimal_part(number):
        """
        Usage:
            NumberUtils().length_decimal_part(1.00000000001000123) returns 17
        """
        if number == None:
            raise Exception("number is invalid")

        if str(number).find(".") >= 0:
            decimal_parte = str(number).split(".")[1]
            total_zero = str(decimal_parte).count("0")
            if len(decimal_parte) == total_zero:
                return 0
            return len(decimal_parte)
        else:
            return 0

    @staticmethod
    def convert_price_usdt_to_coin(clnt_bit: BitgetClienManager, quantity_usdt, symbol):

        # Obtener el precio actual de la moneda en USDT
        price_coin_buy = float(clnt_bit.client_bit.mix_get_single_symbol_ticker(symbol=symbol)['data']['last'])

        # Realizar la conversión de USDT a la moneda específica
        return float(quantity_usdt) / float(price_coin_buy), price_coin_buy

    @staticmethod
    def convert_price_coin_to_usdt(clnt_bit: BitgetClienManager, quantity_usdt, price_convert_coin, symbol):

        qtty_coin_buy = float(quantity_usdt) / float(price_convert_coin)

        # Obtener el precio actual de la moneda en USDT
        price_coin = float(clnt_bit.client_bit.mix_get_single_symbol_ticker(symbol=symbol)['data']['last'])

        # Realizar la conversión de USDT a la moneda específica
        return float(qtty_coin_buy) * float(price_coin), price_coin

    @staticmethod
    def multiple_closest(price, price_place, price_end_step):
        
        # Redondear el precio a la cantidad especificada por price_place
        rounded_price = round(price, price_place)

        # Calcular la cantidad de veces que price_end_step cabe en la parte decimal redondeada
        multiples = rounded_price / price_end_step

        # Redondear hacia arriba y hacia abajo para obtener los dos números más cercanos
        lower_multiple = int(multiples)
        upper_multiple = lower_multiple + 1

        # Calcular las dos opciones más cercanas
        lower_option = lower_multiple * price_end_step
        upper_option = upper_multiple * price_end_step

        # Determinar cuál de las dos opciones está más cerca del precio original
        if abs(price - lower_option) < abs(price - upper_option):
            return lower_option
        else:
            return upper_option

    def format_price(price, priceplace, priceendstep):
        """
        Formatea un precio según la cantidad de decimales especificada por priceplace,
        y sustituye la última posición decimal por priceendstep.

        Args:
        - price (float): Precio a formatear.
        - priceplace (int): Cantidad de decimales para el precio (price).
        - priceendstep (int): Nuevo valor para la última posición decimal.

        Returns:
        - formatted_price (float): Precio formateado.
        """
        # Formatear el precio según la cantidad de decimales proporcionada por priceplace
        formatted_price = round(price, priceplace)

        # Convertir el precio formateado a una cadena para manipular los dígitos
        price_str = str(formatted_price)

        # Sustituir la última posición decimal por priceendstep
        price_str = price_str[:-1] + str(priceendstep)

        # Convertir nuevamente la cadena a un número flotante
        formatted_price = float(price_str)

        return formatted_price

