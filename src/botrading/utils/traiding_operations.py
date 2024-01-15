
from datetime import datetime
import pandas
import numpy
from binance import helpers

from src.botrading.bit import BitgetClienManager
from src.botrading.constants import botrading_constant
from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.price_util import PriceUtil
import math

from configs.config import settings as settings

def get_open_orders(clnt_bit: BitgetClienManager, startTime:datetime) -> pandas.DataFrame:
    
    productType = settings.FUTURE_CONTRACT
    
    if settings.BITGET_CLIENT_TEST_MODE == True:
        productType = 'S' + settings.FUTURE_CONTRACT

    orders = clnt_bit.get_orders_history(productType=productType, startTime=startTime)

    return orders

def get_open_positions(clnt_bit: BitgetClienManager) -> pandas.DataFrame:
    
    productType = settings.FUTURE_CONTRACT
    
    if settings.BITGET_CLIENT_TEST_MODE == True:
        productType = 'S' + settings.FUTURE_CONTRACT

    positions = clnt_bit.get_open_positions(productType=productType)

    return positions
    
"""_summary_
    sideType: short o long
"""
def logic_buy(clnt_bit: BitgetClienManager, df_buy, quantity_usdt: int):

    for ind in df_buy.index:
        symbol = df_buy.loc[ind,DataFrameColum.SYMBOL.value]        
        sideType = str(df_buy.loc[ind, DataFrameColum.SIDE_TYPE.value])
        levereage = str(df_buy.loc[ind,DataFrameColum.LEVEREAGE.value])
        percentage_profit_flag = df_buy.loc[ind,DataFrameColum.PERCENTAGE_PROFIT_FLAG.value]
        takeProfit = str(df_buy.loc[ind,DataFrameColum.TAKE_PROFIT.value])
        stopLoss = str(df_buy.loc[ind,DataFrameColum.STOP_LOSS.value])
        margin_coin = settings.MARGINCOIN
        price_place = int(df_buy.loc[ind,DataFrameColum.PRICEPLACE.value])
        price_end_step = int(df_buy.loc[ind,DataFrameColum.PRICEENDSTEP.value])
        volume_place = int(df_buy.loc[ind,DataFrameColum.VOLUMEPLACE.value])

        try:
            size, price_coin_buy = TradingUtil.calculate_size_with_leverage(clnt_bit=clnt_bit, symbol = symbol, quantity_usdt = quantity_usdt, leverage = int(levereage))

            #Solo se ejecuta en para modo TEST
            if settings.BITGET_CLIENT_TEST_MODE == True:
                margin_coin = 'S' + settings.MARGINCOIN
                baseCoin = 'S' +  df_buy.loc[ind,DataFrameColum.BASE.value]
                mode = 'S' + settings.FUTURE_CONTRACT
                symbol = baseCoin + margin_coin + "_" + mode

            symbol = str(symbol).upper()
            print("------------------- INICIO COMPRA " + str(symbol) + "-------------------")
            
            if int(levereage) > 0:

                try:
                    clnt_bit.client_bit.mix_adjust_margintype(symbol=symbol, marginCoin=margin_coin, marginMode=settings.MARGIN_MODE)
                    order_leverage = clnt_bit.client_bit.mix_adjust_leverage(symbol=symbol, marginCoin=margin_coin, leverage=levereage, holdSide=sideType) 
                    print(order_leverage)
                except Exception as e:
                    print(f"Error al realizar apalancamiento {symbol}: {e}")
                    #continue
        
            if percentage_profit_flag:
                print ('--------------------------------------------------------------')
                print ('price_place: ' + str(price_place))
                print ('price_end_step: ' + str(price_end_step))
                print ('volume_place: ' + str(volume_place))
                print ('--------------------------------------------------------------')

                print ('symbol: ' + str(symbol))
                print ('marginCoin: ' + str(margin_coin))
                print ('size: ' + str(size))
                print ('sideType: ' + str(sideType))
                print ('takeProfit: ' + str(takeProfit))
                print ('stopLoss: ' + str(stopLoss))

                
                multiplier = float(f"{price_place:.{price_end_step}f}")
                
                takeProfit = float(takeProfit)
                takeProfit = TradingUtil.format_price(takeProfit, price_place, price_end_step)
                #print(takeProfit)
                stopLoss = float(stopLoss)
                stopLoss = TradingUtil.format_price(stopLoss, price_place, price_end_step)
                #print(stopLoss)

                print ('symbol: ' + str(symbol))
                print ('marginCoin: ' + str(margin_coin))
                print ('size: ' + str(size))
                print ('sideType: ' + str(sideType))
                print ('takeProfit: ' + str(takeProfit))
                print ('stopLoss: ' + str(stopLoss))


                order = clnt_bit.client_bit.mix_place_order(symbol, marginCoin = margin_coin, size = size, side = 'open_' + sideType, orderType = 'market', presetTakeProfitPrice = takeProfit, presetStopLossPrice = stopLoss)
            else:
                order = clnt_bit.client_bit.mix_place_order(symbol, marginCoin = margin_coin, size = size, side = 'open_' + sideType, orderType = 'market') 
                
            if order['msg'] == 'success':
                print(order)
                orderInfo = order['data']
                df_buy.loc[ind,DataFrameColum.STATE.value] = ColumStateValues.BUY.value
                df_buy.loc[ind,DataFrameColum.MONEY_SPENT.value] = quantity_usdt   
                df_buy.loc[ind,DataFrameColum.SIZE.value] = price_coin_buy
                df_buy.loc[ind,DataFrameColum.ORDER_OPEN.value] = True
                df_buy.loc[ind,DataFrameColum.ORDER_ID.value] = orderInfo['orderId']
                df_buy.loc[ind,DataFrameColum.CLIENT_ORDER_ID.value] = orderInfo['clientOid'] #! clientOrderId
                df_buy.loc[ind,DataFrameColum.DATE.value] = datetime.now()
            else:
                print("------------------- ERRO AL COMPRAR "   + str(symbol) + "-------------------")

        except Exception as e:
            print(f"Error al realizar la orden de compra para {symbol}: {e}")
            df_buy[DataFrameColum.STATE.value][ind] = ColumStateValues.ERR_BUY.value
            df_buy[DataFrameColum.DATE.value][ind] = datetime.now()

            print(f"------------------- ERROR AL COMPRAR {symbol} -------------------")
            continue  # Continuar con la próxima iteración del bucle sin detenerse

    excel_util.save_buy_file(df_buy)
        
    return df_buy


def logic_sell(clnt_bit: BitgetClienManager, df_sell:pandas.DataFrame) -> pandas.DataFrame:

    print("------------------- INICIO VENTA  -------------------")

    for ind in df_sell.index:

        symbol = df_sell.loc[ind,DataFrameColum.SYMBOL.value]
        sideType = str(df_sell.loc[ind, DataFrameColum.SIDE_TYPE.value])
        quantity_usdt = df_sell.loc[ind,DataFrameColum.MONEY_SPENT.value] 
        price_coin_buy = df_sell.loc[ind,DataFrameColum.SIZE.value]
        margin_coin = settings.MARGINCOIN

        #Solo se ejecuta en para modo TEST
        if settings.BITGET_CLIENT_TEST_MODE == True:
            margin_coin = 'S' + settings.MARGINCOIN
            baseCoin = 'S' +  df_sell.loc[ind,DataFrameColum.BASE.value]
            mode = 'S' + settings.FUTURE_CONTRACT
            symbol = baseCoin + margin_coin + "_" + mode

        price_convert_coin_to_usdt, price_coin = TradingUtil.convert_price_coin_to_usdt(clnt_bit = clnt_bit, quantity_usdt=quantity_usdt, price_convert_coin = price_coin_buy, symbol=symbol)

        order = TradingUtil.sell_with_retries(clnt_bit, symbol, margin_coin, price_convert_coin_to_usdt, sideType)

        if order['msg'] == 'success':
            df_sell[DataFrameColum.STATE.value][ind] = ColumStateValues.SELL.value
            df_sell[DataFrameColum.ORDER_OPEN.value] = False
            df_sell[DataFrameColum.PRICE_SELL.value][ind] = price_coin
            df_sell[DataFrameColum.ORDER_ID.value] = "-"
            df_sell[DataFrameColum.CLIENT_ORDER_ID.value] = "-"
        else:
            df_sell[DataFrameColum.STATE.value][ind] = ColumStateValues.ERR_SELL.value

    excel_util.save_sell_file(df_sell)

    return df_sell


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
    def calculate_size_with_leverage(clnt_bit: BitgetClienManager, symbol, quantity_usdt, leverage):

        price_coin = float(clnt_bit.client_bit.mix_get_single_symbol_ticker(symbol=symbol)['data']['last'])
        size = (quantity_usdt / price_coin) * leverage

        return size, price_coin
    
    def multiple_closest(price, z):
        """
        Calcula el múltiplo más cercano de un valor X con un múltiplo Z.

        Args:
            x: El valor a calcular.
            z: El múltiplo a calcular.

        Returns:
            El múltiplo más cercano de x con z.
        """

        # Calculamos la diferencia entre x y el múltiplo anterior de z.
        difference = price - (price // z) * z

        # Si la diferencia es menor que la mitad de z, entonces x es el múltiplo más cercano.
        if difference < z / 2:
            return price

        # De lo contrario, el múltiplo más cercano es x - difference.
        return price - difference

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

