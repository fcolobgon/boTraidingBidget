
from datetime import datetime
import pandas
import numpy
from binance import helpers

from src.botrading.bit import BitgetClienManager
from src.botrading.constants import botrading_constant
from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.data_frame_colum import ColumStateValues

from configs.config import settings as settings

def check_open_order(clnt_bit: BitgetClienManager, order_id:str) -> bool:
    
    open_orders = get_open_orders(clnt_bit=clnt_bit)
    return numpy.isin(open_orders, order_id)

def get_open_orders(clnt_bit: BitgetClienManager) -> numpy:
    
    margin_coin = settings.MARGINCOIN
    productType = settings.FUTURE_CONTRACT
    
    if settings.BITGET_CLIENT_TEST_MODE == True:
        margin_coin = 'S' + settings.MARGINCOIN
        productType = 'S' + settings.FUTURE_CONTRACT
    
    return clnt_bit.get_open_orders(marginCoin=margin_coin,productType=productType)
    
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

        try:
            price_convert_coin, price_coin_buy = TradingUtil.convert_price_usdt_to_coin(clnt_bit = clnt_bit, quantity_usdt=quantity_usdt, symbol=symbol)

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
                order = clnt_bit.client_bit.mix_place_order(symbol, marginCoin = margin_coin, size = price_convert_coin, side = 'open_' + sideType, orderType = 'market')
            else:
                order = clnt_bit.client_bit.mix_place_order(symbol, marginCoin = margin_coin, size = price_convert_coin, side = 'open_' + sideType, orderType = 'market', presetTakeProfitPrice = takeProfit, presetStopLossPrice = stopLoss)
                
            if order['msg'] == 'success':
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

