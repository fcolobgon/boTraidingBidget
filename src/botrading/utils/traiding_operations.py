
from datetime import datetime
import pandas
from binance import helpers

from src.botrading.bnb import BinanceClienManager
from src.botrading.constants import botrading_constant
from src.botrading.utils import excel_util
from src.botrading.model.binance_coin_model import *
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.data_frame_colum import ColumStateValues


def logic_buy(
    clnt_bnb: BinanceClienManager,
    df_buy,
    quantity_buy: int,
):

    for ind in df_buy.index:
        symbol = df_buy[DataFrameColum.BASE.value][ind]
        print(
            "------------------- INICIO COMPRA " + str(symbol) + "-------------------"
        )
        has_balance = True
        order = None

        # TODO: Calcular el balance para comprar con la siguiente función
        # if clnt_bnb.buy_sell == True:
        #    buy_balance = quantity_buy

        # buy_balance =  self.bnb_client.has_balance_for_buy()
        # if buy_balance == 0:
        #    return df_buy

        balance_for_asset = clnt_bnb.get_balance_for_symbol(
            botrading_constant.QUOTE_ASSET
        )

        if clnt_bnb.buy_sell == True:
            if float(balance_for_asset) > quantity_buy:
                quantity_buy = quantity_buy
            elif float(balance_for_asset) > botrading_constant.QUANTITY_BUY_MIN:
                quantity_buy = botrading_constant.QUANTITY_BUY_MIN
            else:
                has_balance = False
                print("------------------- NO HAY SUFICIENTE BLANCE PARA REALIZAR LA COMPRA COMPRA "+ str(symbol)+ "-------------------")
                print("------------------- BALANCE ACTUAL "+ str(balance_for_asset)+ "-------------------")
                print("------------------- CANTIDAD DE COMPRA SOLICITADA "+ str(quantity_buy)+ "-------------------")

        if has_balance:

            print("------------------- BALANCE ACTUAL "+ str(balance_for_asset)+ "-------------------")
            print("------------------- CANTIDAD DE COMPRA SOLICITADA "+ str(quantity_buy)+ "-------------------")
            order = clnt_bnb.orde_buy(symbol, quantity_buy)

            if order is None:
                print("------------------- ERRO AL COMPRAR "   + str(symbol) + "-------------------")
            elif order.side == "BUY":
                df_buy[DataFrameColum.STATE.value][ind] = ColumStateValues.BUY.value
                df_buy[DataFrameColum.PRICE_BUY.value][ind] = order.price
                df_buy[DataFrameColum.DATE.value][ind] = datetime.now()

    excel_util.save_buy_file(df_buy)

    return df_buy


def logic_sell(clnt_bnb: BinanceClienManager, df_sell:pandas.DataFrame) -> pandas.DataFrame:

    print("------------------- INICIO VENTA  -------------------")

    for ind in df_sell.index:

        symbol = df_sell[DataFrameColum.BASE.value][ind]

        order = TradingUtil.sell_with_retries(clnt_bnb, symbol, botrading_constant.PAIR_ASSET_DEFAULT)

        if order is None:
            df_sell[DataFrameColum.STATE.value][ind] = ColumStateValues.ERR_SELL.value
        else:
            df_sell[DataFrameColum.STATE.value][ind] = ColumStateValues.SELL.value
            df_sell[DataFrameColum.PRICE_SELL.value][ind] = order.price

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
    def sell_with_retries(
        clnt_bnb: BinanceClienManager, symbol: str, base_asset: str
    ) -> BinandeOrder:
        max_retries = 5
        order = None
        qntty_assent = clnt_bnb.get_balance_for_symbol(symbol) 
        #! Este es el valor que debemos tocar para ganar más precisión en la venta. Debemo sumarle precicion_decimal()
        #qntty_assent = qntty_assent + (TradingUtil.precision_decimal(qntty_assent)*2)
        price = clnt_bnb.get_price_for_symbol(symbol + base_asset) 
        qty = TradingUtil.format_qty_for_sell(clnt_bnb, symbol + base_asset, price, float(qntty_assent))

        cont = 1
        #!Reintentos
        while order is None and cont < max_retries:
            print("-------------------" + str(cont) + " REINTENTO VENTA " + symbol + " CANTIDAD " + qntty_assent + "-------------------")
            
            order = clnt_bnb.orde_sell_quantity(symbol, qty, base_asset)
            print("INTENTO DE VENTA NUMERO " + str(cont) + " ORDEN " + str(order))

            if order:
                return order

            qty_diff = TradingUtil.diff_precision_decimal(qty)
            qty = TradingUtil.format_qty_for_sell(clnt_bnb, symbol + base_asset, price, qty_diff)
            cont += 1

        print("LA VENTA NO SE HA PODIDO REALIZAR PARA " + str(symbol))

        return None

    @staticmethod
    def format_qty_for_sell(clnt_bnb: BinanceClienManager, symbol, price, qty: float):

        coin_inf = clnt_bnb.get_inf_coin(symbol)

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
