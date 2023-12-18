from multiprocessing.connection import wait
import platform
import pandas
import time
from datetime import datetime

from tenacity import retry, stop_after_attempt, stop_after_delay
from binance import helpers
from pybitget import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

from src.botrading.constants import botrading_constant


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
    
    def orde_buy_for_market(self,symbol,marginCoin: str = botrading_constant.PAIR_ASSET_DEFAULT, size: float = 0):

        self.client_bit.mix_place_order(symbol,marginCoin, size, side= 'Buy',orderType = 'market', price='')

        """
        symbol: El símbolo del activo subyacente.
        marginCoin: La moneda de margen utilizada para la orden.
        size: El tamaño de la orden.
        side: El lado de la orden, ya sea "buy" o "sell".
        orderType: El tipo de orden, ya sea "market", "limit", "stop_loss", o "take_profit".
        price: El precio de la orden, si se trata de una orden limitada, de stop loss, o de take profit.
        clientOrderId: Un ID de orden único proporcionado por el usuario.
        reduceOnly: Un booleano que indica si la orden solo debe reducir la posición existente.
        timeInForceValue: El tipo de orden de tiempo en vigor, ya sea "normal", "ioc", o "fok".
        presetTakeProfitPrice: El precio de activación de la orden de take profit.
        presetStopLossPrice: El precio de activación de la orden de stop loss.
        """




        coin_inf = self.get_inf_coin(symbol + pair)
        quote_prec = coin_inf.quoteAssetPrecision

        for filter in coin_inf.filters:
            if filter.filterType == "LOT_SIZE":
                tick_size = float(filter.stepSize)

        price = self.get_price_for_symbol(symbol + pair)

        result_quantity = import_bet_usdt / price
        buy_quantity = "{:0.0{}f}".format(result_quantity, quote_prec)
        fquantity = float(buy_quantity)
        rounded_quantity = helpers.round_step_size(fquantity, tick_size)

        try:
            order_dic = self.bnb_client.create_order(
                symbol=symbol + pair,
                side=self.bnb_client.SIDE_BUY,
                type=self.bnb_client.ORDER_TYPE_MARKET,
                quantity=rounded_quantity,
            )
            order = BinandeOrder(order_dic)
            order.price = price
            #logger.debug("order: " + str(order))
            #logger.info("------------------- FIN BUY -------------------")
            return order

        except BinanceAPIException as err:
            # error handling goes here
            print(
                "ERROR BUY - symbol: "
                + str(symbol)
                + " | price: "
                + str(price)
                + " | rounded_quantity: "
                + str(rounded_quantity)
                + " | BinanceAPIException e: "
                + str(err)
            )
            #logger.info("------------------- FIN BUY -------------------")
            return None
        except BinanceOrderException as err:
            # error handling goes here
            print(
                "ERROR BUY - symbol: "
                + str(symbol)
                + " | price: "
                + str(price)
                + " | rounded_quantity: "
                + str(rounded_quantity)
                + " | BinanceAPIException e: "
                + str(err)
            )
            #logger.info("------------------- FIN BUY -------------------")
            return None
