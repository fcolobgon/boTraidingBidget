import numpy
import pandas
import time
from datetime import datetime
from operator import itemgetter
from collections import namedtuple
import json
#import logging


from binance import helpers
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

from src.botrading.constants import binance_access_constant
from src.botrading.constants import botrading_constant
from src.botrading.model.binance_coin_model import *


#logger = logging.getLogger(__name__)
#logger = logger_util.definition_logger(logger)


class BinanceClienManager:

    bnb_client: Client
    buy_sell: bool

    def __init__(self, test_mode: bool = True):
        """_summary_

        Args:
            buy_sell (bool, optional): False ejecuta compras y ventas en modo DUMMY, no ejecuta ordenes reales. Defaults to True.
        """
        if test_mode:
            self.buy_sell = False
        else:
            self.buy_sell = True
        self.bnb_client = Client(
            binance_access_constant.API_KEY_BIN,
            binance_access_constant.SECRET_KEY_BIN,
            {"verify": False, "timeout": 20},
            testnet=False,
        )

    def get_account_info(self) -> BinanceAccountInfo:
        """Devuelve la información de una cuenta

        Returns:
            BinanceAccountInfo: Modelo
        """
        account_info = self.bnb_client.get_account()
        return BinanceAccountInfo(account_info)

    def get_balance_for_symbol(self, asset: str) -> str:
        """Devuelve la cantidad de una moneda para la cuenta dada

        Args:
            asset (str): Simbolo de la moneda de la que se quiere recuperar el balance, ej: BTC para bitcoin

        Returns:
            str: Cantidad de la moneda indicada en formato STRING
        """
        account_info = self.get_account_info()
        for balance in account_info.balances:
            if balance.asset == asset:
                return balance.free

        return None

    def get_price_for_symbol_list(self, symbol_list: List[str]):

        return self.bnb_client.get_symbol_ticker(symbol_list)

    def get_price_for_symbol(self, symbol: str) -> float:
        """Devuelve el precio de una moneda asociada a su par

        Args:
            symbol (str): Simbolo de la moneda y par, ej, para saber el precio de BTC en USDT -> BTCUSDT

        Returns:
            float: Precio actual de la moneda en formato FLOAT
        """
        return float(self.bnb_client.get_symbol_ticker(symbol=symbol)["price"])

    def get_price_for_all_symbols(self) -> List[BinanceMarketTickers]:
        """Devuelve el precio de una lista de monedas asociada a su par

        Args:
            symbols (List[str]): Simbolo de la moneda y par, ej, para saber el precio de BTC en USDT -> BTCUSDT

        Returns:
           List[BinanceMarketTickers]: Modelo
        """

        tickers_dict = self.bnb_client.get_symbol_ticker()
        tickers = []
        for ticker in tickers_dict:
            ticker = BinanceMarketTickers(ticker)
            if "USDT" in ticker.symbol:
                tickers.append(ticker)
        return tickers

    def get_ticker_by_symbol(
        tikers: List[BinanceMarketTickers], symbol: str
    ) -> BinanceMarketTickers:
        """Filtra sobre la lista dada el simbolo buscado

        Args:
            tikers (List[BinanceMarketTickers]): Lista de BinanceMarketTickers
            symbol (str):  Simbolo de la moneda BTC

        Returns:
            BinanceMarketTickers: Modelo
        """
        for ticker in tikers:
            if ticker.symbol == symbol:
                return ticker
        return None

    def filter_binance_symbol_model(
        self, list_products: List[BinanceSymbolModel], symbol: str
    ) -> List[BinanceSymbolModel]:
        """Filtra sobre la lista dada el simbolo buscado

        Args:
            list_products (List[BinanceSymbolModel]): Modelo
            symbol (str): Simbolo de la moneda BTC

        Returns:
            List[BinanceSymbolModel]:Modelo
        """
        list_products_fitered = []

        for s in list_products:
            if s.baseAsset == symbol and s.status == "TRADING":
                list_products_fitered.append(s)

        return list_products_fitered

    def filter_binance_symbol_model_by_quote_assets(
        self, list_products: List[BinanceSymbolModel], quote_asset: str
    ) -> List[BinanceSymbolModel]:
        """Filtra sobre la lista dada el simbolo buscado quoteAsset y STATUS distinto de BREAK

        Args:
            list_products (List[BinanceSymbolModel]): Lista de BinanceSymbolModel
            quote_asset (str): USDT para obtener los simbolos que se corresponden con este

        Returns:
            List[BinanceSymbolModel]: Modelo
        """
        list_products_fitered = []

        for symbol in list_products:
            if symbol.status == "BREAK":
                logger.info(
                    "------------------- FILTERING BREAK COINS "
                    + str(symbol.symbol)
                    + " -------------------"
                )
            else:
                if symbol.quoteAsset == quote_asset:
                    list_products_fitered.append(symbol)

        return list_products_fitered

    def get_all_coins_filter_quote_assets(self, quote_asset) -> List[BinanceSymbolModel]:

        bpm = BinanceProductModel(self.bnb_client.get_exchange_info())
        return self.filter_binance_symbol_model_by_quote_assets(bpm.symbols, quote_asset)

    def returns_list_products_filtered_by_quote_assets(
        self, list_products: List[BinanceSymbolModel], quote_asset
    ) -> pandas.DataFrame:

        list_coins = []
        list_products = self.filter_binance_symbol_model_by_quote_assets(
            list_products, quote_asset
        )

        list_tickers = []

        list_tickers_dict = self.bnb_client.get_all_tickers()

        for tickers_dict in list_tickers_dict:
            list_tickers.append(BinanceMarketTickers(tickers_dict))

        for product in list_products:
            ticker = self.get_ticker_by_symbol(list_tickers, product.symbol)
            data = [
                product.symbol,
                product.baseAsset,
                quote_asset,
                ticker.price,
                datetime.now(),
            ]
            list_coins.append(data)

        return list_coins

    def list_all_tickers_for_quote_asset(self, quote_asset):

        p = BinanceProductModel(self.bnb_client.get_exchange_info())

        list_tickers = self.returns_list_products_filtered_by_quote_assets(
            self.bnb_client, p.symbols, quote_asset
        )

        return list_tickers

    def get_historial_x_day_ago(self, symbol, x_days, interval) -> pandas.DataFrame:

        str_days = str(x_days) + " day ago UTC"
        temp_data = self.bnb_client.get_historical_klines(symbol, interval, str_days)

        df_history_symbol = pandas.DataFrame(
            temp_data,
            columns=[
                "Open time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Close time",
                "Quote asset volume",
                "Number of trades",
                "Taker buy base asset volume",
                "Taker buy quote asset volume",
                "Ignore",
            ],
        )

        # df_history_symbol.insert(column="isNew", value=isNew,  loc=1, allow_duplicates=False)

        return df_history_symbol

    def get_historial(self, symbol, query, interval) -> pandas.DataFrame:

        str_days = str(query)
        temp_data = self.bnb_client.get_historical_klines(symbol, interval, str_days)

        df_history_symbol = pandas.DataFrame(
            temp_data,
            columns=[
                "Open time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Close time",
                "Quote asset volume",
                "Number of trades",
                "Taker buy base asset volume",
                "Taker buy quote asset volume",
                "Ignore",
            ],
        )

        # df_history_symbol.insert(column="isNew", value=isNew,  loc=1, allow_duplicates=False)

        return df_history_symbol

    def get_inf_coin(self, symbol: str) -> BinanceSymbolModel:
        """Devuelve toda la informació para una moneda y su par

        Args:
            symbol (str): Simbolo de la moneda y par, ej,  BTC en USDT -> BTCUSDT

        Returns:
            BinanceSymbolModel: Modelo
        """
        coin_inf = self.bnb_client.get_symbol_info(symbol)
        coin = BinanceSymbolModel(coin_inf)
        return coin

    def orde_buy(
        self,
        symbol,
        import_bet_usdt: float,
        pair: str = botrading_constant.PAIR_ASSET_DEFAULT,
    ) -> BinandeOrder:
        """Ejecuta una orden de compra.

        Si en el constructor se indicó  buy_sell=False, no se ejecutarán ordenes de comprar real (Ver documentacion de constructor)

        Args:
            symbol (_type_): Simbolo de la moneda a comprar, ej BTC
            import_bet_usdt (float): Cantidad que se quiere comprar
            pair (str, optional): Par para la ejecución de la compra, ej. USDT. Defaults to botrading_constant.PAIR_ASSET_DEFAULT.

        Returns:
            BinandeOrder: Modelo
        """
        if self.buy_sell == False:
            time.sleep(2)
            symbol_price = self.get_price_for_symbol(symbol + pair)
            oder_dict = {"side": "BUY", "price": +symbol_price}
            order = BinandeOrder(oder_dict)
            logger.info("Compra modo MOCK " + str(order))

            return order

        logger.info("------------------- CREANDO ORDER DE COMPRA -------------------")

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
            logger.debug("order: " + str(order))
            logger.info("------------------- FIN BUY -------------------")
            return order

        except BinanceAPIException as err:
            # error handling goes here
            logger.debug(
                "ERROR BUY - symbol: "
                + str(symbol)
                + " | price: "
                + str(price)
                + " | rounded_quantity: "
                + str(rounded_quantity)
                + " | BinanceAPIException e: "
                + str(err)
            )
            logger.info("------------------- FIN BUY -------------------")
            return None
        except BinanceOrderException as err:
            # error handling goes here
            logger.debug(
                "ERROR BUY - symbol: "
                + str(symbol)
                + " | price: "
                + str(price)
                + " | rounded_quantity: "
                + str(rounded_quantity)
                + " | BinanceAPIException e: "
                + str(err)
            )
            logger.info("------------------- FIN BUY -------------------")
            return None

    def orde_sell_quantity(
        self, symbol, qty, pair: str = botrading_constant.PAIR_ASSET_DEFAULT
    ) -> BinandeOrder:
        """Ejecuta una orden de venta de la cantidad indicada

        Args:
            symbol (_type_): Simbolo de la moneda a comprar, ej BTC
            qty (_type_): Cantidad que se va a vender
            pair (str, optional): Par para la ejecución de la compra, ej. USDT. Defaults to botrading_constant.PAIR_ASSET_DEFAULT.

        Returns:
            BinandeOrder: Modelo
        """
        order = None
        symbol_price = self.get_price_for_symbol(symbol + pair)

        if self.buy_sell == False:
            # symbol_price =  self.get_price_for_symbol(symbol + pair)
            oder_dict = {"side": "SELL", "price": +symbol_price}
            order = BinandeOrder(oder_dict)
            logger.info("Venta modo MOCK " + str(order))
            return order

        try:
            order_dic = self.bnb_client.create_order(
                symbol=symbol + pair,
                side=self.bnb_client.SIDE_SELL,
                type=self.bnb_client.ORDER_TYPE_MARKET,
                quantity=qty,
            )
            order = BinandeOrder(order_dic)
            order.price = symbol_price
            logger.debug("order: " + str(order))
            logger.info("------------------- FIN SELL -------------------")
            return order
        except BinanceAPIException as err:
            # error handling goes here
            logger.debug(
                "ERROR SELL - symbol: "
                + str(symbol)
                + " | qty: "
                + str(qty)
                + " | BinanceAPIException e: "
                + str(err)
            )

        except BinanceOrderException as err:
            # error handling goes here
            logger.debug(
                "ERROR SELL - symbol: "
                + str(symbol)
                + " | qty: "
                + str(qty)
                + " | BinanceAPIException e: "
                + str(err)
            )

    def has_balance_for_buy(
        self, quantity_buy: float = botrading_constant.QUANTITY_BUY_ORDER
    ) -> float:

        balance_for_asset = self.get_balance_for_symbol(botrading_constant.QUOTE_ASSET)

        if float(balance_for_asset) > quantity_buy:
            quantity_buy = quantity_buy
            logger.info(
                "------------------- BALANCE ACTUAL "
                + str(balance_for_asset)
                + "-------------------"
            )
            logger.info(
                "------------------- CANTIDAD DE COMPRA "
                + str(quantity_buy)
                + "-------------------"
            )
            return quantity_buy
        elif float(balance_for_asset) > botrading_constant.QUANTITY_BUY_MIN:
            quantity_buy = float(balance_for_asset)
            logger.info(
                "------------------- BALANCE ACTUAL "
                + str(balance_for_asset)
                + "-------------------"
            )
            logger.info(
                "------------------- CANTIDAD DE COMPRA "
                + str(quantity_buy)
                + "-------------------"
            )
            return quantity_buy
        else:
            logger.info(
                "------------------- NO HAY SUFICIENTE BLANCE PARA REALIZAR LA NUEVAS COMPRA -------------------"
            )
            logger.info(
                "------------------- BALANCE ACTUAL "
                + str(balance_for_asset)
                + "-------------------"
            )
            logger.info(
                "------------------- CANTIDAD DE COMPRA SOLICITADA "
                + str(quantity_buy)
                + "-------------------"
            )

        return 0

    def get_change_price_24h(self, symbol: str) -> float:

        price_24h = self.bnb_client.get_ticker(symbol=symbol)
        return float(price_24h["priceChangePercent"])
