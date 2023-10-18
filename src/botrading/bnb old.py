from multiprocessing.connection import wait
import platform
import pandas
import time
from datetime import datetime

from tenacity import retry, stop_after_attempt, stop_after_delay
from binance import helpers
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

from src.botrading.constants import botrading_constant
from src.botrading.model.binance_coin_model import *


class BinanceClienManager:

    retry_delay = 60
    retry_delay_attempt = 5
    bnb_client: Client
    buy_sell: bool

    # OJO! NO TOCAR ESTE CONSTRUCTOR
    def __init__(self, test_mode: bool = True, api_key:str = "", api_secret:str = ""):

        if test_mode:
            self.buy_sell = False
        else:
            self.buy_sell = True
        self.bnb_client = Client(
            api_key,
            api_secret,
            {"verify": False, "timeout": 20},
            testnet=False,
        )

        # Check OS
        my_os = platform.system()

        if my_os == "Windows":
            from src.botrading.utils.bitget_client_util import BinanceClientUtil
            
            #Synchronise Time for Win
            BinanceClientUtil.synchronise_times(self.bnb_client)
    
    

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_account_info(self) -> BinanceAccountInfo:

        account_info = self.bnb_client.get_account()
        return BinanceAccountInfo(account_info)

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_balance_for_symbol(self, asset: str) -> str:
        
        account_info = self.get_account_info()
        for balance in account_info.balances:
            if balance.asset == asset:
                return balance.free

        return None
    
    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_price_for_symbol_list(self, symbol_list: List[str]):

        return self.bnb_client.get_symbol_ticker(symbol_list)

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_price_for_symbol(self, symbol: str) -> float:
        return float(self.bnb_client.get_symbol_ticker(symbol=symbol)["price"])

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_price_for_all_symbols(self) -> List[BinanceMarketTickers]:

        tickers_dict = self.bnb_client.get_symbol_ticker()
        tickers = []
        for ticker in tickers_dict:
            ticker = BinanceMarketTickers(ticker)
            if "USDT" in ticker.symbol:
                tickers.append(ticker)
        return tickers

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_ticker_by_symbol(
        tikers: List[BinanceMarketTickers], symbol: str
    ) -> BinanceMarketTickers:

        for ticker in tikers:
            if ticker.symbol == symbol:
                return ticker
        return None

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def filter_binance_symbol_model(
        self, list_products: List[BinanceSymbolModel], symbol: str
    ) -> List[BinanceSymbolModel]:

        list_products_fitered = []

        for s in list_products:
            if s.baseAsset == symbol and s.status == "TRADING":
                list_products_fitered.append(s)

        return list_products_fitered

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def filter_binance_symbol_model_by_quote_assets(
        self, list_products: List[BinanceSymbolModel], quote_asset: str
    ) -> List[BinanceSymbolModel]:

        list_products_fitered = []

        for symbol in list_products:

            if symbol.status != "BREAK":
                if symbol.quoteAsset == quote_asset:
                    list_products_fitered.append(symbol)

        return list_products_fitered

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_all_coins_filter_quote_assets(self, quote_asset) -> List[BinanceSymbolModel]:

        bpm = BinanceProductModel(self.bnb_client.get_exchange_info())
        return self.filter_binance_symbol_model_by_quote_assets(bpm.symbols, quote_asset)

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
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

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def list_all_tickers_for_quote_asset(self, quote_asset):

        p = BinanceProductModel(self.bnb_client.get_exchange_info())

        list_tickers = self.returns_list_products_filtered_by_quote_assets(
            self.bnb_client, p.symbols, quote_asset
        )

        return list_tickers

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_historial_x_day_ago(self, symbol, x_days, interval, retry = 0, limit:int = 500) -> pandas.DataFrame:

        str_days = str(x_days) + " day ago UTC"
            
        temp_data = self.bnb_client.get_historical_klines(symbol, interval, str_days, limit=limit)
        
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

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
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

        return df_history_symbol

    @retry(stop=(stop_after_delay(retry_delay) | stop_after_attempt(retry_delay_attempt)))
    def get_inf_coin(self, symbol: str) -> BinanceSymbolModel:

        coin_inf = self.bnb_client.get_symbol_info(symbol)
        coin = BinanceSymbolModel(coin_inf)
        return coin

    def orde_buy(
        self,
        symbol,
        import_bet_usdt: float,
        pair: str = botrading_constant.PAIR_ASSET_DEFAULT,
    ) -> BinandeOrder:

        if self.buy_sell == False:
            time.sleep(2)
            symbol_price = self.get_price_for_symbol(symbol + pair)
            oder_dict = {"side": "BUY", "price": +symbol_price}
            order = BinandeOrder(oder_dict)

            return order

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
            return None

    def orde_sell_quantity(
        self, symbol, qty, pair: str = botrading_constant.PAIR_ASSET_DEFAULT
    ) -> BinandeOrder:

        order = None
        symbol_price = self.get_price_for_symbol(symbol + pair)

        if self.buy_sell == False:

            oder_dict = {"side": "SELL", "price": +symbol_price}
            order = BinandeOrder(oder_dict)

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

            return order
        except BinanceAPIException as err:
            # error handling goes here
            print(
                "ERROR SELL - symbol: "
                + str(symbol)
                + " | qty: "
                + str(qty)
                + " | BinanceAPIException e: "
                + str(err)
            )

        except BinanceOrderException as err:
            # error handling goes here
            print(
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

            return quantity_buy

        elif float(balance_for_asset) > botrading_constant.QUANTITY_BUY_MIN:
            quantity_buy = float(balance_for_asset)

            return quantity_buy
        else:
            print("------------------- NO HAY SUFICIENTE BLANCE PARA REALIZAR LA NUEVAS COMPRA -------------------")
            print("------------------- BALANCE ACTUAL " + str(balance_for_asset) + "-------------------")

        return 0

    def get_change_price_24h(self, symbol: str) -> float:

        price_24h = self.bnb_client.get_ticker(symbol=symbol)
        return float(price_24h["priceChangePercent"])
