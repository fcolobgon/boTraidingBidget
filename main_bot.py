import urllib3

from src.botrading.thread.binance_buy_thread import BinanceBuyThreed
from src.botrading.thread.binance_sell_thread import BinanceSellThreed
from src.botrading.model.time_ranges import *
from src.botrading.utils import excel_util
from src.botrading.bnb import BinanceClienManager
from src.botrading.telegram.telegram_bot import TelegramBot
from src.botrading.strategy.strategy import Strategy

from configs.config import settings as settings

urllib3.disable_warnings()

# ----------------------------  MAIN  -----------------------------

strategy_name = "Estrategia X"
strategy = Strategy(name=strategy_name)

if __name__ == '__main__':
    output_data = []

    urllib3.disable_warnings()
    
    excel_util.custom_init(settings.FILES_BASE_PATH)

    bnb_client = BinanceClienManager(test_mode = settings.BINANCE_CLIENT_TEST_MODE, api_key = settings.BO_TRADING_API_KEY_BIN, api_secret = settings.BO_TRADING_SECRET_KEY_BIN)

    print("### START MAIN ###")

    binance_buy_threed = BinanceBuyThreed(bnb_client = bnb_client, strategy = strategy, max_coin_buy = settings.MAX_COIN_BUY, quantity_buy_order = settings.QUANTITY_BUY_ORDER, load_from_previous_execution = settings.LOAD_FROM_PREVIOUS_EXECUTION, observe_coin_list=settings.OBSERVE_COIN_LIST, remove_coin_list=settings.REMOVE_COIN_LIST)
    binance_buy_threed.start()
    
    binance_buy_threed.wait_buy_thread_ready()
    
    binance_sell_threed = BinanceSellThreed(bnb_client = bnb_client, strategy = strategy, buy_thread = binance_buy_threed)
    binance_sell_threed.start()

    bnb_market_client = BinanceClienManager(api_key = settings.MARKET_API_KEY_BIN, api_secret = settings.MARKET_SECRET_KEY_BIN)
    
    telegram_bot = TelegramBot(bnb_client = bnb_client, buy_thread = binance_buy_threed, sell_thread = binance_sell_threed, base_path = settings.FILES_BASE_PATH, bot_token = settings.TELEGRAM_BOT_TOKEN)
    telegram_bot.start()
    