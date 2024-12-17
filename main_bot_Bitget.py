import urllib3

from src.botrading.thread.bitget_buy_thread import BitgetBuyThreed
from src.botrading.thread.bitget_sell_thread import BitgetSellThreed
from src.botrading.model.time_ranges import *
from src.botrading.utils import excel_util
from src.botrading.bit import BitgetClienManager
from src.botrading.telegram.telegram_bot import TelegramBot
from src.botrading.strategy.strategy import Strategy

from configs.config import settings as settings

urllib3.disable_warnings()


def divide_and_round_to_multiple_of_5( value: int, divide_num: int = 2) -> int:
    # Dividir el valor entre 3 y redondear al entero más cercano
    divided_value = round(value / divide_num)
    # Ajustar al múltiplo de 5 más cercano
    multiple_of_5 = round(divided_value / 5) * 5
    
    return multiple_of_5
    

# ----------------------------  MAIN  -----------------------------

strategy_name = "Estrategia X"
strategy = Strategy(name=strategy_name)

if __name__ == '__main__':
    output_data = []

    urllib3.disable_warnings()
    
    excel_util.custom_init(settings.FILES_BASE_PATH)

    client_bit = BitgetClienManager(test_mode = settings.BITGET_CLIENT_TEST_MODE, api_key = settings.API_KEY_BIT, api_secret = settings.API_SECRET_BIT, api_passphrase = settings.API_PASSPHRASE_BIT)

    print("### START MAIN ###")

    open_order = client_bit.client_bit.mix_get_accounts(productType=settings.FUTURE_CONTRACT) 

    if settings.QUANTITY_BUY_ORDER == 0:
        qtty_float = float(open_order['data'][0]['available']) 
        qtty_buy = divide_and_round_to_multiple_of_5(value = int(qtty_float), divide_num = settings.DIVIDE_AVAILABLE)
    else:
        qtty_buy = settings.QUANTITY_BUY_ORDER

    bitget_buy_threed = BitgetBuyThreed(client_bit = client_bit, strategy = strategy, max_coin_buy = settings.MAX_COIN_BUY, quantity_buy_order = qtty_buy, load_from_previous_execution = settings.LOAD_FROM_PREVIOUS_EXECUTION, observe_coin_list=settings.OBSERVE_COIN_LIST, remove_coin_list=settings.REMOVE_COIN_LIST)
    bitget_buy_threed.start()
    bitget_buy_threed.wait_buy_thread_ready()

    bitget_sell_threed = BitgetSellThreed(client_bit = client_bit, strategy = strategy, buy_thread = bitget_buy_threed)
    bitget_sell_threed.start()

    """
    bit_market_client = BitgetClienManager(api_key = settings.MARKET_API_KEY_BIN, api_secret = settings.MARKET_SECRET_KEY_BIN)
    """
    #telegram_bot = TelegramBot(bit_client = client_bit, buy_thread = bitget_buy_threed, sell_thread = bitget_sell_threed, base_path = settings.FILES_BASE_PATH, bot_token = settings.TELEGRAM_BOT_TOKEN)
    #telegram_bot.start()



