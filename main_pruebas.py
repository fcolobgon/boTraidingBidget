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

# ----------------------------  MAIN  -----------------------------

strategy_name = "Estrategia X"
strategy = Strategy(name=strategy_name)

if __name__ == '__main__':
    output_data = []

    urllib3.disable_warnings()
    
    excel_util.custom_init(settings.FILES_BASE_PATH)

    client_bit = BitgetClienManager(test_mode = settings.BITGET_CLIENT_TEST_MODE, api_key = settings.API_KEY_BIT, api_secret = settings.API_SECRET_BIT, api_passphrase = settings.API_PASSPHRASE_BIT)

    # Obtener el precio actual de la moneda en USDT
    precio_entrada = float(client_bit.client_bit.mix_get_single_symbol_ticker(symbol="BTCUSDT_UMCBL")['data']['last'])

    apuesta_usdt= 20
    apalancamiento = 10

    cantidad = float(apuesta_usdt / (precio_entrada * apalancamiento))
    costo = cantidad * precio_entrada
    costo_total = apuesta_usdt * precio_entrada * apalancamiento

    print (format(cantidad, '.5f'))
    print (costo)
    print (costo_total)

