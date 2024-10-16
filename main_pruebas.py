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


    order_id = 1228503297044389907
    symbol = 'BTCUSDT_UMCBL'
    margin_coin = 'USDT'
    preset_stop_loss_price = 60450.8
    product_type = 'umcbl'

    open_order = client_bit.get_open_positions(productType=product_type)

    print (open_order)

    plan = client_bit.client_bit.mix_place_PositionsTPSL(,marginCoin=margin_coin, orderId=order_id, symbol=symbol, presetStopLossPrice=preset_stop_loss_price) 

    order = client_bit.client_bit.mix_modify_(orderId=order_id, symbol=symbol, marginCoin = margin_coin, presetStopLossPrice=preset_stop_loss_price) 

    print (order)



    # Obtener el precio actual de la moneda en USDT
    precio_entrada = float(client_bit.client_bit.mix_get_single_symbol_ticker(symbol="BTCUSDT_UMCBL")['data']['last'])

    apuesta_usdt= 19
    apalancamiento = 5

    cantidad = float(apuesta_usdt / (precio_entrada * apalancamiento))
    costo2 = cantidad * precio_entrada * apalancamiento
    size = (apuesta_usdt / precio_entrada) * apalancamiento

    print (format(cantidad, '.5f'))
    print (costo)
    print (costo_total)
    print (costo2)
    print (size)

