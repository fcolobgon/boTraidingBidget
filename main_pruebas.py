import urllib3

from src.botrading.thread.bitget_buy_thread import BitgetBuyThreed
from src.botrading.thread.bitget_sell_thread import BitgetSellThreed
from src.botrading.model.time_ranges import *
from src.botrading.utils import excel_util
from src.botrading.bit import BitgetClienManager
from src.botrading.telegram.telegram_bot import TelegramBot
from src.botrading.strategy.strategy import Strategy

import requests
import time
import hmac
import base64
import json
from urllib.parse import urlencode

from configs.config import settings as settings

urllib3.disable_warnings()

# ----------------------------  MAIN  -----------------------------

strategy_name = "Estrategia X"
strategy = Strategy(name=strategy_name)

class BitgetFuturesClient:
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.base_url = 'https://api.bitget.com'
    
    def _generate_signature(self, method, endpoint, body='', timestamp=None):
        if timestamp is None:
            timestamp = str(int(time.time() * 1000))
        
        if method == 'GET':
            if body:
                message = timestamp + method + endpoint + '?' + body
            else:
                message = timestamp + method + endpoint
        else:
            message = timestamp + method + endpoint + body

        signature = base64.b64encode(
            hmac.new(
                self.secret_key.encode('utf-8'),
                message.encode('utf-8'),
                digestmod='sha256'
            ).digest()
        ).decode()
        
        return signature, timestamp

    def modify_stop_loss(self, symbol, stopLossPrice, planType='normal', triggerType='mark'):
        """
        Modifica un stop loss existente en futuros
        
        Args:
            symbol: Par de trading (ejemplo: 'BTCUSDT_UMCBL')
            stopLossPrice: Nuevo precio del stop loss
            planType: Tipo de orden ('normal' o 'track')
            triggerType: Tipo de precio para activar ('market' o 'mark')
        """
        endpoint = '/api/v2/mix/plan/modify-tpsl'
        method = 'POST'
        
        # Preparar el body de la petición
        body = {
            'symbol': symbol,
            'stopLossPrice': str(stopLossPrice),
            'planType': planType,
            'triggerType': triggerType
        }
        
        # Convertir el body a string
        body_str = json.dumps(body)
        
        # Generar firma
        timestamp = str(int(time.time() * 1000))
        signature, _ = self._generate_signature(method, endpoint, body_str, timestamp)
        
        # Preparar headers
        headers = {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
        
        # Realizar la petición
        response = requests.post(
            self.base_url + endpoint,
            headers=headers,
            data=body_str
        )
        
        return response.json()



if __name__ == '__main__':
    output_data = []

    urllib3.disable_warnings()
    
    excel_util.custom_init(settings.FILES_BASE_PATH)

    client_bit = BitgetClienManager(test_mode = settings.BITGET_CLIENT_TEST_MODE, api_key = settings.API_KEY_BIT, api_secret = settings.API_SECRET_BIT, api_passphrase = settings.API_PASSPHRASE_BIT)

    time_range = TimeRanges("HOUR_4") #DAY_1  HOUR_4  MINUTES_1
    hours_window_check = 10

    order_id = 1228503297044389907
    symbol = 'BTCUSDT_UMCBL'
    margin_coin = 'USDT'
    preset_stop_loss_price = 60450.8
    product_type = 'umcbl'

    client = BitgetFuturesClient(
        api_key=settings.API_KEY_BIT,
        secret_key=settings.API_SECRET_BIT,
        passphrase=settings.API_PASSPHRASE_BIT
    )


    result = client.modify_stop_loss(
        symbol='BTCUSDT_UMCBL',
        stopLossPrice='78000',  # Nuevo precio del stop loss
        planType='normal',
        triggerType='mark'
    )
    
    print(result)

    open_order = client_bit.get_open_positions(productType=product_type)

    print (open_order)

    plan = client_bit.client_bit.mix_modify_plan_order_tpsl(marginCoin=margin_coin, orderId=order_id, symbol=symbol, presetStopLossPrice=preset_stop_loss_price) 

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

