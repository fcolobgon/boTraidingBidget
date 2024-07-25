from ta.trend import ADXIndicator
from src.botrading.utils import koncorde
from src.botrading.utils import bbwp
import pandas_ta
import numpy
from src.botrading.utils import traiding_operations
from datetime import datetime, timedelta
from pybitget import Client
from src.botrading.constants import botrading_constant
from src.botrading.telegram.telegram_notify import TelegramNotify

# ----------------------------  MAIN  -----------------------------

if __name__ == '__main__':
    output_data = []    

    bitget_client = Client(api_key=botrading_constant.API_KEY_BIT, api_secret_key=botrading_constant.API_SECRET_BIT, passphrase=botrading_constant.API_PASSPHRASE_BIT,use_server_time=False)

    print("### START MAIN ###")
    buy_quantity=80
    levereage=10
    takeProfit=None
    stopLoss=None
    productType = botrading_constant.FUTURE_CONTRACT_USDT_UMCBL
    
    limit=1000
    x_days=100
    interval="1H"
    volatility_min = 40
    volatility_max = 60
    adx_min = 20
 
    run = True
    buy = False
    all_coin = bitget_client.mix_get_symbols_info(productType = productType)['data']
    
    while run:
        
        if not buy:
            print
            for coin in all_coin:
                
                try:
                
                    baseCoin = coin['baseCoin']
                    symbol = coin['symbol']
                    print("Calculando " + str(baseCoin))
                    
                    
                    #candels = bitget_client.get_history(symbol=symbol, startTime=startTime_ms, endTime=endTime_ms, granularity=interval, kLineType='market', limit=limit)
                    candels = traiding_operations.get_history(clnt_bit=bitget_client, symbol=symbol, interval=interval)
                    
                    long = False
                    
                    prices_high = candels['High']
                    prices_low = candels['Low']
                    prices_close = candels['Close']

                    volatility = bbwp.calculate(candels)                
                    volatility_2 = volatility.iloc[-2]
                    volatility_1 = volatility.iloc[-1]
                    
                    if volatility_2 < volatility_1 and volatility_1 > volatility_min and volatility_1 < volatility_max:
                        print("Volatilidad " + str(volatility_1))
                        adx = numpy.array(ADXIndicator(high = prices_high, low = prices_low, close = prices_close, window= 14).adx())
                        
                        adx_2 = adx[-2]
                        adx_1 = adx[-1]
                        
                        if adx_2 < adx_1:
                            
                            koncorde_df = koncorde.calculate(data=candels)

                            azul_1 = koncorde_df['azul'].iloc[-1]
                            verde_1 = koncorde_df['verde'].iloc[-1]
                            media_1 = koncorde_df['media'].iloc[-1]

                            # LONG
                            if (azul_1 > media_1):
                                if (verde_1 < media_1):
                                    long = True
                            
                            if (verde_1 > media_1):
                                if (azul_1 < media_1):
                                    long = True
                            
                            # SHORT
                            if (azul_1 < media_1) and (verde_1 < media_1):
                                long = False
                                
                            ao = numpy.array(pandas_ta.ao(high = prices_high, low = prices_low))
                            
                            ao_2 = ao[-2]
                            ao_1 = ao[-1]
                            
                            if long:
                                if ao_2 < 0 and ao_1 > 0 and adx_1 > adx_min:
                                    buy = True
                                    break
                            else:
                                if ao_2 > 0 and ao_1 < 0 and adx_1 > adx_min:
                                    buy = True
                                    break
                except Exception as e:
                    print("Error ejecutando calculos")
                    continue
                
        if buy:
            
            if long:
                sideType = "long"
            else:
                sideType = "short"
                            
            price_place  = coin['pricePlace']
            price_end_stepe  = coin['priceEndStep'] 
            volume_placee  = coin['volumePlace']
            
            compra_msg = "Comprar " +str(symbol) + " en " + str(sideType)
            print(compra_msg)
            TelegramNotify.notify(compra_msg)
            order = traiding_operations.logic_buy(clnt_bit=bitget_client, 
                                          symbol=symbol, 
                                          sideType=sideType,
                                          quantity_usdt=buy_quantity,
                                          levereage=levereage,
                                          takeProfit=takeProfit,
                                          stopLoss=stopLoss
                                          )
            print("ORDEN DE COMPRA")
            print(order)
            
            while buy:
                
                positions = traiding_operations.get_open_positions(clnt_bit=bitget_client)
                
                print("POSICIONES ABIERTAS")
                print(positions)
                
                if not positions:
                    TelegramNotify.notify("Venta realizada")
                    buy = False
            
        


    