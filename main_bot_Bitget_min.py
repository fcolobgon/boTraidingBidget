from src.botrading.utils import traiding_operations
from src.botrading.strategy import Strategy
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
    
    strategy = Strategy()
 
 
    run = True
    buy = False
    all_coin = bitget_client.mix_get_symbols_info(productType = productType)['data']
    
    while run:
        
        if not buy:

            for coin in all_coin:
                
                try:
                
                    baseCoin = coin['baseCoin']
                    symbol = coin['symbol']
                    print("Calculando " + str(baseCoin))
                    
                    
                    #candels = bitget_client.get_history(symbol=symbol, startTime=startTime_ms, endTime=endTime_ms, granularity=interval, kLineType='market', limit=limit)
                    candels = traiding_operations.get_history(clnt_bit=bitget_client, symbol=symbol, interval=interval)
                    
                    long, buy = strategy.buy(candels)
                    
                except Exception as e:
                    print("Error ejecutando calculos")
                    print(e)
                    all_coin = [coin for coin in all_coin if coin.get('symbol') != symbol]

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
            
        


    