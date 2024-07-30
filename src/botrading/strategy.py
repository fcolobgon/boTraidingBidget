from ta.trend import ADXIndicator
from src.botrading.utils import koncorde
from src.botrading.utils import bbwp
import numpy
import pandas_ta

class Strategy:

    volatility_min = 40
    volatility_max = 60
    adx_min = 20

    # OJO! NO TOCAR ESTE CONSTRUCTOR
    def __init__(self):
        print("Iniciando estrategia")
    
    def buy(self, candels):

        buy = False
        long = False
                    
        prices_high = candels['High']
        prices_low = candels['Low']
        prices_close = candels['Close']

        volatility = bbwp.calculate(candels)                
        volatility_2 = volatility.iloc[-2]
        volatility_1 = volatility.iloc[-1]
                    
        if volatility_2 < volatility_1 and volatility_1 > self.volatility_min and volatility_1 < self.volatility_max:
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
                           
                ao_3 = ao[-3] 
                ao_2 = ao[-2]
                ao_1 = ao[-1]
                            
                if long:
                    if ao_2 < 0 and ao_1 > 0 and adx_1 > self.adx_min:
                        buy = True
                    elif ao_2 < 0 and ao_1 > 0 and ao_3 > 0 and adx_1 > self.adx_min:
                        buy = True
                else:
                    if ao_2 > 0 and ao_1 < 0 and adx_1 > self.adx_min:
                        buy = True
                    elif ao_2 > 0 and ao_1 < 0 and ao_3 < 0 and adx_1 > self.adx_min:
                        buy = True

        return long, buy
    