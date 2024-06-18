import pandas as pd  # For data manipulation
import numpy  # For numerical operations (consider `ta` for technical indicators)
import pandas_ta 

# Function for Positive Volume Index (PVI)
def calculate(data:pd.DataFrame) -> pd.DataFrame:
    
    length=13
    window=252
    
    close = data["Close"]
    
    # Calcular las Bandas de Bollinger
    bbands = pandas_ta.bbands(close=close, length=length)
    # Calcular el ancho de las bandas
    bb_width = (bbands['BBU_' + str(length) + '_2.0'] - bbands['BBL_' + str(length) + '_2.0']) / bbands['BBM_' + str(length) + '_2.0']

    # Calcular el BBWP
    bbwp = bb_width.rolling(window=window).apply(lambda x: percentileofscore(x, x.iloc[-1]))
    
    return bbwp

def percentileofscore(a, score):

    a = numpy.asarray(a)
    n = len(a)

    return (numpy.sum(a < score) + 0.5 * numpy.sum(a == score)) / n * 100



