import pandas as pd  # For data manipulation
import numpy as np  # For numerical operations (consider `ta` for technical indicators)
import pandas_ta 

# Function for Positive Volume Index (PVI)
def calc_pvi(data):
    
    volume = np.array(data["Volume"])
    close = np.array(data["Close"])
    
    sval = volume[-1]
    pvi = []

    for i in range(1, len(volume)):
        if volume[i] > volume[i - 1]:
            if i == 1:
                prev_pvi = sval
            else:
                prev_pvi = pvi[-1]
            pvi_actual = prev_pvi + (close[i] - close[i - 1]) / close[i - 1] * prev_pvi
            pvi.append(pvi_actual)
        else:
            if i == 1:
                pvi_actual = sval
            else:
                pvi_actual = pvi[-1]
            pvi.append(pvi_actual)

    return pd.Series(pvi)      

# Function for Negative Volume Index (NVI)
def calc_nvi(data):

    volume = np.array(data["Volume"])
    close = np.array(data["Close"])
                     
    sval = volume[-1]
    nvi = []

    for i in range(1, len(volume)):
        if volume[i] < volume[i - 1]:
            if i == 1:
                prev_pvi = sval
            else:
                prev_pvi = nvi[-1]
            pvi_actual = prev_pvi + (close[i] - close[i - 1]) / close[i - 1] * prev_pvi
            nvi.append(pvi_actual)
        else:
            if i == 1:
                pvi_actual = sval
            else:
                pvi_actual = nvi[-1]
            nvi.append(pvi_actual)

    return pd.Series(nvi) 

# Function for Money Flow Index (MFI) (consider using `ta.mfi` from `ta` library)
def calc_mfi(high, low, close, volume, length=14):
    
    src = pandas_ta.hlc3(high, low, close)
    mfi = pandas_ta.volume.mfi(high, low, close, volume, length)

    return pd.Series(mfi)

def calc_stoch(src, length=21, smooth_fast_d=3):
    """
    Calculates the Stochastic Oscillator with smoothing for the Fast D line.

    Args:
        src (pd.Series): The price data (usually closing price).
        length (int, optional): The lookback window for the Stochastic Oscillator. Defaults to 21.
        smooth_fast_d (int, optional): The smoothing window for the Fast D line. Defaults to 3.

    Returns:
        pd.Series: The Slow %K and Fast %D lines of the Stochastic Oscillator.
    """

    ll = src.rolling(window=length).min()
    hh = src.rolling(window=length).max()
    fast_k = 100 * (src - ll) / (hh - ll)  # Calculate %K (Fast Stochastic)

    # Apply smoothing for Fast D line
    slow_d = fast_k.ewm(alpha=1/smooth_fast_d, min_periods=smooth_fast_d).mean()

    return slow_d  # Assuming you only need the Fast D (%D) line

# Main script
def calculate(data:pd.DataFrame):

    tprice = data[["Open", "High", "Low", "Close"]].mean(axis=1)
    length_ema = 255
    m = 15

    # Calculate indicators (replace with `ta` library functions if available)
    pvi = pd.Series()
    pvi = calc_pvi(data)
    pvim = pandas_ta.ema(close = pvi, length = 15)
    pvimax = pvim.rolling(window=90).max()
    pvimin = pvim.rolling(window=90).min()
    oscp = 100 * (pvi - pvim) / (pvimax - pvimin)

    nvi = calc_nvi(data)
    nvim = pandas_ta.ema(close = nvi, length = 15)
    nvimax = nvim.rolling(window=90).max()
    nvimin = nvim.rolling(window=90).min()
    

    xmf = calc_mfi(data["High"], data["Low"], data["Close"], data["Volume"])  # Consider using ta.mfi
    mult = 2.0
    basis = data["Close"].rolling(window=25).mean()
    dev = mult * data["Close"].rolling(window=25).std()
    upper = basis + dev
    lower = basis - dev
    OB1 = (upper + lower) / 2.0
    OB2 = upper - lower
    BollOsc = 100 * ((tprice - OB1) / OB2)

    xrsi = pd.Series(pandas_ta.rsi(tprice, window=14))  # Consider using ta.rsi from `ta` library
    print(tprice)
    stoc = calc_stoch(tprice, 21, 3)  # Function not provided, replace with custom or ta.stoch from `
    print("STOC")
    print(stoc)
    print("--------------------------------")
    marron = (xrsi + xmf + BollOsc + (stoc / 3))/2
    print("MARRON")
    print(marron)
    print("--------------------------------")
    verde = marron + oscp
    print("VERDE")
    print(verde)
    print("--------------------------------")
    azul = 100 * (nvi - nvim) / (nvimax - nvimin)
    print("AZUL")
    print(azul)
    print("--------------------------------")
    media = pandas_ta.ema(marron, m)
    print("MEDIA")
    print(media)
    print("--------------------------------")
    print("fin")
