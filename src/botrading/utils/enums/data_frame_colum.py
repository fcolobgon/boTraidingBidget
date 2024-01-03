from enum import Enum

from src.botrading.constants import botrading_constant
from src.botrading.utils.enums.colum_state_values import ColumStateValues


class DataFrameColum(Enum):
    #BITGET

    SYMBOL = "symbol"
    SYMBOL_TEST = "symbol_test"
    SYMBOLNAME = "symbolName"
    SYMBOLTYPE = "symbolType"
    TAKERFEERATE = "takerFeeRate"
    VOLUMEPLACE = "volumePlace"
    SIDE_TYPE = "SIDE_TYPE"
    MONEY_SPENT = "MONEY_SPENT" 
    SIZE = "SIZE" 
    LEVEREAGE = "LEVEREAGE"

    BASE = "baseCoin"
    QUOTE = "quoteCoin"
    
    PRICE = "PRICE"
    CLOSE = "CLOSE"
    DATE = "DATE"
    TAKER_COMMISSION = "TAKER_COMMISSION"
    STATE = "STATE"
    PRICE_BUY = "PRICE_BUY"
    DATE_PRICE_BUY = "DATE_PRICE_BUY"
    PRICE_SELL = "PRICE_SELL"
    LOCK = "LOCK"
    LOOK = "LOOK"
    FIRST_ITERATION = "FIRST_ITERATION"
    ORDER_ID = "ORDER_ID"
    CLIENT_ORDER_ID = "CLIENT_ORDER_ID"
#    DATE_PRICESELLUY = "DATE PRICE SELL"

    #NUEVOS
    MACD_GOOD = "MACD_GOOD"
    MACD_GOOD_ASCENDING = "MACD_GOOD_ASCENDING"
    MACD_BAD = "MACD_BAD"
    MACD_BAD_ASCENDING = "MACD_BAD_ASCENDING"
    MACD_CHAR = "MACD_CHAR"
    MACD_CHAR_ASCENDING = "MACD_CHAR_ASCENDING"
    MACD_CRUCE = "MACD_CRUCE"
    MACD_ZERO = "MACD_ZERO"
    MACD_READY = "MACD_READY"

    IMPULSE_MACD = 'IMPULSE_MACD'
    IMPULSE_MACD_HISTOGRAM = 'IMPULSE_MACD_HISTOGRAM'
    IMPULSE_MACD_SIGNAL = 'IMPULSE_MACD_SIGNAL' 
    IMPULSE_MACD_SIGNALS = 'IMPULSE_MACD_SIGNALS'
    
    SUPER_TREND_GOOD = "SUPER_TREND_GOOD"
    SUPER_TREND_GOOD_ASCENDING = "SUPER_TREND_GOOD_ASCENDINGE"
    SUPER_TREND_READY = "SUPER_TREND_READY"
    
    CANDLE_TREND_NAME = "CANDLE_TREND_NAME"
    CANDLE_TREND = "CANDLE_TREND"
    CANDLE_TREND_PREV = "CANDLE_TREND_PREV"
    
    PRECIO_SOPORTE = "PRECIOS_RESISTENCIA"
    PRECIOS_RESISTENCIA = "PRECIOS_RESISTENCIA"

    #VIEJORS
    CCI = "CCI"
    CCI_LAST = "CCI_LAST"
    CCI_ASCENDING = "CCI_ASCENDING"
    
    SUPER_TREND_LAST = "SUPER_TREND_LAST"
    SUPER_TREND_1 = "SUPER_TREND_1"
    SUPER_TREND_2 = "SUPER_TREND_2"
    SUPER_TREND_3 = "SUPER_TREND_3"
    SUPER_TREND_4 = "SUPER_TREND_4"

    #Viejo MACD
    MACD = "MACD"
    MACD_LAST = "MACD_LAST"
    MACD_PREVIOUS_CHART = "MACD_PREVIOUS_CHART"
    MACD_LAST_CHART = "MACD_LAST_CHART"
    MACD_CHART_ASCENDING = "MACD_CHART_ASCENDING"
    MACD_ASCENDING = "MACD_ASCENDING"
    MACD_GOOD_LINE = "MACD_GOOD_LINE"
    MACD_BAD_LINE = "MACD_BAD_LINE"
    MACD_BAR_CHART = "MACD_BAR_CHART"
    MACD_CRUCE_LINE = "MACD_CRUCE_LINE"
    MACD_CRUCE_ZERO = "MACD_CRUCE_ZERO"

    ADX = "ADX"
    ADX_LAST = "ADX_LAST"
    ADX_ASCENDING = "ADX_ASCENDING"
    ADX_ANGLE = "ADX_ANGLE"

    RSI = "RSI"
    RSI_LAST = "RSI_LAST"
    RSI_ASCENDING = "RSI_ASCENDING"
    RSI_ANGLE = "RSI_ANGLE"

    STOCH_GOOD_LINE = "STOCH_GOOD_LINE"
    STOCH_BAD_LINE = "STOCH_BAD_LINE"
    STOCH_CRUCE_LINE = "STOCH_CRUCE_LINE"
    STOCH_ASCENDING = "STOCH_ASCENDING"
    STOCH_LAST = "STOCH_LAST"

    RSI_STOCH_GOOD_LINE = "RSI_STOCH_GOOD_LINE"
    RSI_STOCH_GOOD_LINE_LAST = "RSI_STOCH_GOOD_LINE_LAST"
    RSI_STOCH_GOOD_LINE_ASCENDING = "RSI_STOCH_GOOD_LINE_ASCENDING"
    RSI_STOCH_GOOD_LINE_ANGLE = "RSI_STOCH_GOOD_LINE_ANGLE"
    RSI_STOCH_BAD_LINE = "RSI_STOCH_BAD_LINE"
    RSI_STOCH_BAD_LINE_LAST = "RSI_STOCH_BAD_LINE_LAST"
    RSI_STOCH_BAD_LINE_ASCENDING = "RSI_STOCH_BAD_LINE_ASCENDING"
    RSI_STOCH_BAD_LINE_ANGLE = "RSI_STOCH_BAD_LINE_ANGLE"
    RSI_STOCH_CRUCE_LINE = "RSI_STOCH_CRUCE_LINE"

    AO = "AO"
    AO_LAST = "AO_LAST"
    AO_ASCENDING = "AO_ASCENDING"

    TSI_GOOD_LINE = "TSI_GOOD_LINE"
    TSI_BAD_LINE = "TSI_BAD_LINE"
    TSI_CRUCE_LINE = "TSI_CRUCE_LINE"
    TSI_ASCENDING = "TSI_ASCENDING"
    TSI_LAST = "TSI_LAST"
    
    EMA_20 = "EMA_20"
    EMA_20_ASCENDING = "EMA_20_ASCENDING"
    EMA_20_LAST = "EMA_20_LAST"
    
    EMA_50 = "EMA_50"
    EMA_50_ASCENDING = "EMA_50_ASCENDING"
    EMA_50_LAST = "EMA_50_LAST"

    MA = "MA"
    MA_ASCENDING = "MA_ASCENDING"
    MA_LAST = "MA_LAST"
    MA_LAST_ANGLE = "MA_LAST_ANGLE"
    MA_OPEN_PRICE_PERCENTAGE = "MA_OPEN_PRICE_PERCENTAGE"
    MA_CLOSE_PRICE_PERCENTAGE = "MA_CLOSE_PRICE_PERCENTAGE"

    TRIX = "TRIX"
    TRIX_ASCENDING = "TRIX_ASCENDING"
    TRIX_LAST = "TRIX_LAST"
    TRIX_ANGLE = "TRIX_ANGLE"

    PERCENTAGE_PROFIT_FLAG = "PERCENTAGE_PROFIT_FLAG"
    PERCENTAGE_PROFIT = "PERCENTAGE_PROFIT"
    PERCENTAGE_PROFIT_PREV = "PERCENTAGE_PROFIT_PREV"
    PERCENTAGE_PROFIT_ASCENDING = "PERCENTAGE_PROFIT_ASCENDING"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_TOUCH = "TAKE_PROFIT_TOUCH"
    STOP_LOSS = "STOP_LOSS"
    STOP_LOSS_LEVEL = "STOP_LOSS_LEVEL"

    TOP_GAINERS = "TOP_GAINERS"
    TOP_GAINERS_ASCENDING = "TOP_GAINERS_ASCENDING"
    TOP_GAINERS_DATE = "TOP_GAINERS_DATE"
    TOP_GAINERS_NUMBER_UPS = "TG_NUMBER_UPS"
    TOP_GAINERS_PERCENTAGE_UPS = "TG_PERCENTAGE_UPS"

    NOTE = "NOTE"
    NOTE_2 = "NOTE_2"
    NOTE_3 = "NOTE_3"
    NOTE_4 = "NOTE_4"
    NOTE_5 = "NOTE_5"
    
    PRICE_VOLUME = "PRICE_VOLUME"
    PRICE_LOW = "PRICE_LOW"
    PRICE_LOW_ASCENDING = "PRICE_LOW_ASCENDING"
    PRICE_HIGH = "PRICE_HIGH"
    PRICE_HIGH_ASCENDING = "PRICE_HIGH_ASCENDING"
    PRICE_OPEN = "PRICE_OPEN"
    PRICE_OPEN_ASCENDING = "PRICE_OPEN_ASCENDING"
    PRICE_CLOSE = "PRICE_CLOSE"
    PRICE_CLOSE_LAST = "PRICE_CLOSE_LAST"
    PRICE_CLOSE_ASCENDING = "PRICE_CLOSE_ASCENDING"
    PRICE_OPEN_TIME = "PRICE_OPEN_TIME"
    PRICE_CLOSE_TIME = "PRICE_CLOSE_TIME"
    PRICE_PERCENTAGE = "PRICE_PERCENTAGE"
    PRICE_PERCENTAGE_PREV = "PRICE_PERCENTAGE_PREV"
    
    FIBO_LEVEL_1 = "FIBO_LEVEL_1"
    FIBO_LEVEL_2 = "FIBO_LEVEL_2"
    FIBO_LEVEL_3 = "FIBO_LEVEL_3"
    FIBO_LEVEL_4 = "FIBO_LEVEL_4"
    
    RESISTENCIAS = "RESISTENCIAS"
    SOPORTES = "SOPORTES"
    SOPORTE_RESISTENCIA_PERCENTAGE = "SOPORTE_RESISTENCIA_PERCENTAGE"
    EN_RESISTENCIA = "EN_RESISTENCIA"
    EN_SOPORTE = "EN_SOPORTE"
    
    @classmethod
    def list(cls):
        return list(map(lambda c: c, cls))

    @classmethod
    def list_values(cls):
        return list(map(lambda c: c.value, cls))
