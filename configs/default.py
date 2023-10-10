import os

from os.path import abspath, dirname, join
from decouple import config
from src.botrading.constants.indicator_constants import ConfigMACDConstant, ConfigMAConstant, ConfigSTOCHConstant, ConfigCCIConstant, ConfigSTOCHrsiConstant, ConfigADXConstant, ConfigTSIConstant



# Define the application directory
BASE_DIR = dirname(dirname(abspath(__file__)))
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
SOURCE_PATH = join(ROOT_DIR, "src")
CONFIG_PATH = join(ROOT_DIR, "configs")
DOCS_PATH = join(ROOT_DIR, "docs")
TESTS_PATH = join(ROOT_DIR, "tests")
DATA_PATH = join(ROOT_DIR, "data")
REPORTS_PATH = join(ROOT_DIR, "reports")



class ConfigMACD:
    
    MACD_VALUE_FAST  = config('MACD_VALUE_FAST', default=ConfigMACDConstant.DEFAULT_FAST, cast=int)
    MACD_VALUE_SLOW =  config('MACD_VALUE_SLOW', default=ConfigMACDConstant.DEFAULT_SLOW, cast=int)    
    MACD_VALUE_SIGNAL =  config('MACD_VALUE_SIGNAL', default=ConfigMACDConstant.DEFAULT_SIGNAL, cast=int)


class ConfigSTOCH:

    STOCH_VALUE_SERIES = config('MACD_VALUE_FAST', default=ConfigSTOCHConstant.DEFAULT_SERIES, cast=int)
    STOCH_VALUE_D = config('STOCH_VALUE_D', default=ConfigSTOCHConstant.DEFAULT_D, cast=int)
    STOCH_VALUE_SMOOTH = config('STOCH_VALUE_SMOOTH', default=ConfigSTOCHConstant.DEFAULT_SMOOTH, cast=int)


class ConfigCCI:

    CCI_VALUE_SERIES = config('CCI_VALUE_SERIES', default=ConfigCCIConstant.DEFAULT_SERIES, cast=int)


class ConfigMA:

    DEFAULT_LENGTH = config('DEFAULT_LENGTH', default=ConfigMAConstant.DEFAULT_LENGTH, cast=int)



class ConfigSTOCHrsi:

    STOCH_RSI_VALUE_SERIES = config('STOCH_RSI_VALUE_LONGITUD_RSI', default=ConfigSTOCHrsiConstant.DEFAULT_LONGITUD_RSI, cast=int)
    STOCH_RSI_VALUE_SERIES = config('STOCH_RSI_VALUE_LONGITUD_STOCH', default=ConfigSTOCHrsiConstant.DEFAULT_LONGITUD_STOCH, cast=int)
    STOCH_RSI_VALUE_SERIES = config('STOCH_RSI_VALUE_SMOOTH_K', default=ConfigSTOCHrsiConstant.DEFAULT_SMOOTH_K, cast=int)
    STOCH_RSI_VALUE_SERIES = config('STOCH_RSI_VALUE_SMOOTH_D', default=ConfigSTOCHrsiConstant.DEFAULT_SMOOTH_D, cast=int)


class ConfigADX:

    ADX_VALUE_SERIES = config('ADX_VALUE_SERIES', default=ConfigADXConstant.DEFAULT_SERIES, cast=int)


class ConfigTSI:

    TSI_VALUE_LONG = config('TSI_VALUE_LONG', default=ConfigTSIConstant.DEFAULT_LONG, cast=int)
    TSI_VALUE_SHORT = config('TSI_VALUE_SHORT', default=ConfigTSIConstant.DEFAULT_SHORT, cast=int)
    TSI_VALUE_SIGLEN = config('TSI_VALUE_SIGLEN', default=ConfigTSIConstant.DEFAULT_SIGLEN, cast=int)



