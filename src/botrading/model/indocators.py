from src.botrading.constants.indicator_constants import (
    ConfigMACDConstant,
    ConfigSTOCHConstant,
    ConfigCCIConstant,
    ConfigSTOCHrsiConstant,
    ConfigADXConstant,
    ConfigTSIConstant,
    ConfigMAConstant,
    ConfigTRIXConstant,
)
from configs.default import (
    ConfigMACD as loadConfigMACD,
    ConfigSTOCH as loadConfigSTOCH,
    ConfigCCI as loadConfigCCI,
    ConfigMA as loadConfigMA,
    ConfigSTOCHrsi as loadConfigSTOCHrsi,
    ConfigADX as loadConfigADX,
    ConfigTSI as loadConfigTSI
)


class ConfigMACD:

    fast: int
    slow: int
    signal: int

    def __init__(
        self,
        fast: int = ConfigMACDConstant.DEFAULT_FAST,
        slow: int = ConfigMACDConstant.DEFAULT_SLOW,
        signal: int = ConfigMACDConstant.DEFAULT_SIGNAL,
    ):
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def load_env(self):
        self.fast = loadConfigMACD.MACD_VALUE_FAST
        self.slow = loadConfigMACD.MACD_VALUE_SLOW
        self.signal = loadConfigMACD.MACD_VALUE_SIGNAL


class ConfigSTOCH:

    series: int
    d: int
    smooth: int

    def __init__(
        self,
        series: int = ConfigSTOCHConstant.DEFAULT_SERIES,
        d: int = ConfigSTOCHConstant.DEFAULT_D,
        smooth: int = ConfigSTOCHConstant.DEFAULT_SMOOTH,
    ):
        self.series = series
        self.d = d
        self.smooth = smooth

    def load_env(self):
        self.series = loadConfigSTOCH.STOCH_VALUE_SERIES
        self.d = loadConfigSTOCH.STOCH_VALUE_D
        self.smooth = loadConfigSTOCH.STOCH_VALUE_SMOOTH


class ConfigCCI:

    series: int

    def __init__(self, series: int = ConfigCCIConstant.DEFAULT_SERIES):
        self.series = series

    def load_env(self):
        self.series = loadConfigCCI.CCI_VALUE_SERIES


class ConfigSTOCHrsi: 

    longitud_rsi: int
    longitud_stoch: int
    smooth_k: int
    smooth_d:int

    def __init__(
        self, 
        longitud_rsi: int = ConfigSTOCHrsiConstant.DEFAULT_LONGITUD_RSI,
        longitud_stoch: int = ConfigSTOCHrsiConstant.DEFAULT_LONGITUD_STOCH,
        smooth_k: int = ConfigSTOCHrsiConstant.DEFAULT_SMOOTH_K,
        smooth_d: int = ConfigSTOCHrsiConstant.DEFAULT_SMOOTH_D
    ):
        self.longitud_rsi = longitud_rsi
        self.longitud_stoch = longitud_stoch
        self.smooth_k = smooth_k
        self.smooth_d = smooth_d

    def load_env(self):
        self.longitud_rsi = ConfigSTOCHrsiConstant.DEFAULT_LONGITUD_RSI
        self.longitud_stoch = ConfigSTOCHrsiConstant.DEFAULT_LONGITUD_STOCH
        self.smooth_k = ConfigSTOCHrsiConstant.DEFAULT_SMOOTH_K
        self.smooth_d = ConfigSTOCHrsiConstant.DEFAULT_SMOOTH_D


class ConfigADX:

    series: int

    def __init__(self, series: int = ConfigADXConstant.DEFAULT_SERIES):
        self.series = series

    def load_env(self):
        self.series = loadConfigADX.ADX_VALUE_SERIES


class ConfigTSI:

    long: int
    short: int
    siglen: int

    def __init__(
        self,
        long: int = ConfigTSIConstant.DEFAULT_LONG,
        short: int = ConfigTSIConstant.DEFAULT_SHORT,
        siglen: int = ConfigTSIConstant.DEFAULT_SIGLEN
    ):
        self.long = long
        self.short = short
        self.siglen = siglen

    def load_env(self):
        self.long = loadConfigTSI.TSI_VALUE_LONG
        self.short = loadConfigTSI.TSI_VALUE_SHORT
        self.siglen = loadConfigTSI.TSI_VALUE_SIGLEN

class ConfigMA:

    length: int
    type: str

    def __init__(self, length: int = ConfigMAConstant.DEFAULT_LENGTH, type:str = "ema"):
        self.length = length
        self.type = type

    def load_env(self):
        self.length = loadConfigMA.DEFAULT_LENGTH


class ConfigTrix: 

    length: int
    signal: int

    def __init__(
        self, 
        length: int = ConfigTRIXConstant.DEFAULT_LENGTH_TRIX,
        signal: int = ConfigTRIXConstant.DEFAULT_SIGNAL_TRIX
    ):
        self.length = length
        self.signal = signal

    def load_env(self):
        self.length = ConfigTRIXConstant.DEFAULT_LENGTH_TRIX
        self.signal = ConfigTRIXConstant.DEFAULT_SIGNAL_TRIX

class ConfigSupertrend:

    length: int
    factor: int

    def __init__(self, length: int = 10, factor: int = 3):
        self.length = length
        self.factor = factor
