from enum import Enum


class ColumLineValues(Enum):
    BLUE_TOP = "BLUE_ON_TOP"
    RED_TOP = "RED_ON_TOP"
    BLUE_CRUCE_DOWN = "BLUE_CRUCE_DOWN"
    BLUE_CRUCE_TOP = "BLUE_CRUCE_TOP"
    NO_CLEAR_TREND = "NO_CLEAR_TREND"
    
    ZERO_CRUCE_TOP = "LINE_CRUCE_TOP"
    ZERO_CRUCE_DOWN = "ZERO_CRUCE_DOWN"
    
    ALCISTA = "ALCISTA"
    BAJISTA = "BAJISTA"
    NEUTRO = "NEUTRO"

    @classmethod
    def list(cls):
        return list(map(lambda c: c, cls))
