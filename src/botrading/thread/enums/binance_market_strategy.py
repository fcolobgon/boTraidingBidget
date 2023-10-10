from enum import Enum


class BinanceMarketStrategy(Enum):
    STRATEGY_LOBO = "Lobo"
    STRATEGY_EMI = "Emi"
    STRATEGY_GABRI = "Gabri"

    @classmethod
    def list(cls):
        return list(map(lambda c: c, cls))
