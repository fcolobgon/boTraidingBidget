from typing import Dict, Optional, List, Any
import json


class BinanceBalanceModel:
    asset: str
    free: str
    locked: str

    def __init__(self, dictionary: Dict) -> None:
        for k, v in dictionary.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceMarketTickers:
    symbol: str
    price: str

    def __init__(self, dictionary: Dict) -> None:
        for k, v in dictionary.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceRateLimitModel:
    rateLimitType: str
    interval: str
    limit: int

    def __init__(self, dictionary: Dict) -> None:
        for k, v in dictionary.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceFilterModel:
    filterType: str
    minPrice: Optional[str]
    maxPrice: Optional[str]
    tickSize: Optional[str]
    minQty: Optional[str]
    maxQty: Optional[str]
    stepSize: Optional[str]
    minNotional: Optional[str]

    def __init__(self, dictionary: Dict) -> None:
        for k, v in dictionary.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceSymbolModel:
    symbol: str
    status: str
    baseAsset: str
    baseAssetPrecision: int
    quoteAssetPrecision: int
    quoteAsset: str
    quotePrecision: int
    orderTypes: List[str]
    icebergAllowed: bool
    filters: List[BinanceFilterModel]

    def __init__(self, dictionary):
        for k, v in dictionary.items():
            if k == "filters":
                filters = []
                for data in v:
                    filters.append(BinanceFilterModel(data))
                setattr(self, k, filters)
            elif k == "orderTypes":
                order_types = []
                for data in v:
                    order_types.append(data)
                setattr(self, k, order_types)
            else:
                setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceProductModel:
    timezone: str
    serverTime: int
    rateLimits: List[BinanceRateLimitModel]
    exchangeFilters: List[Any]
    symbols: List[BinanceSymbolModel]

    def __init__(self, dictionary: Dict):

        for k, v in dictionary.items():
            if k == "symbols":
                symbols = []
                for data in v:
                    symbols.append(BinanceSymbolModel(data))
                setattr(self, k, symbols)
            elif k == "exchangeFilters":
                exchange_filters = []
                for data in v:
                    exchange_filters.append(data)
                setattr(self, k, exchange_filters)
            elif k == "rateLimits":
                rate_limits = []
                for data in v:
                    rate_limits.append(BinanceRateLimitModel(data))
                setattr(self, k, rate_limits)
            else:
                setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceBalance:
    asset: str
    free: str
    locked: str

    def __init__(self, dictionary: Dict) -> None:
        for k, v in dictionary.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceAccountInfo:
    makerCommission: int
    takerCommission: int
    buyerCommission: int
    sellerCommission: int
    canTrade: bool
    canWithdraw: bool
    canDeposit: bool
    balances: List[BinanceBalance]

    def __init__(self, dictionary: Dict):

        for k, v in dictionary.items():
            if k == "balances":
                balances = []
                for data in v:
                    balances.append(BinanceSymbolModel(data))
                setattr(self, k, balances)
            else:
                setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinandeOrder:
    symbol: str
    orderId: int
    clientOrderId: str
    transactTime: int
    price: str
    origQty: str
    executedQty: str
    status: str
    timeInForce: str
    type: str
    side: str

    def __init__(self, dictionary: Dict) -> None:
        for k, v in dictionary.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class BinanceChangePrice:

    symbol: str
    priceChange: str
    priceChangePercent: str
    weightedAvgPrice: str
    prevClosePrice: str
    lastPrice: str
    lastQty: str
    bidPrice: str
    bidQty: str
    askPrice: str
    askQty: str
    openPrice: str
    highPrice: str
    lowPrice: str
    volume: str
    quoteVolume: str
    # openTime:1649082648110,
    # closeTime:1649169048110,
    # firstId:333353212,
    # lastId:333496498,
    # count:143287

    def __init__(self, dictionary: Dict) -> None:
        for k, v in dictionary.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
