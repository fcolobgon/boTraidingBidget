from enum import Enum


class ColumStateValues(Enum):
    WAIT = "WAIT"
    SELL = "SELL"
    READY_FOR_SELL = "READY_FOR_SELL"
    READY_FOR_BUY = "READY_FOR_BUY"
    BUY = "BUY"
    NEW = "NEW"
    ERR_SELL = "ERR-SELL"
    ERR_BUY = "ERR-BUY"
    BLCK_INS_MNY = "BLCK_INS_MNY"
    BLOCKED = "BLOCKED"

    @classmethod
    def list(cls):
        return list(map(lambda c: c, cls))
