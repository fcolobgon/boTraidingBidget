from enum import Enum


class FutureValues(Enum):
    
    SIDE_TYPE_LONG = "long"
    SIDE_TYPE_SHORT = "short"

    @classmethod
    def list(cls):
        return list(map(lambda c: c, cls))
