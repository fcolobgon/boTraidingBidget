from datetime import datetime
import pandas


class TimeRanges:
    types = ["MINUTES_1", "MINUTES_5", "MINUTES_15", "MINUTES_30", "HOUR_1", "HOUR_4", "DAY_1"]

    name: str

    interval: str

    x_days: int

    refresh_time: float
    
    millis_correlation: float

    def __init__(self, type: str):

        # TODO: Validar tipo
        if type == "MINUTES_1":
            self.name = "MINUTES_1"
            self.interval = "1m"
            self.x_days = 300
            self.refresh_time = 60
            self.millis_correlation = 1000*60
        elif type == "MINUTES_5":
            self.name = "MINUTES_5"
            self.interval = "5m"
            self.x_days = 300
            self.refresh_time = 60 * 5
            self.millis_correlation = 1000*60*5
        elif type == "MINUTES_15":
            self.name = "MINUTES_15"
            self.interval = "15m"
            self.x_days = 300
            self.refresh_time = 60 * 15
            self.millis_correlation = 1000*60*15
        elif type == "MINUTES_30":
            self.name = "MINUTES_30"
            self.interval = "30m"
            self.x_days = 300
            self.refresh_time = 60 * 30
            self.millis_correlation = 1000*60*30
        elif type == "HOUR_1":
            self.name = "HOUR_1"
            self.interval = "1H"
            self.x_days = 300
            self.refresh_time = 60 * 60
            self.millis_correlation = 1000*60*60
        elif type == "HOUR_2":
            self.name = "HOUR_2"
            self.interval = "2H"
            self.x_days = 300
            self.refresh_time = 60 * 60 * 2
            self.millis_correlation = 1000*60*60*2
        elif type == "HOUR_4":
            self.name = "HOUR_4"
            self.interval = "4H"
            self.x_days = 300
            self.refresh_time = 60 * 60 * 4
            self.millis_correlation = 1000*60*60*4
        elif type == "DAY_1":
            self.name = "DAY_1"
            self.interval = "1D"
            self.x_days = 300
            self.refresh_time = 60 * 60 * 24
            self.millis_correlation = 1000*60*60*24
        elif type == "WEEK":
            self.name = "WEEK"
            self.interval = "1W"
            self.x_days = 300
            self.refresh_time = 60 * 60 * 24
            self.millis_correlation = 1000*60*60*24
        else:
            raise Exception("Invalid type " + type)
    
    def get_time_range_by_name(name: str):
        time_range_mapping = {
            "MINUTES_1": TimeRanges("MINUTES_1"),
            "MINUTES_5": TimeRanges("MINUTES_5"),
            "MINUTES_15": TimeRanges("MINUTES_15"),
            "MINUTES_30": TimeRanges("MINUTES_30"),
            "HOUR_1": TimeRanges("HOUR_1"),
            "HOUR_2": TimeRanges("HOUR_2"),
            "HOUR_4": TimeRanges("HOUR_4"),
            "DAY_1": TimeRanges("DAY_1"),
            "WEEK": TimeRanges("WEEK")
            # Agrega más mapeos según sea necesario
        }

        if name in time_range_mapping:
            return time_range_mapping[name]
        else:
            raise Exception("Invalid time range name: " + name)
