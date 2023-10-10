from datetime import datetime, timedelta
import time

from src.botrading.model.time_ranges import *
from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.binance_data_util import BinanceDataUtil
from src.botrading.thread.enums.binance_market_status import BinanceMarketStatus

class DateUtil:
    
    @staticmethod
    def check_execution(time_range:TimeRanges):
        
        if time_range.name == "MINUTES_15":
            return DateUtil.check_minutes_pattern(minutes_list=[0,15,30,45]) and DateUtil.check_minutes_pattern(minutes_list=[0])
        
        if time_range.name == "MINUTES_30":
            return DateUtil.check_minutes_pattern(minutes_list=[0,30]) and DateUtil.check_minutes_pattern(minutes_list=[0])
        
        if time_range.name == "HOUR_1":
            return DateUtil.check_hour_pattern(hour_list=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]) and DateUtil.check_minutes_pattern(minutes_list=[0])

        if time_range.name == "HOUR_4":
            return DateUtil.check_hour_pattern(hour_list=[0,4,8,12,16,20]) and DateUtil.check_minutes_pattern(minutes_list=[0])
        
        if time_range.name == "DAY_1":
            return DateUtil.check_hour_pattern(hour_list=[0]) and DateUtil.check_minutes_pattern(minutes_list=[0])
        
        return False
    
    @staticmethod
    def check_hour_minute(hour:int, minute:int):
        now = datetime.now()
        if now.hour == hour and now.minute == minute: 
                return True
        return False
    
    @staticmethod
    def wait_until_minute(minutes_list):
        
        while not DateUtil.check_minutes_pattern(minutes_list):
            now = datetime.now()
            remaining_seconds = (60 - now.second) % 60
            next_minute = (now + timedelta(seconds=remaining_seconds)).replace(second=0)
            time_to_wait = (next_minute - now).total_seconds()
            time_to_wait = max(time_to_wait, 1)  # Wait at least 1 second to avoid busy loop
            time.sleep(time_to_wait)
            
    @staticmethod
    def check_seconds_pattern(seconds_list:int):
        now = datetime.now()
        for second in seconds_list:
            if now.second == second:
                return True
        return False
            
    @staticmethod
    def check_minutes_pattern(minutes_list:int):
        now = datetime.now()
        for minute in minutes_list:
            if now.minute == minute:
                return True
        return False
    
    @staticmethod
    def check_hour_pattern(hour_list:int):
        now = datetime.now()
        for hour in hour_list:
            if now.hour == hour:
                return True
        return False