#import logging
from datetime import datetime
from typing import List
import pandas
from enum import Enum
import numpy
import math
from math import atan
import time
from collections import Counter

import ta
import ta.trend as trend
from ta.trend import ADXIndicator
import pandas_ta 

from src.botrading.utils.math_calc_util import MathCal_util
from src.botrading.constants import botrading_constant
from src.botrading.model.indocators import *
from src.botrading.model.time_ranges import *
from src.botrading.bit import BitgetClienManager
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils import excel_util
from src.botrading.utils.text_util import textUtil
from src.botrading.utils.enums.colum_state_values import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.colum_good_bad_values import ColumLineValues
from src.botrading.utils.enums.future_values import FutureValues

from configs.config import settings as settings

import requests

     
class BitgetDataUtil:

    quote_asset = botrading_constant.FUTURE_CONTRACT_USDT_UMCBL
    client_bit: BitgetClienManager
    crypto_observe_list: List
    crypto_remove = []
    data_frame_bkp: pandas.DataFrame = pandas.DataFrame()

    def __init__(
        self,
        client_bit: BitgetClienManager,
        crypto_observe_default_list: List[str] = settings.OBSERVE_COIN_LIST,
        crypto_remove_list: List[str] = settings.REMOVE_COIN_LIST,
        load_from_previous_execution: bool = False,
    ):
    
        self.crypto_remove = crypto_remove_list
        self.crypto_observe_list = crypto_observe_default_list
        self.client_bit = client_bit

        if load_from_previous_execution == True:
            self.data_frame_bkp = excel_util.load_dataframe()
        
        if self.data_frame_bkp.empty:
            all_coins_df = self.client_bit.get_all_coins_filter_contract(productType=settings.FUTURE_CONTRACT)
            
            #Remove selected coins
            all_coins_df = all_coins_df.drop(all_coins_df[all_coins_df[DataFrameColum.BASE.value].isin(self.crypto_remove)].index)
            

            if len(self.crypto_observe_list) == 0:
                #All coins
                self.data_frame_bkp = all_coins_df
            else:
                #Mantener filas
                self.data_frame_bkp = all_coins_df[all_coins_df[DataFrameColum.BASE.value].isin(self.crypto_observe_list)]

            #Solo se ejecuta en para modo TEST
            if settings.BITGET_CLIENT_TEST_MODE == True:
                all_coins_df[DataFrameColum.SYMBOL_TEST.value] = all_coins_df.apply(lambda row: textUtil.convert_text_mode_test(row[DataFrameColum.BASE.value], row[DataFrameColum.QUOTE.value], row[DataFrameColum.SYMBOL.value]), axis=1)
                columnas_a_mantener = [DataFrameColum.BASE.value, DataFrameColum.QUOTE.value, 
                                DataFrameColum.SYMBOL.value, DataFrameColum.SYMBOL_TEST.value, DataFrameColum.SYMBOLNAME.value, DataFrameColum.SYMBOLTYPE.value, 
                                DataFrameColum.TAKERFEERATE.value, DataFrameColum.VOLUMEPLACE.value ]  # Índices de las columnas que deseas mantener (0-indexed)
            else:
                columnas_a_mantener = [DataFrameColum.BASE.value, DataFrameColum.QUOTE.value, 
                                DataFrameColum.SYMBOL.value, DataFrameColum.SYMBOLNAME.value, DataFrameColum.SYMBOLTYPE.value, DataFrameColum.TAKERFEERATE.value, 
                                DataFrameColum.VOLUMEPLACE.value ]  # Índices de las columnas que deseas mantener (0-indexed)
            
            # Conservar solo las columnas indicadas en el columnas_a mantener
            self.data_frame_bkp = self.data_frame_bkp[columnas_a_mantener]

            #Columnas necesarias de arranque
            #self.data_frame_bkp[DataFrameColum.SYMBOL.value] = "-"
            self.data_frame_bkp[DataFrameColum.STATE.value] = ColumStateValues.WAIT.value
            #self.data_frame_bkp[DataFrameColum.BASE.value] = "-"
            #self.data_frame_bkp[DataFrameColum.QUOTE.value] = "-"
            self.data_frame_bkp[DataFrameColum.DATE.value] = "-"
            self.data_frame_bkp[DataFrameColum.PRICE_BUY.value] = "-"
            self.data_frame_bkp[DataFrameColum.PRICE_SELL.value] = "-"
            self.data_frame_bkp[DataFrameColum.CLOSE.value] = "-"
            self.data_frame_bkp[DataFrameColum.DATE_PRICE_BUY.value] = "-"
            self.data_frame_bkp[DataFrameColum.LOCK.value] = "-"
            self.data_frame_bkp[DataFrameColum.LOOK.value] = False
            self.data_frame_bkp[DataFrameColum.FIRST_ITERATION.value] = True
            self.data_frame_bkp[DataFrameColum.SIDE_TYPE.value] = "-"
            self.data_frame_bkp[DataFrameColum.MONEY_SPENT.value] = 0.0
            self.data_frame_bkp[DataFrameColum.SIZE.value] = 0.0

            self.data_frame_bkp[DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
            self.data_frame_bkp[DataFrameColum.PERCENTAGE_PROFIT.value] = 0.0
            self.data_frame_bkp[DataFrameColum.PERCENTAGE_PROFIT_PREV.value] = 0.0
            self.data_frame_bkp[DataFrameColum.PERCENTAGE_PROFIT_ASCENDING.value] = False
            self.data_frame_bkp[DataFrameColum.TAKE_PROFIT.value] = 0.0
            self.data_frame_bkp[DataFrameColum.TAKE_PROFIT_TOUCH.value] = False
            self.data_frame_bkp[DataFrameColum.STOP_LOSS.value] = 0.0
            self.data_frame_bkp[DataFrameColum.STOP_LOSS_LEVEL.value] = 0.0
            
            self.data_frame_bkp[DataFrameColum.LEVEREAGE.value] = 0  

            self.data_frame_bkp[DataFrameColum.NOTE.value] = "-"  
            self.data_frame_bkp[DataFrameColum.NOTE_2.value] = "-"
            self.data_frame_bkp[DataFrameColum.NOTE_3.value] = "-"
            self.data_frame_bkp[DataFrameColum.NOTE_4.value] = "-"
            self.data_frame_bkp[DataFrameColum.NOTE_5.value] = "-"

            self.data_frame_bkp = DataFrameCheckUtil.create_price_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_candle_trend_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_rsi_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_supertrend_columns(data_frame=self.data_frame_bkp)
            self.data_frame_bkp = DataFrameCheckUtil.create_adx_columns(data_frame=self.data_frame_bkp)
            self.data_frame_bkp = DataFrameCheckUtil.create_ao_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_macd_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_rsi_stoch_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_stoch_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_cci_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_tsi_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_ma_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_trix_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_top_gainers_columns(data_frame=self.data_frame_bkp)
            self.data_frame_bkp = DataFrameCheckUtil.create_soporte_resistencia_columns(data_frame=self.data_frame_bkp)

    

    
    def get_crypto_observe_list(self):
        return self.crypto_observe_list
        
    def get_historial_x_day_ago_all_crypto(self,  df_master,time_range:TimeRanges=None, limit:int = 500) -> dict:
        
        dict_values = {}

        for ind in df_master.index:
            symbol = df_master[DataFrameColum.SYMBOL.value][ind]

            prices_history = self.client_bit.get_historial_x_day_ago( symbol, time_range.x_days, time_range.interval, limit = limit)[[
                "Open time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Close time"
                ]]
            
            dict_values[symbol] = prices_history

        return dict_values
    
    def updating_price_indicators(self, time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):
        
        data_frame = DataFrameCheckUtil.create_price_columns(data_frame=data_frame)
        
        for ind in data_frame.index:
            
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
            
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                                
                low_numpy = numpy.array(prices_history['Low'].astype(float).values)
                low_numpy = low_numpy[~numpy.isnan(low_numpy)]
                
                high_numpy = numpy.array(prices_history['High'].astype(float).values)
                high_numpy = high_numpy[~numpy.isnan(high_numpy)]
                
                open_numpy = numpy.array(prices_history['Open'].astype(float).values)
                open_numpy = open_numpy[~numpy.isnan(open_numpy)]
                
                close_numpy = numpy.array(prices_history['Close'].astype(float).values)
                close_numpy = close_numpy[~numpy.isnan(close_numpy)]

                data_frame.loc[ind, DataFrameColum.PRICE_LOW.value] = self.get_last_element(element_list = low_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.PRICE_LOW_ASCENDING.value] = self.verificar_ascendente(check_list = low_numpy)
                data_frame.loc[ind, DataFrameColum.PRICE_HIGH.value] = self.get_last_element(element_list = high_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.PRICE_HIGH_ASCENDING.value] = self.verificar_ascendente(check_list = high_numpy)
                data_frame.loc[ind, DataFrameColum.PRICE_OPEN.value] = self.get_last_element(element_list = open_numpy, previous_period = previous_period)
                data_frame[DataFrameColum.PRICE_OPEN_ASCENDING.value][ind] = self.verificar_ascendente(check_list = open_numpy)
                data_frame.loc[ind, DataFrameColum.PRICE_CLOSE.value] = self.get_last_element(element_list = close_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.PRICE_PERCENTAGE_PREV.value] = data_frame.loc[ind, DataFrameColum.PRICE_PERCENTAGE.value]
                data_frame.loc[ind, DataFrameColum.PRICE_PERCENTAGE.value] = (self.get_last_element(element_list = close_numpy, previous_period = previous_period) * 100.0 / self.get_last_element(element_list = open_numpy, previous_period = previous_period)) - 100
                data_frame[DataFrameColum.PRICE_CLOSE_ASCENDING.value][ind] = self.verificar_ascendente(check_list = close_numpy)
                #data_frame[DataFrameColum.PRICE_OPEN_TIME.value][ind] = prices_history['Open time']
                #data_frame[DataFrameColum.PRICE_CLOSE_TIME.value][ind] = prices_history['Close time']

            except Exception as e:
                self.print_error_updating_indicator(symbol, "PRICE", e)
                continue

            
        return data_frame
    
    def verificar_ascendente(self, check_list:numpy):
       
        precios = check_list[-3:]
       
        for i in range(1, len(precios)):
            if precios[i] <= precios[i-1]:
                return False
       
        return True

    def updating_rsi(self, time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):
        
        data_frame = DataFrameCheckUtil.create_rsi_columns(data_frame=data_frame)
        
        for ind in data_frame.index:
            
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
            
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                    
                rsi = pandas_ta.rsi(prices_history['Close'])
                
                rsi_numpy = numpy.array(rsi)
                rsi_numpy = rsi_numpy[~numpy.isnan(rsi_numpy)]
                data_frame[DataFrameColum.RSI.value][ind] = rsi_numpy
                data_frame[DataFrameColum.RSI_LAST.value][ind] = rsi_numpy[-1]
                data_frame[DataFrameColum.RSI_ASCENDING.value][ind] = self.list_is_ascending(check_list = rsi_numpy, ascending_count = ascending_count, previous_period = previous_period)

            except Exception as e:
                self.print_error_updating_indicator(symbol, "RSI", e)
                continue

            
        return data_frame
    
    def updating_supertrend(self, config_supertrend:ConfigSupertrend=ConfigSupertrend(), time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None):
        length = config_supertrend.length
        factor = config_supertrend.factor
        super_name = "SUPERTd_"+ str(length) +"_" + str(factor) +".0"
        
        data_frame = DataFrameCheckUtil.create_supertrend_columns(data_frame=data_frame)
        
        for ind in data_frame.index:
            
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
            
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                    
                prices_high = prices_history['High'].astype(float)
                prices_low = prices_history['Low'].astype(float)
                prices_close = prices_history['Close'].astype(float)
                    
                trend = pandas_ta.supertrend(high=prices_high, low=prices_low, close=prices_close, length=length, multiplier=factor)

                value_trend = numpy.array(trend[super_name])
                value_trend = value_trend[~numpy.isnan(value_trend)]

                if value_trend[-2] == 1:
                    data_frame.loc[ind, DataFrameColum.SUPER_TREND_1.value] = True
                else:
                    data_frame.loc[ind, DataFrameColum.SUPER_TREND_1.value] = False
                    
                if value_trend[-1] == 1:
                    data_frame.loc[ind, DataFrameColum.SUPER_TREND_LAST.value] = True
                else:
                    data_frame.loc[ind, DataFrameColum.SUPER_TREND_LAST.value] = False

            except Exception as e:
                self.print_error_updating_indicator(symbol, "SUPER TREND", e)
                continue

        return data_frame


    def updating_adx(self, config_adx:ConfigADX=ConfigADX(), time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):
        
        series = config_adx.series
        
        data_frame = DataFrameCheckUtil.create_adx_columns(data_frame=data_frame)

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                
                prices_high = prices_history['High'].astype(float)
                prices_low = prices_history['Low'].astype(float)
                prices_close = prices_history['Close'].astype(float)
                open_time_arr = numpy.array(prices_history['Close time'].values)  

                adx = ADXIndicator(high = prices_high, low = prices_low, close = prices_close, window= series).adx()
                adx_numpy = numpy.array(adx)
                adx_numpy = adx_numpy[~numpy.isnan(adx_numpy)]
                #data_frame[DataFrameColum.ADX.value][ind] = adx_numpy
                data_frame.loc[ind, DataFrameColum.ADX_LAST.value] = self.get_last_element(element_list = adx_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.ADX_ASCENDING.value] = self.list_is_ascending(check_list = adx_numpy, ascending_count = ascending_count, previous_period = previous_period)
                #data_frame.loc[ind, DataFrameColum.ADX_ANGLE.value] = self.angle_one_line(line_points = adx_numpy, time_points = open_time_arr, time_range = time_range)
                data_frame.loc[ind, DataFrameColum.ADX_ANGLE.value] = self.adx_angle(list_adx = adx_numpy, previous_period = previous_period)
                
                                                                    
            except Exception as e:
                self.print_error_updating_indicator(symbol, "ADX", e)
                continue
        
        return data_frame  
    
    def updating_ao(self, time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):

        data_frame = DataFrameCheckUtil.create_ao_columns(data_frame=data_frame)
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                
                ao = pandas_ta.ao(high = prices_history['High'].astype(float), low = prices_history['Low'].astype(float))
                ao_numpy = numpy.array(ao)
                ao_numpy = ao_numpy[~numpy.isnan(ao_numpy)]
                #data_frame[DataFrameColum.AO.value][ind] = ao_numpy
                data_frame.loc[ind, DataFrameColum.AO_LAST.value] = self.get_last_element(element_list = ao_numpy ,previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.AO_ASCENDING.value] =  self.list_is_ascending(check_list = ao_numpy, ascending_count = ascending_count, previous_period = previous_period)

            except Exception as e:
                self.print_error_updating_indicator(symbol, "AO", e)
                continue
        
        return data_frame
    
    def updating_price(self, data_frame:pandas.DataFrame=pandas.DataFrame()):
        
        for ind in data_frame.index:
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            currentPrice = self.client_bit.get_price_for_symbol(symbol)

            data_frame.loc[ind, DataFrameColum.PRICE_BUY.value] = currentPrice
        
        return data_frame  
    
    def updating_impulse_macd(self, config_macd:ConfigMACD=ConfigMACD(), time_range:TimeRanges = None, data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0):
        
        fast=config_macd.fast
        slow=config_macd.slow
        signal=config_macd.signal
        
        chars = "MACDh_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        blue_line = "MACD_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        red_line = "MACDs_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        
        data_frame = DataFrameCheckUtil.create_macd_columns(data_frame=data_frame)
                        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                
                prices_close = prices_history['Close'].astype(float)
                ta.add_momentum_ta()
                macd =  pandas_ta.macd(close = prices_close, fast=fast, slow=slow, signal=signal)
                
                macd_numpy = numpy.array(macd[blue_line])
                macd_h_numpy = numpy.array(macd[chars])
                macd_s_numpy = numpy.array(macd[red_line])
                
                macd_numpy = macd_numpy[~numpy.isnan(macd_numpy)]
                macd_h_numpy = macd_h_numpy[~numpy.isnan(macd_h_numpy)]
                macd_s_numpy = macd_s_numpy[~numpy.isnan(macd_s_numpy)]
        
                #data_frame[DataFrameColum.MACD_BAR_CHART.value][ind] = macd_h_numpy
                #data_frame[DataFrameColum.MACD_GOOD_LINE.value][ind] = macd_numpy
                #data_frame[DataFrameColum.MACD_BAD_LINE.value][ind] = macd_s_numpy
                data_frame.loc[ind, DataFrameColum.MACD_LAST.value] = self.get_last_element(element_list = macd_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_LAST_CHART.value] = self.get_last_element(element_list = macd_h_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_PREVIOUS_CHART.value] = macd_h_numpy[-2]
                data_frame.loc[ind, DataFrameColum.MACD_CHART_ASCENDING.value] = self.list_is_ascending(check_list = macd_h_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_ASCENDING.value] = self.list_is_ascending(check_list = macd_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_LINE.value] = self.good_indicator_on_top_of_bad(macd_numpy, macd_s_numpy, ascending_count, previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_ZERO.value] = self.cruce_zero(macd_h_numpy)
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "MACD", e)
                continue
        
        return data_frame       
    
    def updating_macd(self, config_macd:ConfigMACD=ConfigMACD(), time_range:TimeRanges = None, data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0):
        
        fast=config_macd.fast
        slow=config_macd.slow
        signal=config_macd.signal
        
        chars = "MACDh_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        blue_line = "MACD_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        red_line = "MACDs_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        
        data_frame = DataFrameCheckUtil.create_macd_columns(data_frame=data_frame)
                        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                
                prices_close = prices_history['Close'].astype(float)

                macd =  pandas_ta.macd(close = prices_close, fast=fast, slow=slow, signal=signal)
                
                macd_numpy = numpy.array(macd[blue_line])
                macd_h_numpy = numpy.array(macd[chars])
                macd_s_numpy = numpy.array(macd[red_line])
                
                macd_numpy = macd_numpy[~numpy.isnan(macd_numpy)]
                macd_h_numpy = macd_h_numpy[~numpy.isnan(macd_h_numpy)]
                macd_s_numpy = macd_s_numpy[~numpy.isnan(macd_s_numpy)]
        
                #data_frame[DataFrameColum.MACD_BAR_CHART.value][ind] = macd_h_numpy
                #data_frame[DataFrameColum.MACD_GOOD_LINE.value][ind] = macd_numpy
                #data_frame[DataFrameColum.MACD_BAD_LINE.value][ind] = macd_s_numpy
                data_frame.loc[ind, DataFrameColum.MACD_LAST.value] = self.get_last_element(element_list = macd_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_LAST_CHART.value] = self.get_last_element(element_list = macd_h_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_PREVIOUS_CHART.value] = macd_h_numpy[-2]
                data_frame.loc[ind, DataFrameColum.MACD_CHART_ASCENDING.value] = self.list_is_ascending(check_list = macd_h_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_ASCENDING.value] = self.list_is_ascending(check_list = macd_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_LINE.value] = self.good_indicator_on_top_of_bad(macd_numpy, macd_s_numpy, ascending_count, previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_ZERO.value] = self.cruce_zero(macd_h_numpy)
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "MACD", e)
                continue
        
        return data_frame 

    def updating_impulse_macd_lazybear (self, config_macd:ConfigMACD=ConfigMACD(), time_range:TimeRanges = None, data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0):
        """
        Calcula el indicador Impulse MACD LazyBear para un conjunto de datos de precios.

        Parámetros:
            data: Un DataFrame de Pandas que contiene los precios de cierre.
            fast_period: El período de la media móvil exponencial rápida.
            slow_period: El período de la media móvil exponencial lenta.
            signal_period: El período de la media móvil exponencial de la señal.

        Devuelve:
            Un DataFrame de Pandas que contiene los valores del indicador Impulse MACD LazyBear.
        """        
        fast=config_macd.fast
        slow=config_macd.slow
        signal=config_macd.signal
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                
                prices_close = prices_history['Close'].astype(float)

                # Calcula las medias móviles exponenciales.
                fast_ema = pandas.Series(prices_close.ewm(span=fast, min_periods=fast - 1).mean())
                slow_ema = pandas.Series(prices_close.ewm(span=slow, min_periods=slow - 1).mean())

                # Calcula la línea MACD.
                macd = fast_ema - slow_ema

                # Calcula la línea de señal.
                signal_ema = pandas.Series(macd.ewm(span=signal, min_periods=signal - 1).mean())

                # Calcula el histograma.
                histogram = macd - signal_ema

                # Crea la columna de señales.
                signals = []
                for i in range(len(prices_close)):
                    if histogram[i] > 0 and histogram[i - 1] <= 0:
                        signals.append(FutureValues.SIDE_TYPE_LONG.value)
                    elif histogram[i] < 0 and histogram[i - 1] >= 0:
                        signals.append(FutureValues.SIDE_TYPE_SHORT.value)
                    else:
                        signals.append("No trade")

                data_frame.loc[ind, DataFrameColum.IMPULSE_MACD.value] = macd
                data_frame.loc[ind, DataFrameColum.IMPULSE_MACD_HISTOGRAM.value] = histogram
                data_frame.loc[ind, DataFrameColum.IMPULSE_MACD_SIGNAL.value] = signal
                data_frame.loc[ind, DataFrameColum.IMPULSE_MACD_SIGNALS.value] = signals

            except Exception as e:
                self.print_error_updating_indicator(symbol, "MACD", e)
                continue

        return data_frame 

    
    def updating_stochrsi(self, config_stoch_rsi:ConfigSTOCHrsi = ConfigSTOCHrsi(), time_range:TimeRanges = None, data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0):
        
        long_stoch = config_stoch_rsi.longitud_stoch
        long_rsi = config_stoch_rsi.longitud_rsi
        smooth_k = config_stoch_rsi.smooth_k
        smooth_d = config_stoch_rsi.smooth_d

        blue_line = "STOCHRSIk_" + str(long_stoch) + "_" + str(long_rsi) + "_" + str(smooth_k) + "_" + str(smooth_d)
        red_line = "STOCHRSId_" + str(long_stoch) + "_" + str(long_rsi) + "_" + str(smooth_k) + "_" + str(smooth_d)
        
        data_frame = DataFrameCheckUtil.create_rsi_stoch_columns(data_frame=data_frame)

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]

                open_time_arr = numpy.array(prices_history['Close time'].values)                
                high = prices_history['High'].astype(float)
                low = prices_history['Low'].astype(float)
                close = prices_history['Close'].astype(float)   

                stochrsi = pandas_ta.stochrsi(close = close, low = low, high = high, rsi_length = long_rsi, length = long_stoch, k = smooth_k, d = smooth_d)
                stochrsi_k_numpy = numpy.array(stochrsi[blue_line])
                stochrsi_d_numpy = numpy.array(stochrsi[red_line])
                stochrsi_k_numpy = stochrsi_k_numpy[~numpy.isnan(stochrsi_k_numpy)]
                stochrsi_d_numpy = stochrsi_d_numpy[~numpy.isnan(stochrsi_d_numpy)]
                
                #data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE.value][ind] = stochrsi_k_numpy
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value] = self.get_last_element(element_list = stochrsi_k_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value] = self.list_is_ascending(check_list = stochrsi_k_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_GOOD_LINE_ANGLE.value] = self.angle_one_line(line_points = stochrsi_k_numpy, time_points = open_time_arr, time_range = time_range)
                
                #data_frame[DataFrameColum.RSI_STOCH_BAD_LINE.value][ind] = stochrsi_d_numpy
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_BAD_LINE_LAST.value] = self.get_last_element(element_list = stochrsi_d_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_BAD_LINE_ASCENDING.value] = self.list_is_ascending(check_list = stochrsi_d_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_BAD_LINE_ANGLE.value] = self.angle_one_line(line_points = stochrsi_d_numpy, time_points = open_time_arr, time_range = time_range)
                
                data_frame.loc[ind,DataFrameColum.RSI_STOCH_CRUCE_LINE.value] = self.good_indicator_on_top_of_bad(good_series = stochrsi_k_numpy, bad_series = stochrsi_d_numpy, ascending_count = ascending_count, previous_period = previous_period)

            except Exception as e:
                self.print_error_updating_indicator(symbol, "STOCH RSI", e)
                continue
        
        return data_frame
    


    def updating_stoch(self, config_stoch:ConfigSTOCH=ConfigSTOCH(), time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3):
        
        #Configuracion Gabri
        series = config_stoch.series
        d = config_stoch.d
        smooth = config_stoch.smooth
        
        blue_line = "STOCHk_" + str(series) + "_" + str(d) + "_" + str(smooth)
        red_line = "STOCHd_" + str(series) + "_" + str(d) + "_" + str(smooth)
        
        data_frame = DataFrameCheckUtil.create_stoch_columns(data_frame=data_frame)

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]

                high = prices_history['High'].astype(float)
                low = prices_history['Low'].astype(float)
                close = prices_history['Close'].astype(float)   
                
                stoch = pandas_ta.stoch(close = close, low = low, high = high, k = series, d = d, smooth_k= smooth)
                
                stoch_k_numpy = numpy.array(stoch[blue_line])
                stoch_d_numpy = numpy.array(stoch[red_line])
                
                stoch_k_numpy = stoch_k_numpy[~numpy.isnan(stoch_k_numpy)]
                stoch_d_numpy = stoch_d_numpy[~numpy.isnan(stoch_d_numpy)]
                
                data_frame[DataFrameColum.STOCH_GOOD_LINE.value][ind] = stoch_k_numpy
                data_frame[DataFrameColum.STOCH_BAD_LINE.value][ind] = stoch_d_numpy
                data_frame[DataFrameColum.STOCH_ASCENDING.value][ind] = self.list_is_ascending(check_list = stoch_k_numpy, ascending_count = ascending_count)
                data_frame[DataFrameColum.STOCH_CRUCE_LINE.value][ind] = self.good_indicator_on_top_of_bad(stoch_k_numpy, stoch_d_numpy, ascending_count)
                data_frame[DataFrameColum.STOCH_LAST.value][ind] = stoch_k_numpy[-1]
                
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "STOCH", e)
                continue
        
        return data_frame
    
    def updating_cci(self, config_cci:ConfigCCI=ConfigCCI(), time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3):
                
        series = config_cci.series
        
        data_frame = DataFrameCheckUtil.create_cci_columns(data_frame=data_frame)

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]
                
                high_prices = prices_history['High'].astype(float)
                low_prices = prices_history['Low'].astype(float)
                close_prices = prices_history['Close'].astype(float) 
                
                cci =  pandas_ta.cci(low= low_prices, close= close_prices, high= high_prices, length=series)
 
                cci_numpy = numpy.array(cci)       
                cci_numpy = cci_numpy[~numpy.isnan(cci_numpy)]
        
                data_frame[DataFrameColum.CCI.value][ind] = cci_numpy
                data_frame[DataFrameColum.CCI_LAST.value][ind] = cci_numpy[-1]
                data_frame[DataFrameColum.CCI_ASCENDING.value][ind] = self.list_is_ascending(check_list = cci_numpy, ascending_count = ascending_count)

            except Exception as e:
                self.print_error_updating_indicator(symbol, "CCI", e)
                continue
        
        return data_frame


    def updating_tsi(self, config_tsi:ConfigTSI=ConfigTSI(), time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3):
        
        #Configuracion Gabri

        long = config_tsi.long
        short = config_tsi.short
        siglen = config_tsi.siglen
        
        blue_line = "TSI_" + str(long) + "_" + str(short) + "_" + str(siglen)
        red_line = "TSIs_" + str(long) + "_" + str(short) + "_" + str(siglen)
        
        data_frame = DataFrameCheckUtil.create_tsi_columns(data_frame=data_frame)

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]

                close = prices_history['Close'].astype(float)   
                
                tsi = pandas_ta.tsi(close = close, fast = long, signal=siglen, slow=short)
                
                tsi_numpy = numpy.array(tsi[blue_line])
                tsi_s_numpy = numpy.array(tsi[red_line])
                
                tsi_numpy = tsi_numpy[~numpy.isnan(tsi_numpy)]
                tsi_s_numpy = tsi_s_numpy[~numpy.isnan(tsi_s_numpy)]
                
                data_frame[DataFrameColum.TSI_GOOD_LINE.value][ind] = tsi_numpy
                data_frame[DataFrameColum.TSI_BAD_LINE.value][ind] = tsi_s_numpy
                data_frame[DataFrameColum.TSI_ASCENDING.value][ind] = self.list_is_ascending(check_list = tsi_numpy, ascending_count = ascending_count)
                data_frame[DataFrameColum.TSI_CRUCE_LINE.value][ind] = self.good_indicator_on_top_of_bad(tsi_numpy, tsi_s_numpy, ascending_count)
                data_frame[DataFrameColum.TSI_LAST.value][ind] = tsi_numpy[-1]
                
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "TSI", e)
                continue
        
        return data_frame


    def updating_ma(self, config_ma:ConfigMA=ConfigMA(), time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):
        
        length = config_ma.length
        type = config_ma.type
        
        data_frame = DataFrameCheckUtil.create_ma_columns(data_frame=data_frame)
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]

                close = prices_history['Close'].astype(float)
                open_price = prices_history['Open'].astype(float)
                open_price_arr = numpy.array(open_price)
                close_price_arr = numpy.array(close)
                  
                #ma = pandas_ta.ma("ema", close, length = length)
                ma = pandas_ta.ma(type, close, length = length)
                ma_numpy = numpy.array(ma)
                ma_numpy = ma_numpy[~numpy.isnan(ma_numpy)]
                                
                data_frame[DataFrameColum.MA.value][ind] = ma_numpy
                data_frame.loc[ind, DataFrameColum.MA_ASCENDING.value] = self.list_is_ascending(check_list = ma_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MA_LAST.value] = self.get_last_element(element_list = ma_numpy, previous_period = previous_period)
                #data_frame.loc[ind, DataFrameColum.MA_LAST_ANGLE.value] = MathCal_util.angle(values=ma_numpy, time_range=time_range)
                #data_frame.loc[ind, DataFrameColum.MA_OPEN_PRICE_PERCENTAGE.value] = (self.get_last_element(element_list = open_price_arr) * 100.0 / self.get_last_element(element_list = ma_numpy)) - 100
                data_frame.loc[ind, DataFrameColum.MA_CLOSE_PRICE_PERCENTAGE.value] = (self.get_last_element(element_list = close_price_arr) * 100.0 / self.get_last_element(element_list = ma_numpy)) - 100
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "MA", e)
                continue
        
        return data_frame

    def update_percentage_profit(self, data_frame:pandas.DataFrame=pandas.DataFrame()) -> pandas.DataFrame:
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            previous_price = data_frame[DataFrameColum.PRICE_BUY.value][ind]
            currentPrice = self.client_bit.client_bit.mix_get_single_symbol_ticker(symbol=symbol)['data']['last']
            profit =  (float(currentPrice)*100.0 / float(previous_price))-100

            previous_profit = data_frame.loc[ind, DataFrameColum.PERCENTAGE_PROFIT.value]
            if previous_profit < profit or previous_profit == profit:
                data_frame.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_ASCENDING.value] = True
            else:
                data_frame.loc[ind, DataFrameColum.PERCENTAGE_PROFIT_ASCENDING.value] = False
            data_frame.loc[ind, DataFrameColum.PERCENTAGE_PROFIT.value] = round(profit, 5)


        return data_frame

    def updating_trix(self, config_trix:ConfigTrix=ConfigTrix(), time_range:TimeRanges=None, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3):

        length = config_trix.length
        signal = config_trix.signal

        trix_line = "TRIX_" + str(length) + "_" + str(signal)
        
        data_frame = DataFrameCheckUtil.create_trix_columns(data_frame=data_frame)
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                if prices_history_dict == None:
                    prices_history = self.client_bit.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
                else:
                    prices_history = prices_history_dict[symbol]

                open_time_arr = numpy.array(prices_history['Close time'].values)
                close = prices_history['Close'].astype(float) 
                  
                trix = pandas_ta.trix(close = close, length = length)
                trix_numpy = numpy.array(trix)

                trix_numpy = numpy.array(trix[trix_line])

                trix_numpy = trix_numpy[~numpy.isnan(trix_numpy)]

                data_frame[DataFrameColum.TRIX.value][ind] = trix_numpy
                data_frame[DataFrameColum.TRIX_ASCENDING.value][ind] = self.list_is_ascending(check_list = trix_numpy, ascending_count = ascending_count)
                data_frame[DataFrameColum.TRIX_LAST.value][ind] = trix_numpy[-1]
                data_frame[DataFrameColum.TRIX_ANGLE.value][ind] = self.angle_one_line(line_points = trix_numpy, time_points = open_time_arr, time_range = time_range)
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "TRIX", e)
                continue
        
        return data_frame
    
    def get_last_element(self, element_list:numpy, previous_period:int = 0):
        
        last_element_position = -1 - previous_period
        element_value = element_list[last_element_position]
        return element_value
    
    def list_is_ascending(self, check_list:numpy=[], ascending_count:int = 3, previous_period:int = 0) -> bool:

        elements = check_list[-ascending_count:]
        is_ascending = all(elements[i] <= elements[i+1] for i in range(len(elements)-1))
        return is_ascending
 
    def cruce_zero(self, series:numpy) -> str:

        last_val = self.remove_trailing_zeros(series[-1])
        prev_val = self.remove_trailing_zeros(series[-2])
        
        if last_val > 0 and prev_val <= 0:
            return ColumLineValues.ZERO_CRUCE_TOP.value
        elif last_val < 0 and prev_val >= 0:
            return ColumLineValues.ZERO_CRUCE_DOWN.value
        else:
            return "-"
    
    def remove_trailing_zeros(self, number: float) -> float:
        num_str = str(float(number))
        if num_str.startswith('0'):
            num_str = num_str.rstrip('0').rstrip('.')
        return float(num_str)
        
    # ELIMINAR ascending_count:int = 3 ???
    def good_indicator_on_top_of_bad(self, good_series:numpy, bad_series:numpy, ascending_count:int = 3, previous_period:int = 0) -> str:
        
        elements = 3
        count_elements = elements + previous_period

        line_good:numpy = good_series[-count_elements:]
        line_bad:numpy = bad_series[-count_elements:]

        if previous_period > 0:
            line_good = line_good[0:elements]
            line_bad = line_bad[0:elements]

        position = 0
        line_good_count = 0
        line_bad_count = 0

        for point_good in line_good:
            if point_good > line_bad[position]:
                line_good_count+= 1
            else:
                line_bad_count+= 1
            position += 1
        
        if line_good_count == count_elements:
            return ColumLineValues.BLUE_TOP.value

        if line_bad_count == count_elements:
            return ColumLineValues.RED_TOP.value
        
        if line_good[1] <= line_bad[1]:
            if line_good[2] >= line_bad[2]:
                return ColumLineValues.BLUE_CRUCE_TOP.value
        
        return ColumLineValues.BLUE_CRUCE_DOWN.value
    
    def get_top_24h(self, data_frame:pandas.DataFrame=pandas.DataFrame()) -> pandas.DataFrame:
        """ Funcion que devuelve un data frame ordenado de mayor a menor por TOP_GAINERS (subida en las últimas 24h).
            Se calcula el desvio de 24h, cogiendo el primer valor y último de la lista de "Close"
        """
        dict_values = {}
        
        data_frame = DataFrameCheckUtil.create_top_gainers_columns(data_frame=data_frame)

        for ind in data_frame.index:
            symbol = data_frame.loc[ind, DataFrameColum.SYMBOL.value]
            prices_history_24h = self.client_bit.get_change_price_24h(symbol)
            dict_values[symbol] = prices_history_24h
        
        now =  datetime.now()
          
        for ind in data_frame.index:
                
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            prev_top_gainer = data_frame[DataFrameColum.TOP_GAINERS.value][ind]
            actual_top_gainer = dict_values[symbol]
            data_frame.loc[ind, DataFrameColum.TOP_GAINERS_ASCENDING.value] = True if actual_top_gainer > prev_top_gainer else False
            data_frame.loc[ind, DataFrameColum.TOP_GAINERS.value] = actual_top_gainer
            data_frame.loc[ind, DataFrameColum.TOP_GAINERS_DATE.value] = now

        return data_frame.sort_values(DataFrameColum.TOP_GAINERS.value, ascending=False)
   
    def update_take_profit(self, data_frame:pandas.DataFrame=pandas.DataFrame(), take_profit:float=1.5) -> pandas.DataFrame:
                 
        for ind in data_frame.index:
                
            data_frame[DataFrameColum.TAKE_PROFIT.value][ind] = take_profit
        
        return data_frame
    
    def adx_angle(self, list_adx:numpy=[], previous_period:int = 0) -> float:
        
        if previous_period > 0:
            list_adx = list_adx[previous_period:]
            
        pos_adx_3 = list_adx[-3]
        pos_adx_2 = list_adx[-2]
        pos_adx_1 = list_adx[-1]
        
        if (pos_adx_3 > pos_adx_2) and (pos_adx_2 < pos_adx_1):

            list_data_prev = [pos_adx_3, pos_adx_2]
            angle_prev = float(MathCal_util.calculate_angle(list_data_prev, int_eje_x = 1 ))

            list_data_crrnt = [pos_adx_2, pos_adx_1]
            angle_crrnt = float(MathCal_util.calculate_angle(list_data_crrnt, int_eje_x = 1 ))

            sum_angles = 180 - (abs(angle_prev) + angle_crrnt)
            
        elif (pos_adx_1 > pos_adx_2) and (pos_adx_2 < pos_adx_3):

            list_data_prev = [pos_adx_3, pos_adx_2]
            angle_prev = float(MathCal_util.calculate_angle(list_data_prev, int_eje_x = 1 ))

            list_data_crrnt = [pos_adx_2, pos_adx_1]
            angle_crrnt = float(MathCal_util.calculate_angle(list_data_crrnt, int_eje_x = 1 ))

            sum_angles = 180 - (abs(angle_prev) + angle_crrnt)
            
        else:
            return pandas.NaT
        
        return sum_angles
    
    def print_error_updating_indicator(self, symbol, indicator, e):
        
        print("Symbol " + str(symbol))
        print("Error creando " + str(indicator))
        print(str(e))
        print("Posible nueva cripto " + str(symbol))
