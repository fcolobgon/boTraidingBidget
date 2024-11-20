#import logging
from datetime import datetime
from typing import List
import pandas
import numpy
from math import atan
from decimal import Decimal
from ta.trend import ADXIndicator
import pandas_ta 

from src.botrading.utils import koncorde
from src.botrading.utils import traiding_operations
from src.botrading.utils.math_calc_util import MathCal_util
from src.botrading.constants import botrading_constant
from src.botrading.model.indocators import *
from src.botrading.model.time_ranges import *
from src.botrading.bit import BitgetClienManager
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils import excel_util
from src.botrading.utils.enums.colum_state_values import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.colum_good_bad_values import ColumLineValues


from configs.config import settings as settings

     
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
            """
            if settings.BITGET_CLIENT_TEST_MODE == True:
                all_coins_df[DataFrameColum.SYMBOL_TEST.value] = all_coins_df.apply(lambda row: textUtil.convert_text_mode_test(row[DataFrameColum.BASE.value], row[DataFrameColum.QUOTE.value], row[DataFrameColum.SYMBOL.value]), axis=1)
                columnas_a_mantener = [DataFrameColum.BASE.value, DataFrameColum.QUOTE.value, 
                                DataFrameColum.SYMBOL.value, DataFrameColum.SYMBOL_TEST.value, DataFrameColum.SYMBOLNAME.value, DataFrameColum.SYMBOLTYPE.value, 
                                DataFrameColum.TAKERFEERATE.value, DataFrameColum.VOLUMEPLACE.value ]  # Índices de las columnas que deseas mantener (0-indexed)
            else:
            """
            columnas_a_mantener = [DataFrameColum.BASE.value, DataFrameColum.QUOTE.value, 
                                DataFrameColum.SYMBOL.value, DataFrameColum.SYMBOLNAME.value, DataFrameColum.SYMBOLTYPE.value, DataFrameColum.TAKERFEERATE.value, 
                                DataFrameColum.VOLUMEPLACE.value, DataFrameColum.PRICEPLACE.value, DataFrameColum.PRICEENDSTEP.value,DataFrameColum.BUY_LIMIT_PRICE_RATIO.value,
                                DataFrameColum.MIN_TRADE_NUM.value, DataFrameColum.SIZE_MULTIPLIER.value]  # Índices de las columnas que deseas mantener (0-indexed)
            
            self.data_frame_bkp = self.data_frame_bkp.loc[:,columnas_a_mantener]
            

            #Columnas necesarias de arranque
            #self.data_frame_bkp.loc[:,DataFrameColum.SYMBOL.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.STATE.value] = ColumStateValues.WAIT.value
            self.data_frame_bkp.loc[:,DataFrameColum.ORDER_OPEN.value] = False
            self.data_frame_bkp.loc[:,DataFrameColum.ORDER_ID.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.CLIENT_ORDER_ID.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.DATE.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.PRICE_BUY.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.PRICE_SELL.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.CLOSE.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.DATE_PRICE_BUY.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.LOCK.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.LOOK.value] = False
            self.data_frame_bkp.loc[:,DataFrameColum.FIRST_ITERATION.value] = True
            self.data_frame_bkp.loc[:,DataFrameColum.SIDE_TYPE.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.MONEY_SPENT.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.SIZE.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.ROE.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.PNL.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.PRESET_STOP_LOSS_PRICE.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.PRESET_TAKE_PROFIT_PRICE.value] = 0.0

            self.data_frame_bkp.loc[:,DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = True
            self.data_frame_bkp.loc[:,DataFrameColum.PERCENTAGE_PROFIT.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.PERCENTAGE_PROFIT_PREV.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.PERCENTAGE_PROFIT_ASCENDING.value] = False
            self.data_frame_bkp.loc[:,DataFrameColum.TAKE_PROFIT.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.TAKE_PROFIT_TOUCH.value] = False
            self.data_frame_bkp.loc[:,DataFrameColum.STOP_LOSS.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.STOP_LOSS_LEVEL.value] = 0.0
            
            self.data_frame_bkp.loc[:,DataFrameColum.LEVEREAGE.value] = 0  

            self.data_frame_bkp.loc[:,DataFrameColum.NOTE.value] = "-"  
            self.data_frame_bkp.loc[:,DataFrameColum.NOTE_2.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.NOTE_3.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.NOTE_4.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.NOTE_5.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.ERROR.value] = "-"
            self.data_frame_bkp.loc[:,DataFrameColum.FEE_BUY.value] = 0.0
            self.data_frame_bkp.loc[:,DataFrameColum.FEE_SELL.value] = 0.0

            self.data_frame_bkp = DataFrameCheckUtil.create_price_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_adx_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_ao_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_rsi_stoch_columns(data_frame=self.data_frame_bkp)
            self.data_frame_bkp = DataFrameCheckUtil.create_ma_columns(data_frame=self.data_frame_bkp)
            #self.data_frame_bkp = DataFrameCheckUtil.create_soporte_resistencia_columns(data_frame=self.data_frame_bkp)
    
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

            prices_history['Open'] = prices_history['Open'].astype(float)
            prices_history['High'] = prices_history['High'].astype(float)
            prices_history['Low'] = prices_history['Low'].astype(float)
            prices_history['Close'] = prices_history['Close'].astype(float)
            prices_history['Volume'] = prices_history['Volume'].astype(float)
            
            dict_values[symbol] = prices_history

        return dict_values
    
    def updating_open_orders(self, data_frame:pandas.DataFrame=pandas.DataFrame(), startTime:datetime=None):
        
        df = traiding_operations.get_open_positions(clnt_bit=self.client_bit)
        
        if df.empty:
            data_frame[DataFrameColum.ORDER_OPEN.value] = False

        for ind in data_frame.index:
            
            symbol = data_frame.loc[ind, DataFrameColum.BASE.value]
            sideType = data_frame.loc[ind, DataFrameColum.SIDE_TYPE.value]

            row_values = df[df["symbol"].str.contains(symbol)& (df['holdSide'] == sideType)] 

            excel_util.save_data_frame( data_frame=row_values, exel_name="buy.xlsx")

            if row_values.empty == False:

                profit = float(row_values["unrealizedPL"].values[0] )  
                data_frame.loc[ind, DataFrameColum.PERCENTAGE_PROFIT.value] = profit
                
                position = float(row_values["total"].values[0])
                
                if position == 0:
                    data_frame.loc[ind, DataFrameColum.ORDER_OPEN.value] = False
            
            
        return data_frame

    def updating_pnl_roe_orders(self, data_frame:pandas.DataFrame=pandas.DataFrame(), startTime:datetime=None):
        
        df = traiding_operations.get_open_positions(clnt_bit=self.client_bit)

        excel_util.save_data_frame( data_frame=df, exel_name="buy.xlsx")

        if df.empty:
            data_frame.loc[:,DataFrameColum.ORDER_OPEN.value] = False

            return data_frame


        for ind in data_frame.index:
            
            symbol = data_frame.loc[ind, DataFrameColum.BASE.value]
            sideType = data_frame.loc[ind, DataFrameColum.SIDE_TYPE.value]

            row_values = df[df["symbol"].str.contains(symbol)& (df['holdSide'] == sideType)]             

            if row_values.empty == False:

                pnl = float(row_values["unrealizedPL"].values[0] )
                roe = (pnl/ float(row_values["margin"].values[0] ))*100

                data_frame.loc[ind, DataFrameColum.ROE.value] = roe
                data_frame.loc[ind, DataFrameColum.PNL.value] = pnl
            
            else:
                data_frame.loc[ind, DataFrameColum.ORDER_OPEN.value] = False

            
            
        return data_frame
    
    def updating_price_indicators(self, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):
        
        data_frame = DataFrameCheckUtil.create_price_columns(data_frame=data_frame)
        
        for ind in data_frame.index:
            
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
            
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
                data_frame.loc[ind, DataFrameColum.PRICE_OPEN_ASCENDING.value] = self.verificar_ascendente(check_list = open_numpy)
                data_frame.loc[ind, DataFrameColum.PRICE_CLOSE.value] = self.get_last_element(element_list = close_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.PRICE_PERCENTAGE_PREV.value] = data_frame.loc[ind, DataFrameColum.PRICE_PERCENTAGE.value]
                data_frame.loc[ind, DataFrameColum.PRICE_PERCENTAGE.value] = (self.get_last_element(element_list = close_numpy, previous_period = previous_period) * 100.0 / self.get_last_element(element_list = open_numpy, previous_period = previous_period)) - 100
                data_frame.loc[ind, DataFrameColum.PRICE_CLOSE_ASCENDING.value] = self.verificar_ascendente(check_list = close_numpy)
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

    def updating_adx(self, config_adx:ConfigADX=ConfigADX(), data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):
        
        series = config_adx.series
        
        data_frame = DataFrameCheckUtil.create_adx_columns(data_frame=data_frame)

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                prices_history = prices_history_dict[symbol]
                
                prices_high = prices_history['High'].astype(float)
                prices_low = prices_history['Low'].astype(float)
                prices_close = prices_history['Close'].astype(float)
                open_time_arr = numpy.array(prices_history['Close time'].values)  

                adx = ADXIndicator(high = prices_high, low = prices_low, close = prices_close, window= series).adx()
                adx_numpy = numpy.array(adx)
                adx_numpy = adx_numpy[~numpy.isnan(adx_numpy)]
                data_frame[DataFrameColum.ADX.value][ind] = adx_numpy
                data_frame.loc[ind, DataFrameColum.ADX_LAST.value] = self.get_last_element(element_list = adx_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.ADX_ASCENDING.value] = self.list_is_ascending(check_list = adx_numpy, ascending_count = ascending_count, previous_period = previous_period)
                #data_frame.loc[ind, DataFrameColum.ADX_ANGLE.value] = self.angle_one_line(line_points = adx_numpy, time_points = open_time_arr, time_range = time_range)
                data_frame.loc[ind, DataFrameColum.ADX_ANGLE.value] = self.adx_angle(list_adx = adx_numpy, previous_period = previous_period)
                
                                                                    
            except Exception as e:
                self.print_error_updating_indicator(symbol, "ADX", e)
                continue
        
        return data_frame  
    
    def updating_koncorde_org(self, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None):

        length = 255
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                prices_history = prices_history_dict[symbol]
                
                high = numpy.array(prices_history['High'].astype(float))
                low = numpy.array(prices_history['Low'].astype(float))
                close = numpy.array(prices_history['Close'].astype(float))
                volume = numpy.array(prices_history['Volume'].astype(float))
                
                koncorde.calculate(prices_history)
                                                                    
            except Exception as e:
                self.print_error_updating_indicator(symbol, "KONCORDE", e)
                continue
        
        return data_frame 
    
    def updating_ao(self, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):

        data_frame = DataFrameCheckUtil.create_ao_columns(data_frame=data_frame)
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                
                prices_history = prices_history_dict[symbol]
                
                ao = pandas_ta.ao(high = prices_history['High'].astype(float), low = prices_history['Low'].astype(float))
                ao_numpy = numpy.array(ao)
                ao_numpy = ao_numpy[~numpy.isnan(ao_numpy)]
                data_frame[DataFrameColum.AO.value][ind] = ao_numpy
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
    
    def updating_stochrsi(self, config_stoch_rsi:ConfigSTOCHrsi = ConfigSTOCHrsi(), data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0):
        
        long_stoch = config_stoch_rsi.longitud_stoch
        long_rsi = config_stoch_rsi.longitud_rsi
        smooth_k = config_stoch_rsi.smooth_k
        smooth_d = config_stoch_rsi.smooth_d

        blue_line = "STOCHRSIk_" + str(long_stoch) + "_" + str(long_rsi) + "_" + str(smooth_k) + "_" + str(smooth_d)
        red_line = "STOCHRSId_" + str(long_stoch) + "_" + str(long_rsi) + "_" + str(smooth_k) + "_" + str(smooth_d)
        
        #data_frame = DataFrameCheckUtil.create_rsi_stoch_columns(data_frame=data_frame)

        if DataFrameColum.RSI_STOCH_GOOD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE.value] = "-"

        if DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value] = 0.0

        if DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value] = "-"

        if DataFrameColum.RSI_STOCH_GOOD_LINE_ANGLE.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE_ANGLE.value] = 0.0

        if DataFrameColum.RSI_STOCH_BAD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_BAD_LINE.value] = "-"           

        if DataFrameColum.RSI_STOCH_BAD_LINE_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_BAD_LINE_LAST.value] = 0.0

        if DataFrameColum.RSI_STOCH_BAD_LINE_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_BAD_LINE_ASCENDING.value] = "-"     

        if DataFrameColum.RSI_STOCH_BAD_LINE_ANGLE.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_BAD_LINE_ANGLE.value] = 0.0

        if DataFrameColum.RSI_STOCH_CRUCE_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_STOCH_CRUCE_LINE.value] = "-"     

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
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
                #data_frame.loc[ind, DataFrameColum.RSI_STOCH_GOOD_LINE_ANGLE.value] = self.angle_one_line(line_points = stochrsi_k_numpy, time_points = open_time_arr, time_range = time_range)
                
                #data_frame[DataFrameColum.RSI_STOCH_BAD_LINE.value][ind] = stochrsi_d_numpy
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_BAD_LINE_LAST.value] = self.get_last_element(element_list = stochrsi_d_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.RSI_STOCH_BAD_LINE_ASCENDING.value] = self.list_is_ascending(check_list = stochrsi_d_numpy, ascending_count = ascending_count, previous_period = previous_period)
                #data_frame.loc[ind, DataFrameColum.RSI_STOCH_BAD_LINE_ANGLE.value] = self.angle_one_line(line_points = stochrsi_d_numpy, time_points = open_time_arr, time_range = time_range)
                
                data_frame.loc[ind,DataFrameColum.RSI_STOCH_CRUCE_LINE.value] = self.good_indicator_on_top_of_bad(good_series = stochrsi_k_numpy, bad_series = stochrsi_d_numpy, ascending_count = ascending_count, previous_period = previous_period)

            except Exception as e:
                self.print_error_updating_indicator(symbol, "STOCH RSI", e)
                continue
        
        return data_frame

    def updating_ma(self, config_ma:ConfigMA=ConfigMA(), data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0, scalar = bool):
        
        length = config_ma.length
        type = config_ma.type
        
        #data_frame = DataFrameCheckUtil.create_ma_columns(data_frame=data_frame)

        if DataFrameColum.MA.value not in data_frame.columns:
            data_frame[DataFrameColum.MA.value] = "-"

        if DataFrameColum.MA_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.MA_LAST.value] = 0.0

        if DataFrameColum.MA_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.MA_ASCENDING.value] = "-"

        if DataFrameColum.MA_LAST_ANGLE.value not in data_frame.columns:
            data_frame[DataFrameColum.MA_LAST_ANGLE.value] = 0.0

        if DataFrameColum.MA_OPEN_PRICE_PERCENTAGE.value not in data_frame.columns:
            data_frame[DataFrameColum.MA_OPEN_PRICE_PERCENTAGE.value] = 0.0

        if DataFrameColum.MA_CLOSE_PRICE_PERCENTAGE.value not in data_frame.columns:
            data_frame[DataFrameColum.MA_CLOSE_PRICE_PERCENTAGE.value] = 0.0
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:
                
                prices_history = prices_history_dict[symbol]



                close = prices_history['Close'].astype(float)
                open_price = prices_history['Open'].astype(float)
                #open_price_arr = numpy.array(open_price)
                #close_price_arr = numpy.array(close)
                  
                #ma = pandas_ta.ma("ema", close, length = length)
                ma = pandas_ta.ma(type, close, length = length)
                ma_numpy = numpy.array(ma)
                ma_numpy = ma_numpy[~numpy.isnan(ma_numpy)]
                                
                data_frame[DataFrameColum.MA.value][ind] = ma_numpy
                data_frame.loc[ind, DataFrameColum.MA_ASCENDING.value] = self.list_is_ascending(check_list = ma_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MA_LAST.value] = self.get_last_element(element_list = ma_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MA_LAST_ANGLE.value] = self.angle(ma_numpy, last_numbers = 20)
                #data_frame.loc[ind, DataFrameColum.MA_OPEN_PRICE_PERCENTAGE.value] = (self.get_last_element(element_list = open_price_arr) * 100.0 / self.get_last_element(element_list = ma_numpy)) - 100
                #data_frame.loc[ind, DataFrameColum.MA_CLOSE_PRICE_PERCENTAGE.value] = (self.get_last_element(element_list = close_price_arr) * 100.0 / self.get_last_element(element_list = ma_numpy)) - 100
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "MA", e)
                continue
        
        return data_frame
    
    def updating_macd(self, config_macd:ConfigMACD=ConfigMACD(), time_range:TimeRanges = None, data_frame:pandas.DataFrame = pandas.DataFrame(), prices_history_dict:dict = None, ascending_count:int = 3, previous_period:int = 0):
        
        fast=config_macd.fast
        slow=config_macd.slow
        signal=config_macd.signal

        if DataFrameColum.MACD_GOOD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_GOOD_LINE.value] = "-"

        if DataFrameColum.MACD_BAD_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_BAR_CHART.value] = "-"

        if DataFrameColum.MACD_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_LAST.value] = "-"

        if DataFrameColum.MACD_LAST_CHART.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_LAST_CHART.value] = "-"
    
        if DataFrameColum.MACD_PREVIOUS_CHART.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_PREVIOUS_CHART.value] = "-"

        if DataFrameColum.MACD_CHART_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CHART_ASCENDING.value] = "-"

        if DataFrameColum.MACD_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_ASCENDING.value] = "-"

        if DataFrameColum.MACD_CRUCE_LINE.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CRUCE_LINE.value] = "-"

        if DataFrameColum.MACD_CRUCE_ZERO.value not in data_frame.columns:
            data_frame[DataFrameColum.MACD_CRUCE_ZERO.value] = "-"


        chars = "MACDh_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        blue_line = "MACD_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        red_line = "MACDs_" + str(fast) + "_" + str(slow) + "_" + str(signal)
        
        data_frame = DataFrameCheckUtil.create_macd_columns(data_frame=data_frame)
                        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                
                if prices_history_dict == None:
                    prices_history = self.bnb_client.get_historial_x_day_ago(symbol, time_range.x_days, time_range.interval)
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
                data_frame[DataFrameColum.MACD_GOOD_LINE.value][ind] = macd_numpy
                data_frame[DataFrameColum.MACD_BAD_LINE.value][ind] = macd_s_numpy
                data_frame.loc[ind, DataFrameColum.MACD_LAST.value] = self.get_last_element(element_list = macd_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_LAST_CHART.value] = self.get_last_element(element_list = macd_h_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.MACD_PREVIOUS_CHART.value] = macd_h_numpy[-2]
                data_frame.loc[ind, DataFrameColum.MACD_CHART_ASCENDING.value] = self.list_is_ascending(check_list = macd_h_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.MACD_ASCENDING.value] = self.list_is_ascending(check_list = macd_numpy, ascending_count = ascending_count)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_LINE.value] = self.good_indicator_on_top_of_bad(macd_numpy, macd_s_numpy)
                data_frame.loc[ind, DataFrameColum.MACD_CRUCE_ZERO.value] = self.cruce_zero(macd_h_numpy)
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "MACD", e)
                continue
        
        return data_frame 

    def updating_rsi(self, length:int = 14, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 2, previous_period:int = 0):

        if DataFrameColum.RSI.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI.value] = None

        if DataFrameColum.RSI_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_ASCENDING.value] = None
    
        if DataFrameColum.RSI_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.RSI_LAST.value] = None

        for ind in data_frame.index:
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            try:                
                prices_history = prices_history_dict[symbol]

                close = prices_history['Close'].astype(float)   

                rsi = pandas_ta.rsi(length=length, close=close )

                rsi_numpy = numpy.array(rsi)
                rsi_numpy = rsi_numpy[~numpy.isnan(rsi_numpy)]

                data_frame[DataFrameColum.RSI.value][ind] = rsi_numpy
                data_frame.loc[ind, DataFrameColum.RSI_LAST.value] = rsi_numpy[-1]
                data_frame.loc[ind,DataFrameColum.RSI_ASCENDING.value] = self.list_is_ascending(check_list = rsi_numpy, ascending_count = ascending_count)
                                                                    
            except Exception as e:
                self.print_error_updating_indicator(symbol, "RSI", e)
                continue
        
        return data_frame 
    
    def updating_sma(self, length:int = 14, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None, ascending_count:int = 3, previous_period:int = 0):
        
        if DataFrameColum.SMA.value not in data_frame.columns:
            data_frame[DataFrameColum.SMA.value] = "-"

        if DataFrameColum.SMA_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.SMA_LAST.value] = 0.0

        if DataFrameColum.SMA_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.SMA_ASCENDING.value] = "-"
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:                
                prices_history = prices_history_dict[symbol]

                close = prices_history['Close'].astype(float)
                  
                sma = pandas_ta.ma(type, close, length = length)
                sma_numpy = numpy.array(sma)
                sma_numpy = sma_numpy[~numpy.isnan(sma_numpy)]
                                
                data_frame[DataFrameColum.SMA.value][ind] = sma_numpy
                data_frame.loc[ind, DataFrameColum.SMA_ASCENDING.value] = self.list_is_ascending(check_list = sma_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.SMA_LAST.value] = self.get_last_element(element_list = sma_numpy, previous_period = previous_period)
               
            except Exception as e:
                self.print_error_updating_indicator(symbol, "SMA", e)
                continue
        
        return data_frame

    def bbwp(self, prices_history_dict:dict=None, data_frame:pandas.DataFrame=pandas.DataFrame(), length=13, window=252, ascending_count:int = 2, previous_period:int = 0):
        """
        Calcula el indicador BBWP (Bollinger Band Width Percentile).

        :param datos: DataFrame de pandas con una columna 'Close' (precios de cierre).
        :param periodo: Periodo para las Bandas de Bollinger (por defecto 20).
        :param ventana: Ventana para el cálculo del percentil (por defecto 252, equivalente a un año de trading).
        :return: Serie de pandas con los valores BBWP.
        """
        if DataFrameColum.BBWP.value not in data_frame.columns:
            data_frame[DataFrameColum.BBWP.value] = None

        if DataFrameColum.BBWP_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.BBWP_LAST.value] = None

        if DataFrameColum.BBWP.value not in data_frame.columns:
            data_frame[DataFrameColum.BBWP.value] = None
        
        for ind in data_frame.index:
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]

            try:
                
                prices_history = prices_history_dict[symbol]      

                close = prices_history['Close'].astype(float)   

                # Calcular las Bandas de Bollinger
                bbands = pandas_ta.bbands(close=close, length=length)
                # Calcular el ancho de las bandas
                bb_width = (bbands['BBU_' + str(length) + '_2.0'] - bbands['BBL_' + str(length) + '_2.0']) / bbands['BBM_' + str(length) + '_2.0']

                def percentileofscore(a, score):
                    """
                    Calcula el percentil de un valor dado en relación con un array.
                    
                    :param a: Array o Serie.
                    :param score: Valor para el cual se quiere calcular el percentil.
                    :return: Percentil (0-100).
                    """
                    a = numpy.asarray(a)
                    n = len(a)

                    return (numpy.sum(a < score) + 0.5 * numpy.sum(a == score)) / n * 100
                
                # Calcular el BBWP
                bbwp = bb_width.rolling(window=window).apply(lambda x: percentileofscore(x, x.iloc[-1]))

                bbwp_numpy = numpy.array(bbwp)
                bbwp_numpy = bbwp_numpy[~numpy.isnan(bbwp_numpy)]

                data_frame[DataFrameColum.BBWP.value][ind] = bbwp_numpy
                data_frame.loc[ind, DataFrameColum.BBWP_LAST.value] = self.get_last_element(element_list = bbwp_numpy, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.BBWP_ASCENDING.value] = self.list_is_ascending(check_list = bbwp_numpy, ascending_count = ascending_count, previous_period = previous_period)
                
            except Exception as e:
                self.print_error_updating_indicator(symbol, "BBWP", e)
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
    
    def get_last_element(self, element_list:numpy, previous_period:int = 0):

        if previous_period > 0:
            element_value = element_list[:-previous_period]
        else:
            element_value = element_list

        return element_value[-1]
    """
    def list_is_ascending(self, check_list:numpy=[], ascending_count:int = 3, previous_period:int = 0) -> bool:

        #elements = check_list[-ascending_count:]
        #is_ascending = all(elements[i] <= elements[i+1] for i in range(len(elements)-1))
        return check_list[-1] > check_list[-2]
    """
        
    def list_is_ascending(self, check_list:numpy=[], ascending_count:int= 2) -> bool:

        last_elements = check_list[-ascending_count:]

        # Verificamos si es ascendente
        is_ascending = all(last_elements[i] < last_elements[i + 1] for i in range(len(last_elements) - 1))
        
        # Verificamos si es descendente
        is_descending = all(last_elements[i] > last_elements[i + 1] for i in range(len(last_elements) - 1))

        if is_ascending:
            return True
        elif is_descending:
            return False
        else:
            return -1

        """
        if ((last_elements[-1] > last_elements[-2]) and (last_elements[-2] > last_elements[-3])):
            return True
        elif (last_elements[-1] < last_elements[-2]) and (last_elements[-2] < last_elements[-3]):
            return False
        else:
            return -1
        """
        
 
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
    """
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
    """
    def good_indicator_on_top_of_bad(self, line_good:numpy, line_bad:numpy) -> str:
        
        if (line_good[-2] < line_bad[-2]) and (line_good[-1] > line_bad[-1]): 
            return ColumLineValues.BLUE_CRUCE_TOP.value
        elif (line_good[-2] > line_bad[-2]) and (line_good[-1] < line_bad[-1]): 
            return ColumLineValues.BLUE_CRUCE_DOWN.value
        elif (line_good[-3] < 0) and (line_good[-2] > line_bad[-2]) and (line_good[-1] > line_bad[-1]): 
            return ColumLineValues.BLUE_TOP.value
        elif (line_bad[-3] < 0) and (line_good[-2] < line_bad[-2]) and (line_good[-1] < line_bad[-1]):
            return ColumLineValues.RED_TOP.value

    
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


    def angle(self, list, last_numbers:int):
        # Calcular la media de los precios de cierre
        #mean_close = df['Close'].mean()

        # Calcular la desviación estándar de los precios de cierre
        #std_close = df['Close'].std()
        result_list = list[-last_numbers:]
        df = pandas.DataFrame(result_list, columns=['values'])

        df['Index'] = numpy.arange(len(df))

        # Calcular la pendiente de la línea de regresión
        slope = numpy.cov(df['values'], df['Index'])[0, 1] / numpy.var(df['Index'])


        # Calcular el ángulo de regresión en radianes
        angle_radians = numpy.arctan2(slope, 1)

        # Convertir radianes a grados
        angle_degrees = numpy.rad2deg(angle_radians) % 180
        
        # Ajustar al rango deseado (-90 a 90 grados)
        if angle_degrees > 90:
            angle_degrees = angle_degrees - 180

        return angle_degrees
