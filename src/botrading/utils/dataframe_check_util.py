from numpy import double
from typing import List
import pandas
import datetime
from datetime import datetime, timedelta

from src.botrading.utils.enums.data_frame_colum import *


class DataFrameCheckUtil:
    
    @staticmethod
    def add_columns_to_dataframe(column_names, df:pandas.DataFrame):

        existing_columns = list(df.columns)
        columns_to_add = []

        for column in column_names:
            if column not in existing_columns:
                columns_to_add.append(column)

        if columns_to_add:
            new_columns = pandas.DataFrame(columns=columns_to_add)
            df = pandas.concat([df, new_columns], axis=1)

        return df

    @staticmethod
    def create_macd_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        
        colums = [
            DataFrameColum.MACD_LAST.value,
            DataFrameColum.MACD_ASCENDING.value,
            DataFrameColum.MACD_CHART_ASCENDING.value,
            DataFrameColum.MACD_LAST_CHART.value,
            DataFrameColum.MACD_PREVIOUS_CHART.value,
            DataFrameColum.MACD_BAR_CHART.value,
            DataFrameColum.MACD_GOOD_LINE.value,
            DataFrameColum.MACD_BAD_LINE.value,
            DataFrameColum.MACD_CRUCE_LINE.value,
            DataFrameColum.MACD_CRUCE_ZERO.value
            ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=colums, df=data_frame)
        
        data_frame[DataFrameColum.MACD_LAST.value] = 0.0
        data_frame[DataFrameColum.MACD_ASCENDING.value] = "-"
        data_frame[DataFrameColum.MACD_CHART_ASCENDING.value] = "-"
        data_frame[DataFrameColum.MACD_LAST_CHART.value] = 0.0
        data_frame[DataFrameColum.MACD_PREVIOUS_CHART.value] = 0.0
        data_frame[DataFrameColum.MACD_BAR_CHART.value] = "-"
        data_frame[DataFrameColum.MACD_GOOD_LINE.value] = "-"
        data_frame[DataFrameColum.MACD_BAD_LINE.value] = "-"
        data_frame[DataFrameColum.MACD_CRUCE_LINE.value] = "-"
        data_frame[DataFrameColum.MACD_CRUCE_ZERO.value] = "-"
        
        return data_frame
    
    @staticmethod
    def create_supertrend_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        
        colums = [
            DataFrameColum.SUPER_TREND_LAST.value,
            DataFrameColum.SUPER_TREND_1.value,
            DataFrameColum.SUPER_TREND_2.value,
            DataFrameColum.SUPER_TREND_3.value,
            DataFrameColum.SUPER_TREND_4.value
            ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=colums, df=data_frame)
        
        data_frame[DataFrameColum.SUPER_TREND_LAST.value] = False
        data_frame[DataFrameColum.SUPER_TREND_1.value] = False
        data_frame[DataFrameColum.SUPER_TREND_2.value] = False
        data_frame[DataFrameColum.SUPER_TREND_3.value] = False
        data_frame[DataFrameColum.SUPER_TREND_4.value] = False
        
        return data_frame
    
    @staticmethod
    def create_rsi_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        
        colums = [
            DataFrameColum.RSI.value,
            DataFrameColum.RSI_LAST.value,
            DataFrameColum.RSI_ASCENDING.value,
            DataFrameColum.RSI_ANGLE.value
            ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=colums, df=data_frame)
        
        data_frame[DataFrameColum.RSI.value] = "-"
        data_frame[DataFrameColum.RSI_LAST.value] = 0.0
        data_frame[DataFrameColum.RSI_ASCENDING.value] = "-"
        data_frame[DataFrameColum.RSI_ANGLE.value] = 0.0
        
        return data_frame
    
    @staticmethod
    def create_adx_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        
        colums = [
            DataFrameColum.ADX.value,
            DataFrameColum.ADX_LAST.value,
            DataFrameColum.ADX_ASCENDING.value,
            DataFrameColum.ADX_ANGLE.value
            ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=colums, df=data_frame)

        data_frame[DataFrameColum.ADX.value] = "-"
        data_frame[DataFrameColum.ADX_LAST.value] = 0.0
        data_frame[DataFrameColum.ADX_ASCENDING.value] = "-"
        data_frame[DataFrameColum.ADX_ANGLE.value] = 0.0
        
        return data_frame
    
    @staticmethod
    def create_stoch_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        
        colums = [
            DataFrameColum.STOCH_GOOD_LINE.value,
            DataFrameColum.STOCH_BAD_LINE.value,
            DataFrameColum.STOCH_CRUCE_LINE.value,
            DataFrameColum.STOCH_LAST.value,
            DataFrameColum.STOCH_ASCENDING.value
            ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=colums, df=data_frame)

        data_frame[DataFrameColum.STOCH_GOOD_LINE.value] = "-"
        data_frame[DataFrameColum.STOCH_BAD_LINE.value] = "-"
        data_frame[DataFrameColum.STOCH_CRUCE_LINE.value] = "-"
        data_frame[DataFrameColum.STOCH_LAST.value] = 0.0
        data_frame[DataFrameColum.STOCH_ASCENDING.value] = "-"
        
        return data_frame
        
    @staticmethod
    def create_rsi_stoch_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.RSI_STOCH_GOOD_LINE.value,
            DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value,
            DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value,
            DataFrameColum.RSI_STOCH_GOOD_LINE_ANGLE.value,
            DataFrameColum.RSI_STOCH_BAD_LINE.value,
            DataFrameColum.RSI_STOCH_BAD_LINE_LAST.value,
            DataFrameColum.RSI_STOCH_BAD_LINE_ASCENDING.value,
            DataFrameColum.RSI_STOCH_BAD_LINE_ANGLE.value,
            DataFrameColum.RSI_STOCH_CRUCE_LINE.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE.value] = "-"
        data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE_LAST.value] = 0.0
        data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE_ASCENDING.value] = "-"
        data_frame[DataFrameColum.RSI_STOCH_GOOD_LINE_ANGLE.value] = 0.0
        data_frame[DataFrameColum.RSI_STOCH_BAD_LINE.value] = "-"
        data_frame[DataFrameColum.RSI_STOCH_BAD_LINE_LAST.value] = 0.0
        data_frame[DataFrameColum.RSI_STOCH_BAD_LINE_ASCENDING.value] = "-"
        data_frame[DataFrameColum.RSI_STOCH_BAD_LINE_ANGLE.value] = 0.0
        data_frame[DataFrameColum.RSI_STOCH_CRUCE_LINE.value] = "-"
        
        return data_frame

    @staticmethod
    def create_ao_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.AO.value,
            DataFrameColum.AO_LAST.value,
            DataFrameColum.AO_ASCENDING.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.AO.value] = "-"
        data_frame[DataFrameColum.AO_LAST.value] = 0.0
        data_frame[DataFrameColum.AO_ASCENDING.value] = "-"
        
        return data_frame

    @staticmethod
    def create_cci_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.CCI.value,
            DataFrameColum.CCI_LAST.value,
            DataFrameColum.CCI_ASCENDING.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.CCI.value] = "-"
        data_frame[DataFrameColum.CCI_LAST.value] = 0.0
        data_frame[DataFrameColum.CCI_ASCENDING.value] = "-"
        
        return data_frame

    @staticmethod
    def create_tsi_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.TSI_GOOD_LINE.value,
            DataFrameColum.TSI_BAD_LINE.value,
            DataFrameColum.TSI_CRUCE_LINE.value,
            DataFrameColum.TSI_LAST.value,
            DataFrameColum.TSI_ASCENDING.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.TSI_GOOD_LINE.value] = "-"
        data_frame[DataFrameColum.TSI_BAD_LINE.value] = "-"
        data_frame[DataFrameColum.TSI_CRUCE_LINE.value] = "-"
        data_frame[DataFrameColum.TSI_LAST.value] = 0.0
        data_frame[DataFrameColum.TSI_ASCENDING.value] = "-"
        
        return data_frame

    @staticmethod
    def create_ma_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.MA.value,
            DataFrameColum.MA_LAST.value,
            DataFrameColum.MA_ASCENDING.value,
            DataFrameColum.MA_LAST_ANGLE.value,
            DataFrameColum.MA_OPEN_PRICE_PERCENTAGE.value,
            DataFrameColum.MA_CLOSE_PRICE_PERCENTAGE.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.MA.value] = "-"
        data_frame[DataFrameColum.MA_LAST.value] = 0.0
        data_frame[DataFrameColum.MA_ASCENDING.value] = "-"
        data_frame[DataFrameColum.MA_LAST_ANGLE.value] = 0.0
        data_frame[DataFrameColum.MA_OPEN_PRICE_PERCENTAGE.value] = 0.0
        data_frame[DataFrameColum.MA_CLOSE_PRICE_PERCENTAGE.value] = 0.0
        
        return data_frame

    @staticmethod
    def create_trix_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.TRIX.value,
            DataFrameColum.TRIX_LAST.value,
            DataFrameColum.TRIX_ASCENDING.value,
            DataFrameColum.TRIX_ANGLE.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.TRIX.value] = "-"
        data_frame[DataFrameColum.TRIX_LAST.value] = 0.0
        data_frame[DataFrameColum.TRIX_ASCENDING.value] = "-"
        data_frame[DataFrameColum.TRIX_ANGLE.value] = 0.0
        
        return data_frame
    
    @staticmethod
    def create_price_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.PRICE_VOLUME.value,
            DataFrameColum.PRICE_LOW.value,
            DataFrameColum.PRICE_LOW_ASCENDING.value,
            DataFrameColum.PRICE_HIGH.value,
            DataFrameColum.PRICE_HIGH_ASCENDING.value,
            DataFrameColum.PRICE_OPEN.value,
            DataFrameColum.PRICE_OPEN_ASCENDING.value,
            DataFrameColum.PRICE_CLOSE.value,
            DataFrameColum.PRICE_CLOSE_ASCENDING.value,
            DataFrameColum.PRICE_OPEN_TIME.value,
            DataFrameColum.PRICE_CLOSE_TIME.value,
            DataFrameColum.PRICE_PERCENTAGE.value,
            DataFrameColum.PRICE_PERCENTAGE_PREV.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame.loc[:,DataFrameColum.PRICE_VOLUME.value] = 0.0
        data_frame.loc[:,DataFrameColum.PRICE_LOW.value] = 0.0
        data_frame.loc[:,DataFrameColum.PRICE_LOW_ASCENDING.value] = "-"
        data_frame.loc[:,DataFrameColum.PRICE_HIGH.value] = 0.0
        data_frame.loc[:,DataFrameColum.PRICE_HIGH_ASCENDING.value] = "-"
        data_frame.loc[:,DataFrameColum.PRICE_OPEN.value] = 0.0
        data_frame.loc[:,DataFrameColum.PRICE_OPEN_ASCENDING.value] = "-"
        data_frame.loc[:,DataFrameColum.PRICE_CLOSE.value] = 0.0
        data_frame.loc[:,DataFrameColum.PRICE_CLOSE_ASCENDING.value] = "-"
        data_frame.loc[:,DataFrameColum.PRICE_OPEN_TIME.value] = "-"
        data_frame.loc[:,DataFrameColum.PRICE_CLOSE_TIME.value] = "-"
        data_frame.loc[:,DataFrameColum.PRICE_PERCENTAGE.value] = 0.0
        data_frame.loc[:,DataFrameColum.PRICE_PERCENTAGE_PREV.value] = 0.0
        
        return data_frame

    @staticmethod
    def create_candle_trend_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.CANDLE_TREND_NAME.value,
            DataFrameColum.CANDLE_TREND.value,
            DataFrameColum.CANDLE_TREND_PREV.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.CANDLE_TREND_NAME.value] = "-"
        data_frame[DataFrameColum.CANDLE_TREND.value] = "-"
        data_frame[DataFrameColum.CANDLE_TREND_PREV.value] = "-"
        
        return data_frame
    
    @staticmethod
    def create_soporte_resistencia_columns(data_frame:pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.SOPORTES.value,
            DataFrameColum.RESISTENCIAS.value,
            DataFrameColum.EN_SOPORTE.value,
            DataFrameColum.EN_RESISTENCIA.value,
            DataFrameColum.SOPORTE_RESISTENCIA_PERCENTAGE.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.SOPORTES.value] = False
        data_frame[DataFrameColum.RESISTENCIAS.value] = False
        data_frame[DataFrameColum.EN_SOPORTE.value] = "-"
        data_frame[DataFrameColum.EN_RESISTENCIA.value] = "-"
        data_frame[DataFrameColum.SOPORTE_RESISTENCIA_PERCENTAGE.value] = -1
        
        return data_frame
    
    @staticmethod
    def create_top_gainers_columns(data_frame: pandas.DataFrame) -> pandas.DataFrame:
        columns = [
            DataFrameColum.TOP_GAINERS.value,
            DataFrameColum.TOP_GAINERS_PERCENTAGE_UPS.value,
            DataFrameColum.TOP_GAINERS_DATE.value
        ]
        
        data_frame = DataFrameCheckUtil.add_columns_to_dataframe(column_names=columns, df=data_frame)
        
        data_frame[DataFrameColum.TOP_GAINERS.value] = 0.0
        data_frame[DataFrameColum.TOP_GAINERS_PERCENTAGE_UPS.value] = 0.0
        data_frame[DataFrameColum.TOP_GAINERS_DATE.value] = "-"
        
        return data_frame
        