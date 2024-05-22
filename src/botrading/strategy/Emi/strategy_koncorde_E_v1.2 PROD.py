import time
import numpy
import pandas_ta 

from src.botrading.model.indocators import *
from src.botrading.utils.bitget_data_util import BitgetDataUtil

from src.botrading.utils import koncorde
from src.botrading.model.time_ranges import *
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.dataframe_util import DataFrameUtil
from src.botrading.utils.dataframe_check_util import DataFrameCheckUtil
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.colum_good_bad_values import ColumLineValues
from src.botrading.utils.enums.future_values import FutureValues
from src.botrading.telegram.telegram_notify import TelegramNotify
from src.botrading.utils.price_util import PriceUtil
from src.botrading.utils import excel_util
from datetime import datetime, timedelta

from matplotlib import pyplot as plt
from sklearn.preprocessing import MinMaxScaler

from configs.config import settings as settings

class Strategy:
    name:str

    def __init__(self, name:str):

        self.name = name
        
        self.startTime = datetime.now()
        self.startTime = self.startTime.replace(hour=0, minute=0, second=0, microsecond=0)

    def apply_buy(self, bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        rules = [ColumStateValues.WAIT, ColumStateValues.SELL, ColumStateValues.ERR_BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame: pandas.DataFrame
        filtered_data_frame = data_frame.query(state_query)

        filtered_df_master = filtered_data_frame

        time_range = TimeRanges("HOUR_2") #DAY_1  HOUR_4  MINUTES_1

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range, limit=1000)
        filtered_data_frame = Strategy.updating_koncorde(bitget_data_util=bitget_data_util, data_frame=filtered_data_frame, prices_history_dict=prices_history)
        filtered_data_frame = bitget_data_util.updating_rsi(length=14, data_frame=filtered_data_frame, prices_history_dict=prices_history)
        
        #config_sma = ConfigMA(length=14, type="sma")
        #filtered_data_frame = bitget_data_util.updating_ma(config_ma = config_sma, data_frame=filtered_data_frame, prices_history_dict=prices_history)

        #excel_util.save_data_frame( data_frame=filtered_data_frame, exel_name="kcd.xlsx")
        
        """#! SHORT - CON CONFIRMACIÓN
        query = "((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") and (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] > KONCORDE_VERDE.str[-2]) and (KONCORDE_MEDIA.str[-2] > KONCORDE_AZUL.str[-2]))"
        query = query + " and ((KONCORDE_MEDIA.str[-3] < KONCORDE_VERDE.str[-3]) or (KONCORDE_MEDIA.str[-3] < KONCORDE_AZUL.str[-3]))"
        query = query + " and (" + DataFrameColum.RSI_LAST.value + " > 70)"
        df_short = filtered_data_frame.query(query)
        """

        #! SHORT - SIN CONFIRMACIÓN
        query = "((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") and (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] < KONCORDE_VERDE.str[-2]) or (KONCORDE_MEDIA.str[-2] < KONCORDE_AZUL.str[-2]))"
        df_short = filtered_data_frame.query(query)
                
        if df_short.empty == False:
            df_short = Strategy.updating_rsi_sma(bitget_data_util=bitget_data_util, length=14, data_frame=df_short)
            query = "((SMA.str[-2] > RSI.str[-2]) and (SMA.str[-1] > RSI.str[-1])) "
            df_short = df_short.query(query)

        """#! LONG - CON CONFIRMACIÓN
        query = "((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") or (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] < KONCORDE_VERDE.str[-2]) or (KONCORDE_MEDIA.str[-2] < KONCORDE_AZUL.str[-2]))"
        query = query + " and ((KONCORDE_MEDIA.str[-3] > KONCORDE_VERDE.str[-3]) and (KONCORDE_MEDIA.str[-3] > KONCORDE_AZUL.str[-3]))"
        query = query + " and (" + DataFrameColum.RSI_LAST.value + " < 30)"
        df_long = filtered_data_frame.query(query)
        """

        #! LONG - SIN CONFIRMACIÓN
        query = "((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") or (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] > KONCORDE_VERDE.str[-2]) and (KONCORDE_MEDIA.str[-2] > KONCORDE_AZUL.str[-2]))"
        df_long = filtered_data_frame.query(query)
        
        
        if df_long.empty == False:
            df_long = Strategy.updating_rsi_sma(bitget_data_util=bitget_data_util, length=14, data_frame=df_long)
            query = "((SMA.str[-2]) < (RSI.str[-2]) and (SMA.str[-1]) < (RSI.str[-1]))"
            df_long = df_long.query(query)

 
        #excel_util.save_data_frame( data_frame=df_long, exel_name="kcd_l.xlsx")
        #excel_util.save_data_frame( data_frame=df_short, exel_name="kcd_s.xlsx")

        if df_short.empty == False:
            df_short.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_SHORT.value
            df_short.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
            df_short.loc[:, DataFrameColum.LEVEREAGE.value] = 10
            df_short.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_short)

        if df_long.empty == False:
            df_long.loc[:, DataFrameColum.SIDE_TYPE.value] = FutureValues.SIDE_TYPE_LONG.value
            df_long.loc[:, DataFrameColum.PERCENTAGE_PROFIT_FLAG.value] = False
            df_long.loc[:, DataFrameColum.LEVEREAGE.value] = 10
            df_long.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_BUY.value

            filtered_df_master = DataFrameUtil.replace_rows_df_backup_with_df_for_index (df_master = filtered_df_master, df_slave = df_long)

        return filtered_df_master
    
    @staticmethod
    def apply_sell(bitget_data_util: BitgetDataUtil, data_frame: pandas.DataFrame) -> pandas.DataFrame:

        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        filtered_data_frame = data_frame.query(state_query)

        startTime = datetime.now()
        startTime = startTime.replace(hour=0, minute=0, second=0, microsecond=0)

        filtered_data_frame =  bitget_data_util.updating_pnl_roe_orders(data_frame=filtered_data_frame, startTime=startTime)

        Strategy.print_data_frame(message="VENTA ", data_frame=filtered_data_frame)

        value_limit_pctg = 8
        Strategy.mark_price_exceeds_limit(data_frame = filtered_data_frame, value_limit= value_limit_pctg)

        time_range = TimeRanges("HOUR_2")  #DAY_1  HOUR_4  MINUTES_1

        prices_history = bitget_data_util.get_historial_x_day_ago_all_crypto(df_master = filtered_data_frame, time_range = time_range, limit=1000)
        filtered_data_frame = Strategy.updating_koncorde(bitget_data_util=bitget_data_util, data_frame=filtered_data_frame, prices_history_dict=prices_history)

        #print (filtered_data_frame.loc[filtered_data_frame.index[-2], 'KONCORDE_MEDIA'])
        #print (filtered_data_frame.loc[filtered_data_frame.index[-2], 'KONCORDE_VERDE'])
        #print (filtered_data_frame.loc[filtered_data_frame.index[-2], 'KONCORDE_AZUL'])

        """ #! SHORT - CON CONFIRMACIÓN
        
        query = "(" + DataFrameColum.SIDE_TYPE.value + " == '" + FutureValues.SIDE_TYPE_SHORT.value + "')"
        query = query + " and ((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") or (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] < KONCORDE_VERDE.str[-2]) or (KONCORDE_MEDIA.str[-2] < KONCORDE_AZUL.str[-2]))"
        query = query + " and ((KONCORDE_MEDIA.str[-3] > KONCORDE_VERDE.str[-3]) and (KONCORDE_MEDIA.str[-3] > KONCORDE_AZUL.str[-3]))"
        df_short = filtered_data_frame.query(query)
        """

        #! SHORT - SIN CONFIRMACIÓN
        query = "(" + DataFrameColum.SIDE_TYPE.value + " == '" + FutureValues.SIDE_TYPE_SHORT.value + "')"
        query = query + " and ((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") or (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] > KONCORDE_VERDE.str[-2]) and (KONCORDE_MEDIA.str[-2] > KONCORDE_AZUL.str[-2]))"
        df_short = filtered_data_frame.query(query)

        if df_short.empty == False:
            df_short.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            return df_short        

        #! SHORT - UN SEGURO de VENTA
        query = "(" + DataFrameColum.SIDE_TYPE.value + " == '" + FutureValues.SIDE_TYPE_SHORT.value + "')"
        query = query + " and ((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") or (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " < " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] < KONCORDE_VERDE.str[-2]) and (KONCORDE_MEDIA.str[-2] < KONCORDE_AZUL.str[-2]))"
        df_short = filtered_data_frame.query(query)

        if df_short.empty == False:
            df_short.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            return df_short

        """ #! LONG - CON CONFIRMACIÓN
        
        query = "(" + DataFrameColum.SIDE_TYPE.value + " == '" + FutureValues.SIDE_TYPE_LONG.value + "')"
        query = query + " and ((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") and (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] > KONCORDE_VERDE.str[-2]) and (KONCORDE_MEDIA.str[-2] > KONCORDE_AZUL.str[-2]))"
        query = query + " and ((KONCORDE_MEDIA.str[-3] < KONCORDE_VERDE.str[-3]) or (KONCORDE_MEDIA.str[-3] < KONCORDE_AZUL.str[-3]))"
        df_long = filtered_data_frame.query(query)
        """

        #! LONG - SIN CONFIRMACIÓN
        query = "(" + DataFrameColum.SIDE_TYPE.value + " == '" + FutureValues.SIDE_TYPE_LONG.value + "')"
        query = query + " and ((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") and (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] < KONCORDE_VERDE.str[-2]) or (KONCORDE_MEDIA.str[-2] < KONCORDE_AZUL.str[-2]))"
        df_long = filtered_data_frame.query(query)

        if df_long.empty == False:
            df_long.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            return df_long

        #! LONG - UN SEGURO de VENTAN
        query = "(" + DataFrameColum.SIDE_TYPE.value + " == '" + FutureValues.SIDE_TYPE_LONG.value + "')"
        query = query + " and ((" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_VERDE_LAST.value + ") and (" + DataFrameColum.KONCORDE_MEDIA_LAST.value + " > " + DataFrameColum.KONCORDE_AZUL_LAST.value + "))"
        query = query + " and ((KONCORDE_MEDIA.str[-2] > KONCORDE_VERDE.str[-2]) or (KONCORDE_MEDIA.str[-2] > KONCORDE_AZUL.str[-2]))"
        df_long = filtered_data_frame.query(query)

        if df_long.empty == False:
            df_long.loc[:,DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            return df_long
    
        """********************************************** OPCION 1 **********************************************
        Si tiene el valor headdress, es que ya ha superado el value_limit. Se vende en el momento que baja el value_limit """

        query = "((" + DataFrameColum.ROE.value + " < " + str(value_limit_pctg) + ") and (" + DataFrameColum.LOOK.value + " == 'headdress!'))" 
        df_op1 = filtered_data_frame.query(query)

        if df_op1.empty == False:
            df_op1.loc[:, DataFrameColum.LOOK.value] = "-"
            df_op1.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value

            return df_op1

        """********************************************** OPCION 2 **********************************************"""
        query = "((" + DataFrameColum.ROE.value + " > " + str(value_limit_pctg) + ") and (" + DataFrameColum.ROE.value + " < " + DataFrameColum.STOP_LOSS.value + "))"  #! Es muy buena 
        df_op2 = filtered_data_frame.query(query)

        if df_op2.empty == False:
            df_op2.loc[:, DataFrameColum.LOOK.value] = "-"
            df_op2.loc[:, DataFrameColum.STATE.value] = ColumStateValues.READY_FOR_SELL.value
            
            return df_op2
        
        
        for ind in filtered_data_frame.index:
            cntrl_profit_crrnt = float(filtered_data_frame.loc[ind,DataFrameColum.ROE.value]) - 0.5

            if float(filtered_data_frame.loc[ind,DataFrameColum.STOP_LOSS.value]) < cntrl_profit_crrnt:
                filtered_data_frame.loc[ind,DataFrameColum.STOP_LOSS.value] = cntrl_profit_crrnt

        return filtered_data_frame




    def updating_rsi_sma(bitget_data_util: BitgetDataUtil, length:int = 14, data_frame:pandas.DataFrame=pandas.DataFrame(), ascending_count:int = 2, previous_period:int = 0):
        
        if DataFrameColum.SMA.value not in data_frame.columns:
            data_frame[DataFrameColum.SMA.value] = "-"

        if DataFrameColum.SMA_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.SMA_LAST.value] = 0.0

        if DataFrameColum.SMA_ASCENDING.value not in data_frame.columns:
            data_frame[DataFrameColum.SMA_ASCENDING.value] = "-"
        
        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            
            try:                
                rsi = data_frame[DataFrameColum.RSI.value][ind]
                #rsi = pandas.DataFrame(rsi, columns='values')
                  
                sma = pandas_ta.ma(type, pandas.Series(rsi), length = length)
                sma_numpy = numpy.array(sma)
                sma_numpy = sma_numpy[~numpy.isnan(sma_numpy)]
                                
                data_frame[DataFrameColum.SMA.value][ind] = sma_numpy
                data_frame.loc[ind, DataFrameColum.SMA_ASCENDING.value] = bitget_data_util.list_is_ascending(check_list = sma_numpy, ascending_count = ascending_count, previous_period = previous_period)
                data_frame.loc[ind, DataFrameColum.SMA_LAST.value] = bitget_data_util.get_last_element(element_list = sma_numpy, previous_period = previous_period)
               
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "SMA_RSI", e)
                continue
        
        return data_frame

    def updating_koncorde(bitget_data_util: BitgetDataUtil, data_frame:pandas.DataFrame=pandas.DataFrame(), prices_history_dict:dict=None):

        length = 255

        if DataFrameColum.KONCORDE_AZUL.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_AZUL.value] = None

        if DataFrameColum.KONCORDE_VERDE.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_VERDE.value] = None
    
        if DataFrameColum.KONCORDE_MARRON.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_MARRON.value] = None
        
        if DataFrameColum.KONCORDE_MEDIA.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_MEDIA.value] = None
    
        if DataFrameColum.KONCORDE_AZUL_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_AZUL_LAST.value] = None

        if DataFrameColum.KONCORDE_VERDE_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_VERDE_LAST.value] = None

        if DataFrameColum.KONCORDE_MARRON_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_MARRON_LAST.value] = None

        if DataFrameColum.KONCORDE_MEDIA_LAST.value not in data_frame.columns:
            data_frame[DataFrameColum.KONCORDE_MEDIA_LAST.value] = None

        if DataFrameColum.VOLUME.value not in data_frame.columns:
            data_frame[DataFrameColum.VOLUME.value] = None

        for ind in data_frame.index:
            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            try:                
                prices_history = prices_history_dict[symbol]

                azul, marron, verde, media = Strategy.get_konkorde_params(prices_history)
                #azul, verde, marron, media = Strategy.calculate(prices_history)
                #df_neg, df_pos, media = Strategy.plot_konkorde_montains(azul, marron, verde, media, "C:\\MIO\\grafico.png", start_row_num = 0)


                azul = numpy.array(azul.astype(float))
                verde = numpy.array(verde.astype(float))
                marron = numpy.array(marron.astype(float))
                media = numpy.array(media.astype(float))
     
                data_frame[DataFrameColum.KONCORDE_AZUL.value][ind] = azul
                data_frame[DataFrameColum.KONCORDE_VERDE.value][ind] = verde
                #data_frame[DataFrameColum.KONCORDE_MARRON.value][ind] = marron
                data_frame[DataFrameColum.KONCORDE_MEDIA.value][ind] = media
                data_frame[DataFrameColum.KONCORDE_AZUL_LAST.value][ind] = bitget_data_util.get_last_element(element_list = azul, previous_period = -1)
                data_frame[DataFrameColum.KONCORDE_VERDE_LAST.value][ind] = bitget_data_util.get_last_element(element_list = verde, previous_period = -1)
                #data_frame[DataFrameColum.KONCORDE_MARRON_LAST.value][ind] = self.get_last_element(element_list = marron, previous_period = -1)
                data_frame[DataFrameColum.KONCORDE_MEDIA_LAST.value][ind] = bitget_data_util.get_last_element(element_list = media, previous_period = -1)
                                                                    
            except Exception as e:
                bitget_data_util.print_error_updating_indicator(symbol, "KONCORDE", e)
                continue
        
        return data_frame 

    def get_konkorde_params(df_stocks):
        # df['calc_nvi'] =  df.ta.nvi( cumulative=True, append=False) #calc_nvi(df)
        # tprice=ohlc4
        tprice = df_stocks[["Open", "High", "Low", "Close"]].mean(axis=1)
        # lengthEMA = input(255, minval=1)
        # pvi = calc_pvi()
        # df['calc_pvi'] = df.ta.pvi( cumulative=True, append=False) #calc_pvi(df)
        pvi = df_stocks.ta.pvi(cumulative=True, append=False)  # calc_pvi(df)       

        m = 15
        # pvim = ema(pvi, m)
        pvim = pandas_ta.ema(close=pvi, length=m )
        # pvimax = highest(pvim, 90)
        # pvimax = lowest(pvim, 90)
        pvimax = pvim.rolling(window=90).max()  # .shift(-89)
        pvimin = pvim.rolling(window=90).min()  # .shift(-89)
        # oscp = (pvi - pvim) * 100/ (pvimax - pvimin)
        oscp = (pvi - pvim) * 100 / (pvimax - pvimin)
        # nvi =calc_nvi()
        # nvim = ema(nvi, m)
        # nvimax = highest(nvim, 90)
        # nvimin = lowest(nvim, 90)
        # azul = (nvi - nvim) * 100/ (nvimax - nvimin)
        nvi = df_stocks.ta.nvi(cumulative=True, append=False)  # calc_nvi(df)
        nvim = pandas_ta.ema(close=nvi, length=m )
        nvimax = nvim.rolling(window=90).max()  # .shift(-89)
        nvimin = nvim.rolling(window=90).min()  # .shift(-89)
        val_blue = (nvi - nvim) * 100 / (nvimax - nvimin)
        xmf = pandas_ta.mfi(close = df_stocks["Close"], volume=df_stocks["Volume"], high=df_stocks["High"], low=df_stocks["Low"],length=14)
        #xmf = talib.MFI(df_stocks['High'], df_stocks['Low'], df_stocks['Close'], df_stocks['Volume'], timeperiod=14)
        # mult=input(2.0)
        basis = pandas_ta.sma(tprice, 25)
        dev = 2.0 * pandas_ta.stdev(tprice, 25)
        upper = basis + dev
        lower = basis - dev
        # OB1 = (upper + lower) / 2.0
        # OB2 = upper - lower
        # BollOsc = ((tprice - OB1) / OB2 ) * 100
        # xrsi = rsi(tprice, 14)
        OB1 = (upper + lower) / 2.0
        OB2 = upper - lower
        BollOsc = ((tprice - OB1) / OB2) * 100
        #xrsi = pandas_ta.rsi(tprice, 14)
        xrsi = pandas.Series(pandas_ta.rsi(tprice, window=14))

        # calc_stoch(src, length,smoothFastD ) =>
        #     ll = lowest(low, length)
        #     hh = highest(high, length)
        #     k = 100 * (src - ll) / (hh - ll)
        #     sma(k, smoothFastD)
        def calc_stoch(src, length, smoothFastD):
            ll = df_stocks['Low'].rolling(window=length).min()
            hh = df_stocks['High'].rolling(window=length).max()
            k = 100 * (src - ll) / (hh - ll)
            return pandas_ta.sma(k, smoothFastD)

        stoc = calc_stoch(tprice, 21, 3)
        # stoc = py_ti.stochastic(tprice, 21, 3)
        val_brown = (xrsi + xmf + BollOsc + (stoc / 3)) / 2
        val_green = val_brown + oscp
        val_avg = pandas_ta.ema(val_brown, timeperiod=m)

        return val_blue, val_brown, val_green, val_avg, 

    def plot_konkorde_montains(azul, marron, verde, media, path, start_row_num = 0):

        azul = azul.tail(10)
        marron = marron.tail(10)
        verde = verde.tail(10)
        media = media.tail(10)

        df_plot = pandas.DataFrame({'azul': azul, 'marron': marron, 'verde': verde})[start_row_num:]

        cols = ['verde', 'marron', 'verde', 'media']
        colors = ['green', 'brown', 'cyan', 'red']
        # df = pd.DataFrame(columns=cols, data=[verde, marron, azul, media])
        fig, ax = plt.subplots()
        # split dataframe df into negative only and positive only values
        df_neg, df_pos = df_plot.clip(upper=0), df_plot.clip(lower=0)
        # stacked area plot of positive values
        df_pos.plot.area(ax=ax, stacked=True, linewidth=0.)
        # reset the color cycle
        ax.set_prop_cycle(None)
        # stacked area plot of negative values, prepend column names with '_' such that they don't appear in the legend
        df_neg.rename(columns=lambda x: '_' + x).plot.area(ax=ax, stacked=True, linewidth=0.)
        # rescale the y axis
        print (df_neg.sum(axis=1).min())
        print (df_pos.sum(axis=1).max())
        #ax.set_ylim([df_neg.sum(axis=1).min(), df_pos.sum(axis=1).max()])
        ax.set_ylim([-200, 200])
        ax.autoscale(enable=True)
        ax.plot(media[start_row_num:], color="red")
        # ax.plot(df['High'][start_row_num:] * 0.4, color="black")
        plt.savefig(path)

        print (df_neg)
        print (df_pos)
        print (df_plot)

        return df_plot, media

    @staticmethod
    def mark_price_exceeds_limit(data_frame: pandas.DataFrame, value_limit: float = 1) -> pandas.DataFrame:
        """Marcamos las cons con el profit superior al limite marcado"""
        data_frame.loc[data_frame[DataFrameColum.ROE.value] >= value_limit, DataFrameColum.LOOK.value] = 'headdress!'
        return data_frame

    @staticmethod
    def print_data_frame(message: str, data_frame: pandas.DataFrame):

        if data_frame.empty == False:
            print("#####################################################################################################################")
            print(message)
            print(
                data_frame[[DataFrameColum.SYMBOL.value,
                            DataFrameColum.SIDE_TYPE.value,
                            DataFrameColum.ROE.value, 
                            DataFrameColum.PNL.value, 
                            DataFrameColum.STOP_LOSS.value,
                            DataFrameColum.LOOK.value
                            ]])
            print("#####################################################################################################################")
        else:
            print("#####################################################################################################################")
            print(message + " SIN DATOS")
            print("#####################################################################################################################")
