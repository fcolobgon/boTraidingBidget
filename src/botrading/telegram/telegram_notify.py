from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.utils.rules_util import RuleUtils

import requests
import pandas

class TelegramNotify:
    
    @staticmethod
    def notify(settings=None, message=""):
        
        try:
            
            apiURL = f'https://api.telegram.org/bot' + settings.TELEGRAM_BOT_TOKEN + '/sendMessage'
            requests.post(apiURL, json={'chat_id': settings.CHATID, 'text': message})
                        
        except Exception as e:
                print(str(e))
                print("Error notificando por telegram.")
                
    @staticmethod
    def notify_df(settings=None, dataframe:pandas.DataFrame=pandas.DataFrame(), message="", colums=[]):
        
        try:

            for ind in dataframe.index:
                if settings:
                    coin = dataframe.loc[ind, DataFrameColum.BASE.value]
                    url_message = message + str(coin) 
                    requests.post(info, json={'chat_id': settings.CHATID, 'text': url_message})
                    
                    try:
                        if len(colums) > 0:
                            for columna in colums:
                                info = dataframe.loc[ind, columna]
                                info = info + "/n"
                    except Exception as e:
                        print(str(e))
                        print("Error notificando por telegram.")
                        continue
                        
        except Exception as e:
                print(str(e))
                print("Error notificando por telegram.")
    
    @staticmethod
    def notify_buy(settings=None, dataframe:pandas.DataFrame=pandas.DataFrame() ,message=""):
        
        try:
            
            rules = [ColumStateValues.READY_FOR_BUY]
            state_query = RuleUtils.get_rules_search_by_states(rules)
            dataframe = dataframe.query(state_query)

            for ind in dataframe.index:
                if settings:
                    coin = dataframe.loc[ind, DataFrameColum.BASE.value]
                    url_message = "Nueva compra https://www.binance.com/es/trade/" + str(coin) + "_USDT"
                    apiURL = f'https://api.telegram.org/bot' + settings.TELEGRAM_BOT_TOKEN + '/sendMessage'
                    requests.post(apiURL, json={'chat_id': settings.CHATID, 'text': url_message})
                    if message.strip == False:
                        requests.post(apiURL, json={'chat_id': settings.CHATID, 'text': message})
                        
        except Exception as e:
                print(str(e))
                print("Error notificando por telegram.")
    
    @staticmethod
    def notify_sell(settings=None, dataframe:pandas.DataFrame=pandas.DataFrame() ,message=""):

        try:
            
            rules = [ColumStateValues.READY_FOR_SELL]
            state_query = RuleUtils.get_rules_search_by_states(rules)
            dataframe = dataframe.query(state_query)
            
            for ind in dataframe.index:
                if settings:
                    coin = dataframe.loc[ind, DataFrameColum.BASE.value]
                    profit = dataframe.loc[ind, DataFrameColum.PERCENTAGE_PROFIT.value]
                    url_message = "Moneda vendida " + str(coin) + " Beneficio "  + str(profit)  
                    apiURL = f'https://api.telegram.org/bot' + settings.TELEGRAM_BOT_TOKEN + '/sendMessage'
                    requests.post(apiURL, json={'chat_id': settings.CHATID, 'text': url_message})
                    if message.strip == False:
                        requests.post(apiURL, json={'chat_id': settings.CHATID, 'text': message})
                        
        except Exception as e:
                print(str(e))
                print("Error notificando por telegram.")
    
    @staticmethod
    def print_error_updating_indicator(symbol, indicator, e):
        
        print("Symbol " + str(symbol))
        print("Error notificando por telegram " + str(indicator))
        print(str(e))
        print("Posible nueva cripto " + str(symbol))
            
    