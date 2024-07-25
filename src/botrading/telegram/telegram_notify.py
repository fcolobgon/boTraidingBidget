

import requests
from src.botrading.constants import botrading_constant

class TelegramNotify:
    
    @staticmethod
    def notify(message=""):
        
        try:
            
            apiURL = f'https://api.telegram.org/bot' + botrading_constant.TELEGRAM_BOT_TOKEN + '/sendMessage'
            requests.post(apiURL, json={'chat_id': botrading_constant.CHATID, 'text': message})
                        
        except Exception as e:
                print(str(e))
                print("Error notificando por telegram.")
    
    @staticmethod
    def print_error_updating_indicator(symbol, indicator, e):
        
        print("Symbol " + str(symbol))
        print("Error notificando por telegram " + str(indicator))
        print(str(e))
        print("Posible nueva cripto " + str(symbol))
            
    