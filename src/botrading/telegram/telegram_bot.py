from telegram.ext import Updater, CommandHandler,  CallbackContext, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from src.botrading.utils.bitget_data_util import BitgetDataUtil
from src.botrading.utils import excel_util
from src.botrading.utils.rules_util import RuleUtils
from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.utils.enums.data_frame_colum import ColumStateValues
from src.botrading.thread.bitget_buy_thread import BitgetBuyThreed
from src.botrading.thread.bitget_sell_thread import BitgetSellThreed
from src.botrading.utils import traiding_operations
from src.botrading.bit import BitgetClienManager
from src.botrading.utils.dataframe_util import DataFrameUtil

from pathlib import Path
import os
import time

class TelegramBot:
    
    bit_client: BitgetClienManager
    Bitget_data_util: BitgetDataUtil
    buy_thread:BitgetBuyThreed
    sell_thread:BitgetSellThreed
    base_path:str
    bot_token:str
    #Comandos disponobles
    info_command = "info"
    
    pause_buy_command = "pauseBuy"
    restart_buy_command = "restartBuy"
    status_buy_command = "statusBuy"
    
    profit_command = "profit"
    profit_zero_comission_command = "profitZeroComission"
    profit_detail_command = "profitDetail"
    
    execution_command = "execution"
    execution_detail_command = "executionDetail"
    
    new_coin_command = "newCoin"
    sell_coin_command = "sellCoin"
    sell_all_coin_command = "sellAllCoin"
    stop_bot_with_sell_command = "stopBotWithSell"
    
    delete_buy_sell_files_command = "deleteBuySellFiles"

    market_status_command = "marketStatus"
    send_files_command = "sendFiles"

    
    def __init__(self, bit_client: BitgetClienManager, buy_thread:BitgetBuyThreed,  sell_thread:BitgetSellThreed, base_path:str, bot_token:str):
        self.bit_client = bit_client
        self.buy_thread = buy_thread
        self.sell_thread = sell_thread
        self.Bitget_data_util = buy_thread.get_Bitget_data_util()
        self.bot_token = bot_token
        self.base_path = base_path
        excel_util.custom_init(base_path)
    
    def info(self, update:Updater, context:CallbackContext):
        """
        buttons = []

        button = InlineKeyboardButton(text='Profit', callback_data='Profit:')
        buttons.append(button)
        button = InlineKeyboardButton(text='SellAll', callback_data='sellAll:')
        buttons.append(button)
        button = InlineKeyboardButton(text='Status', callback_data='status_buy:')
        buttons.append(button)
        
        keyboard = InlineKeyboardMarkup([buttons])
        update.message.reply_text('Exchange functionality:', reply_markup=keyboard)


        buttons = []        
        
        button = InlineKeyboardButton(text='Paused Buy', callback_data='paused_buy:')
        buttons.append(button)
        button = InlineKeyboardButton(text='Restart Buy', callback_data='restart_buy:')
        buttons.append(button)
        button = InlineKeyboardButton(text='Delete files', callback_data='delete_files:')
        buttons.append(button)
        button = InlineKeyboardButton(text='Send files', callback_data='send_files:')
        buttons.append(button)

        keyboard = InlineKeyboardMarkup([buttons])
        update.message.reply_text('Bot functionality:', reply_markup=keyboard)

        """
        update.message.reply_text("Comandos disponibles")

        update.message.reply_text("/" + str(self.profit_command))
        #update.message.reply_text("/" + str(self.profit_zero_comission_command))
        update.message.reply_text("/" + str(self.profit_detail_command))
        
        update.message.reply_text("/" + str(self.execution_command))
        update.message.reply_text("/" + str(self.execution_detail_command))
        
        #update.message.reply_text("/" + str(self.new_coin_command))
        #update.message.reply_text("/" + str(self.sell_coin_command))
        #update.message.reply_text("/" + str(self.sell_all_coin_command))
        update.message.reply_text("/" + str(self.stop_bot_with_sell_command))
        
        update.message.reply_text("/" + str(self.pause_buy_command))
        update.message.reply_text("/" + str(self.restart_buy_command))
        update.message.reply_text("/" + str(self.status_buy_command))
        
        update.message.reply_text("/" + str(self.delete_buy_sell_files_command))

        update.message.reply_text("/" + str(self.send_files_command))
        #update.message.reply_text("/" + str(self.market_status_command))
        



    def profit(self, update:Updater, context:CallbackContext):
        """Send a message when the command /profit is issued."""
        
        
        file_path = self.base_path
        file_path = file_path + os.path.join("operations")
        pathlist = Path(file_path).glob('**/*.txt')
        total = self.calculate_profit(pathlist=pathlist, comission=True)
        actual_profit = "Profit: " + str(total) + " %"

        if update.callback_query:
            update.callback_query.message.edit_text(actual_profit)
        else:
            update.message.reply_text(str(actual_profit))
        
        
    def profit_zero_comission(self, update:Updater, context:CallbackContext):
        """Send a message when the command /profit is issued."""
        
        file_path = self.base_path
        file_path = file_path + os.path.join("operations")
        pathlist = Path(file_path).glob('**/*.txt')
        total = self.calculate_profit(pathlist=pathlist, comission=False)
        actual_profit = "Profit: " + str(total) + " %"
        
        if update.callback_query:
            update.callback_query.message.edit_text(actual_profit)
        else:
            update.message.reply_text(str(actual_profit))
        
    
    def calculate_profit(self, pathlist, comission:bool) -> float:
        
        beneficios = []

        for path in pathlist:
            file = open(path, "r")
            lines = file.readlines()
            for line in lines:
                if line in "\n":
                    print(" --- ")
                elif "[-]" not in line:
                    is_sell = line.find("SELL -")
                    if is_sell == 0:
                        start = line.find("BENEFICIO [") + len("BENEFICIO [")
                        end = line.find("] %")
                        beneficio = line[start:end]
                        if beneficio != None:
                            beneficios.append(float(beneficio))
            file.close()
        total = 0
        for b in beneficios:
            if comission:
                total = total + (b - 0.4)
            else:
                total = total + b
        
        return total
    
    def profit_detail(self, update:Updater, context:CallbackContext):
        """Send a message when the command /profit is issued."""
        
        file_path = self.base_path
        file_path = file_path + os.path.join("operations")
        pathlist = Path(file_path).glob('**/*.txt')

        for path in pathlist:
            file = open(path, "r")

            if update.callback_query:
                update.callback_query.message.edit_text(file.name)
            else:
                update.message.reply_text(str(file.name))

            lines = file.readlines()

            for line in lines:
                if line not in "\n":
                    is_sell = line.find("SELL -")
                    if is_sell == 0:

                        if update.callback_query:
                            update.callback_query.message.edit_text(line)
                        else:
                            update.message.reply_text(str(line))
                
            file.close()
        
    def execution(self, update:Updater, context:CallbackContext):
        """Send a message when the command /execution is issued."""
            
        data_frame = self.buy_thread.get_data_frame()
        
        rules = [ColumStateValues.BUY, ColumStateValues.ERR_SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        data_frame = data_frame.query(state_query)  
        
        
        if data_frame.empty == True:
            if update.callback_query:
                update.callback_query.message.edit_text('No hay ninguna moneda con estado BUY')
            else:
                update.message.reply_text(str('No hay ninguna moneda con estado BUY'))
        else:
            buttons = []
            for ind in data_frame.index:
                
                base = data_frame[DataFrameColum.BASE.value][ind]
                symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
                state = data_frame[DataFrameColum.STATE.value][ind]
                profit = data_frame[DataFrameColum.PERCENTAGE_PROFIT.value][ind]

                #context.args = (base,)
                button = InlineKeyboardButton(text=base, callback_data='sell_coin:' + base)
                buttons.append(button)
                
                if update.callback_query:
                    update.callback_query.message.edit_text("Coin: " + str(symbol) + " | " + "Profit: " + str(profit))
                else:
                    update.message.reply_text(str("Coin: " + str(symbol) + " | " + "Profit: " + str(profit)))

                
                #update.message.reply_text("Estado " + str(state) )
                #update.message.reply_text("Beneficio " + str(profit))
            button = InlineKeyboardButton(text='¡ALL!', callback_data='sellAll:')
            buttons.append(button)

            keyboard = InlineKeyboardMarkup([buttons])

            if update.callback_query:
                update.callback_query.message.edit_text('Seleccione una moneda para vender:', reply_markup=keyboard)
            else:
                update.message.reply_text('Seleccione una moneda para vender:', reply_markup=keyboard)

    def button(self, update:Updater, context:CallbackContext):

        query = update.callback_query
        if query.data.startswith('sell_coin:'):
            base = query.data.split(':')[1]
            context.args = [base]
            self.sell_coin(update, context)
        elif query.data.startswith('Profit:'):
            self.profit(update, context)
        elif query.data.startswith('sellAll:'):
            self.sell_all_coin(update, context)
        elif query.data.startswith('status_buy:'):
            self.status_buy(update, context)
        elif query.data.startswith('paused_buy:'):
            self.pause_buy(update, context)
        elif query.data.startswith('restart_buy:'):
            self.restart_buy(update, context)
        elif query.data.startswith('delete_files:'):
            self.delete_buy_sell_files(update, context)
        elif query.data.startswith('send_files:'):
            self.send_files(update, context)

    def execution_detail(self, update:Updater, context:CallbackContext):
        """Send a message when the command /execution is issued."""
            
        data_frame = self.buy_thread.get_data_frame()
        
        rules = [ColumStateValues.BUY, ColumStateValues.ERR_SELL]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        data_frame = data_frame.query(state_query)  
        
        
        if data_frame.empty == True:
            if update.callback_query:
                update.callback_query.message.edit_text('No hay ninguna moneda con estado BUY')
            else:
                update.message.reply_text('No hay ninguna moneda con estado BUY')
        else:
            
            for ind in data_frame.index:
                
                base = data_frame[DataFrameColum.BASE.value][ind]
                symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
                state = data_frame[DataFrameColum.STATE.value][ind]
                profit = data_frame[DataFrameColum.PERCENTAGE_PROFIT.value][ind]

                if update.callback_query:
                    update.callback_query.message.edit_text("https://www.binance.com/es/trade/" + str(base) + "_USDT")
                    update.callback_query.message.edit_text("Moneda " + str(symbol) )
                    update.callback_query.message.edit_text("Estado " + str(state) )
                    update.callback_query.message.edit_text("Benficio " + str(profit))
                else:
                    update.message.reply_text("https://www.binance.com/es/trade/" + str(base) + "_USDT")
                    update.message.reply_text("Moneda " + str(symbol) )
                    update.message.reply_text("Estado " + str(state) )
                    update.message.reply_text("Benficio " + str(profit))
    
    def new_coin(self, update:Updater, context:CallbackContext):
        
        data_frame = self.buy_thread.get_data_frame()
        data_frame =self.Bitget_data_util.find_new_cryptos(data_frame)
        
        if data_frame.empty == True:
            update.message.reply_text('No hay nuevas monedas en Binance')
        else:
            
            for ind in data_frame.index:
                
                base = data_frame[DataFrameColum.BASE.value][ind]
                symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
                
                update.message.reply_text("https://www.binance.com/es/trade/" + str(base) + "_USDT")
                update.message.reply_text("Moneda " + str(symbol) )

        
    def sell_coin(self, update:Updater, context:CallbackContext):
        
        sell_coin = context.args[0]
        
        self.sell_thread.pause_thread = True
        self.sell_thread.pause_thread = True
        
        self.buy_thread.wait_buy_thread_ready()
        data_frame = self.buy_thread.get_data_frame()
        
        sell_coin_data_frame = data_frame.loc[data_frame[DataFrameColum.BASE.value] == sell_coin]
        print(sell_coin_data_frame)
        if sell_coin_data_frame.empty:
            if update.callback_query:
                update.callback_query.message.edit_text("No se encontro la moneda " + str(sell_coin))
            else:
                update.message.reply_text("No se encontro la moneda " + str(sell_coin))
        else:
            sell_dataframe = traiding_operations.logic_sell(clnt_bnb=self.bit_client, df_sell=sell_coin_data_frame)
            lock_minutes = 60*14
            sell_dataframe = DataFrameUtil.locking_time(data_frame = sell_dataframe, minutes = lock_minutes)
            self.buy_thread.merge_dataframes(update_data_frame = sell_dataframe)
            time.sleep(2)

            if update.callback_query:
                update.callback_query.message.edit_text("Venta forzada de la moneda " + str(sell_coin))
            else:
                update.message.reply_text("Venta forzada de la moneda " + str(sell_coin))
            
            self.profit(update,context)
        
        self.sell_thread.pause_thread = False
        self.sell_thread.pause_thread = False     

    def sell_all_coin(self, update:Updater, context:CallbackContext):
        
        self.sell_thread.pause_thread = True
        self.sell_thread.pause_thread = True
        
        self.buy_thread.wait_buy_thread_ready()
        data_frame_for_sell = self.buy_thread.get_data_frame()
        
        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        
        data_frame_sell_now = data_frame_for_sell.query(state_query)
        sell_dataframe = traiding_operations.logic_sell(clnt_bnb=self.bit_client, df_sell=data_frame_sell_now)
        self.buy_thread.merge_dataframes(update_data_frame = sell_dataframe)
        
        time.sleep(2)

        if update.callback_query:
            update.callback_query.message.edit_text("Venta forzada de todas las monedas")
        else:
            update.message.reply_text("Venta forzada de todas las monedas")

        self.profit(update,context)
        
        self.sell_thread.pause_thread = False
        self.sell_thread.pause_thread = False
                
    def stop_bot_with_sell(self, update:Updater, context:CallbackContext):
        
        self.sell_thread.stop_thread = True
        
        self.buy_thread.wait_buy_thread_ready()
        self.buy_thread.lock_buy_thread()
        data_frame_for_sell = self.buy_thread.get_data_frame()
        self.buy_thread.stop_thread = True
        
        rules = [ColumStateValues.BUY]
        state_query = RuleUtils.get_rules_search_by_states(rules)
        
        data_frame_sell_now = data_frame_for_sell.query(state_query)
        
        traiding_operations.logic_sell(clnt_bnb=self.bit_client, df_sell=data_frame_sell_now)
        time.sleep(1)
        update.message.reply_text("Parada de bot con venta de todas las monedas")
        self.profit_detail(update,context)
        self.profit(update,context)
        
    def pause_buy(self, update:Updater, context:CallbackContext):
        
        self.buy_thread.pause_thread = True

        if update.callback_query:
            update.callback_query.message.edit_text("Hilo de compras pausado")
        else:
            update.message.reply_text("Hilo de compras pausado")
            
    
    def restart_buy(self, update:Updater, context:CallbackContext):
        
        self.buy_thread.pause_thread = False

        if update.callback_query:
            update.callback_query.message.edit_text("Hilo de compras reiniciado")
        else:
            update.message.reply_text("Hilo de compras reiniciado")
        
    def status_buy(self, update:Updater, context:CallbackContext):
        
        if self.buy_thread.pause_thread:

            if update.callback_query:
                update.callback_query.message.edit_text("Hilo de compras parado")
            else:
                update.message.reply_text("Hilo de compras parado")

        elif self.buy_thread.pause_thread:
            if update.callback_query:
                update.callback_query.message.edit_text("Hilo de compras pausado")
            else:
                update.message.reply_text("Hilo de compras pausado")
        else:

            if update.callback_query:
                update.callback_query.message.edit_text("Hilo de compras arrancado")
            else:
                update.message.reply_text("Hilo de compras arrancado")

            self.market_status(update,context)
            self.profit(update,context)
            self.execution(update,context)

    
    def delete_buy_sell_files(self, update:Updater, context:CallbackContext):
        
        deleted = excel_util.delete_buy_sell_files()
        
        if deleted:
            if update.callback_query:
                update.callback_query.message.edit_text("Se han elimindo todos los ficheros de compras y ventas")
            else:
                update.message.reply_text("Se han elimindo todos los ficheros de compras y ventas")
        else:
            if update.callback_query:
                update.callback_query.message.edit_text("NO!! se han elimindo todos los ficheros de comprs y ventas")
            else:
                update.message.reply_text("NO!! se han elimindo todos los ficheros de comprs y ventas")
    
    def send_files(self, update:Updater, context:CallbackContext):
        file_path = self.base_path
        directory = file_path + os.path.join("operations")
        
        files = os.listdir(directory)
        
        for file in files:
            file_path = os.path.join(directory, file)
            if os.path.isfile(file_path):
                with open(file_path, 'rb') as f:
                    context.bot.send_document(chat_id=update.message.chat_id, document=f)


    def market_status(self, update:Updater, context:CallbackContext):
        status = excel_util.read_market_status_file()

        if update.callback_query:
            update.callback_query.message.edit_text("MARKET STATUS: " + str(status.name))
        else:
            update.message.reply_text("MARKET STATUS: " + str(status.name))

    def create_button(update, context):
        button = InlineKeyboardButton(text="Vender", callback_data="sell")
        keyboard = [[button]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text("Presione el botón para vender", reply_markup=reply_markup)
            
    def start(self):
                
        updater = Updater(self.bot_token, use_context=True)

        dp = updater.dispatcher

        dp.add_handler(CommandHandler(self.info_command, self.info))
        
        dp.add_handler(CommandHandler(self.profit_command, self.profit))
        dp.add_handler(CommandHandler(self.profit_zero_comission_command, self.profit_zero_comission))
        dp.add_handler(CommandHandler(self.profit_detail_command, self.profit_detail))
        
        dp.add_handler(CommandHandler(self.execution_command, self.execution))
        dp.add_handler(CommandHandler(self.execution_detail_command, self.execution_detail))
        
        dp.add_handler(CommandHandler(self.new_coin_command, self.new_coin))
        dp.add_handler(CommandHandler(self.sell_coin_command, self.sell_coin))
        dp.add_handler(CommandHandler(self.sell_all_coin_command, self.sell_all_coin))
        dp.add_handler(CommandHandler(self.stop_bot_with_sell_command, self.stop_bot_with_sell))
        
        dp.add_handler(CommandHandler(self.pause_buy_command, self.pause_buy))
        dp.add_handler(CommandHandler(self.restart_buy_command, self.restart_buy))
        dp.add_handler(CommandHandler(self.status_buy_command, self.status_buy))
        
        dp.add_handler(CommandHandler(self.delete_buy_sell_files_command, self.delete_buy_sell_files))

        dp.add_handler(CommandHandler(self.send_files_command, self.send_files))
        dp.add_handler(CommandHandler(self.market_status_command, self.market_status))

        dp.add_handler(CallbackQueryHandler(self.button))
        
        
        
        # Start the Bot
        updater.start_polling()
        print("TELEGRAM BOT RUNNINGGG!!!!!!!!!!!")
        updater.idle()