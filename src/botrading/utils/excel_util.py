import pandas
#import logging
import time
from pathlib import Path

from src.botrading.constants import botrading_constant

from src.botrading.utils.enums.data_frame_colum import DataFrameColum
from src.botrading.thread.enums.binance_market_status import BinanceMarketStatus
import glob
import os

# Constants
new_coins_file_name = "new_coins.txt"
market_status_file_name = "market_status.txt"
data_frame_file_name = "data_frame_file"
data_frame_file_extension = ".xlsx"
#data_frame_file_extension = ".xls"
data_frame_file = data_frame_file_name + data_frame_file_extension

# Global
base_path = ""
data_frame_file_path = ""
buy_sell_file_path = ""
    
def custom_init(custom_base_path:str = ""):
    global base_path
    global data_frame_file_path 
    global buy_sell_file_path
    global market_status_path
    global new_coins_file_path

    base_path = custom_base_path
    market_status_path = Path(base_path, "status")
    data_frame_file_path = Path(base_path, "dataframes")
    buy_sell_file_path = Path(base_path, "operations")
    new_coins_file_path = Path(base_path, "new_coins")



def calculate_profit(buy, sell) -> str:

    profit = "?"

    if isinstance(buy, str) or isinstance(sell, str):
        return profit
    # TODO: restar comision de compra en principio fijo 2%
    profit = (float(sell) * 100.0 / float(buy)) - 100
    print(str(profit))
    return str(profit)

def save_new_coins_file(new_coins:str):
    
    new_coins_file = Path(new_coins_file_path, new_coins_file_name)
    file = open(new_coins_file, "a")
    
    file.write('\n' + new_coins)
    file.close()

def read_new_coins_file():
    
    new_coins_file = Path(new_coins_file_path, new_coins_file_name)
    file = open(new_coins_file, "r")
    
    lines = []

    for linea in file:
        lines.append(linea.strip())
    
    file.close()
    
    return lines

def save_market_status_file(market_status:BinanceMarketStatus):
    
    market_status_file = Path(market_status_path, market_status_file_name)
    file = open(market_status_file, "w")

    file.write(market_status.value)
    file.close()

def read_market_status_file() -> BinanceMarketStatus:
    
    market_status:BinanceMarketStatus
    market_status_file = Path(market_status_path, market_status_file_name)
    file = open(market_status_file, "r")
    
    lines = file.readlines()
    
    for line in lines:
        market_status = BinanceMarketStatus[line]
        break

    file.close()
    return market_status
    
def save_buy_file(data_frame: pandas.DataFrame):

    if not data_frame.empty:

        for ind in data_frame.index:

            try:
                
                coin_name = data_frame[DataFrameColum.BASE.value][ind]
                buy_sell_file_name = "buy_sell_" + coin_name + ".txt"

                buy_sell_file = Path(buy_sell_file_path, buy_sell_file_name)

                file = open(buy_sell_file, "a")

                buy_price = data_frame[DataFrameColum.PRICE_BUY.value][ind]
                state = str(data_frame[DataFrameColum.STATE.value][ind])

                fecha = time.strftime("%Y%m%d-%H%M%S")

                fecha = time.strftime("%d/%m/%Y %H:%M")
                line = (
                    state
                    + " - Precio compra ["
                    + str(buy_price)
                    + "] - precio venta [-] - "
                    + str(fecha)
                    + " - BENEFICIO [?] % - T: "
                    + str(fecha)
                )

                file.write("\n")
                file.write(line)
                file.close()

            except Exception:
                print("Error escribiendo fichero de compra/ventas " + buy_sell_file)
                print("Datos " + line)
                continue


def save_sell_file(data_frame: pandas.DataFrame):

    if not data_frame.empty:

        for ind in data_frame.index:

            try:

                coin_name = data_frame[DataFrameColum.BASE.value][ind]
                buy_sell_file_name = "buy_sell_" + coin_name + ".txt"

                buy_sell_file = Path(buy_sell_file_path, buy_sell_file_name)

                file = open(buy_sell_file, "a")

                buy_price = data_frame[DataFrameColum.PRICE_BUY.value][ind]
                state = str(data_frame[DataFrameColum.STATE.value][ind])

                sell_price = data_frame[DataFrameColum.PRICE_SELL.value][ind]
                profit = calculate_profit(buy_price, sell_price)

                fecha = time.strftime("%Y%m%d-%H%M%S")

                fecha = time.strftime("%d/%m/%Y %H:%M")
                line = (
                    state
                    + " - Precio compra ["
                    + str(buy_price)
                    + "] - precio venta ["
                    + str(sell_price)
                    + "] - "
                    + str(fecha)
                    + " - BENEFICIO ["
                    + profit
                    + "] % - T: "
                    + str(fecha)
                )

                file.write("\n")
                file.write(line)
                file.close()

            except Exception:
                print("Error escribiendo fichero de compra/ventas " + buy_sell_file)
                print("Datos " + line)
                continue

def delete_buy_sell_files() -> bool:
    
    all_deleted = True

    for file in buy_sell_file_path.glob('*.txt'):
        try:
            file.unlink()
        except Exception as e:
            all_deleted = False
            continue
    
    return all_deleted

def load_dataframe(exel_name: str = ""):

    exel_name = exel_name + data_frame_file_extension

    list_of_files = glob.glob(
        str(Path(str(data_frame_file_path), str(exel_name)))
    )  # * means all if need specific format then *.csv

    df_read_excel = pandas.DataFrame()

    if len(list_of_files) > 0:
        latest_file = max(list_of_files, key=os.path.getctime)
        df_read_excel = pandas.read_excel(latest_file, index_col=0)

    return df_read_excel

def load_dataframe():

    #data_frame_pattern = "*" + data_frame_file_name + "*" + data_frame_file_extension
    data_frame_pattern = data_frame_file_name + data_frame_file_extension

    list_of_files = glob.glob(
        str(Path(str(data_frame_file_path), str(data_frame_pattern)))
    )  # * means all if need specific format then *.csv

    df_read_excel = pandas.DataFrame()

    if len(list_of_files) > 0:
        latest_file = max(list_of_files, key=os.path.getctime)
        df_read_excel = pandas.read_excel(latest_file, index_col=0)

    return df_read_excel


def remove_dataframe(exel_name: str = "") -> bool:
    
    remove_path = Path(str(data_frame_file_path), str(exel_name))
    list_of_files_audit_tmp = glob.glob(
        str(remove_path)
    ) 

    if len(list_of_files_audit_tmp) > 0:
        os.remove(list_of_files_audit_tmp[0])
        return True
    else:
        return False

def save_data_frame(data_frame: pandas.DataFrame, exel_name: str = ""):

    try:

        if not exel_name:
            exel_name = Path(data_frame_file_path, data_frame_file)

        data_frame.to_excel(Path(data_frame_file_path, exel_name))

    except Exception as e:
        print(str(e))
        pass


