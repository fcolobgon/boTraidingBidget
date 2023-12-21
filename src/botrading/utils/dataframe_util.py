from numpy import double
from typing import List
import pandas
import datetime
from datetime import datetime, timedelta

from src.botrading.utils.enums.data_frame_colum import *
from src.botrading.utils.rules_util import RuleUtils


class DataFrameUtil:
    
    @staticmethod
    def check_first_iteration(data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        if any(data_frame[DataFrameColum.FIRST_ITERATION.value] == False):
            return False
        else:
            return True
        
    @staticmethod
    def increase_level(data_frame: pandas.DataFrame, take_profit_flag: bool) -> pandas.DataFrame:
        
        if data_frame.empty == True:
            return data_frame
        
        for ind in data_frame.index:
            
            nivel_actual = data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value]
            profit_actual = data_frame.loc[ind, DataFrameColum.PERCENTAGE_PROFIT.value]
            
            if int(profit_actual) > nivel_actual:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = int(profit_actual)
            if nivel_actual == 1 and profit_actual < 1 and take_profit_flag == True:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = -1
            elif profit_actual < nivel_actual and nivel_actual > 1:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = -1
        #Si 1 y 0.4 vender
        return data_frame

    @staticmethod
    def increase_stop_loss(data_frame: pandas.DataFrame) -> pandas.DataFrame:
        
        if data_frame.empty == True:
            return data_frame
        
        for ind in data_frame.index:

            profit_actual = data_frame.loc[ind, DataFrameColum.PERCENTAGE_PROFIT.value]
            stop_loss = data_frame.loc[ind, DataFrameColum.STOP_LOSS.value]
            stop_loss_level = data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value]
            
            if profit_actual > 1 and stop_loss_level == 0:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 0.5
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = 1  
   
            elif profit_actual > 2 and stop_loss_level == 1:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 1.2
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = 2
                  
            elif profit_actual > 3 and stop_loss_level == 2:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 2
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = 3
            
            elif profit_actual > 4 and stop_loss_level == 3:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 3
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = 4
            
            elif profit_actual > 4.5 and stop_loss_level == 4:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 3.5
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = 5
            
            elif profit_actual > 5 and stop_loss_level == 5:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 4
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = 6
            
            elif profit_actual > 6 and stop_loss_level == 6:
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 5.5
                data_frame.loc[ind, DataFrameColum.STOP_LOSS_LEVEL.value] = 7
            
        return data_frame

    @staticmethod
    def replace_rows_df_backup_with_df_for_index (df_slave: pandas.DataFrame, df_master: pandas.DataFrame):
        #Copiamos el contenido de un df al df_bckp
        for index, row in df_slave.iterrows():
            df_master.loc[index] = df_slave.loc[index]

        return df_master
    
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
    def create_macd_columns(df:pandas.DataFrame):
        
        colums = [
            DataFrameColum.MACD_GOOD.value,
            DataFrameColum.MACD_GOOD_ASCENDING.value,
            DataFrameColum.MACD_BAD.value,
            DataFrameColum.MACD_BAD_ASCENDING.value,
            DataFrameColum.MACD_CHAR.value,
            DataFrameColum.MACD_CHAR_ASCENDING.value,
            DataFrameColum.MACD_CRUCE.value,
            DataFrameColum.MACD_ZERO.value,
            DataFrameColum.MACD_READY.value]
        
        df = DataFrameUtil.add_columns_to_dataframe(column_names=colums, df=df)
        df[DataFrameColum.MACD_GOOD.value] = 0.0
        df[DataFrameColum.MACD_GOOD_ASCENDING.value] = False
        df[DataFrameColum.MACD_BAD.value] = 0.0
        df[DataFrameColum.MACD_BAD_ASCENDING.value] = False
        df[DataFrameColum.MACD_CHAR.value] = 0.0
        df[DataFrameColum.MACD_CHAR_ASCENDING.value] = False
        df[DataFrameColum.MACD_CRUCE.value] = "-"
        df[DataFrameColum.MACD_ZERO.value] = "-"
        df[DataFrameColum.MACD_READY.value] = False
        return df
    
    @staticmethod
    def locking_time(data_frame: pandas.DataFrame,minutes : int):
        
        data_frame[DataFrameColum.LOCK.value] = datetime.now() + timedelta(seconds=minutes*60)
        return data_frame
    
    
    @staticmethod
    def unlocking_time_locked(data_frame: pandas.DataFrame):
        init_time = datetime.now() - timedelta(minutes=1)
        data_frame.loc[data_frame[DataFrameColum.LOCK.value] == '-', DataFrameColum.LOCK.value] = init_time
        mask  = data_frame[DataFrameColum.LOCK.value] < datetime.now()
        
        return data_frame.loc[mask]
        
    @staticmethod
    def locking_time_crypto(data_frame: pandas.DataFrame, time_column : str, minutes : int):
        
        data_frame[time_column] = datetime.now() + timedelta(seconds=minutes*60)
        return data_frame
    
    @staticmethod
    def unlocking_time_locked_crypto(data_frame: pandas.DataFrame, time_column : str):
        """
        La función utiliza la biblioteca datetime de Python para obtener la hora y fecha actuales con datetime.now(). 
        Luego, crea una máscara booleana mask que contiene True para todas las filas donde el valor en la columna de tiempo es anterior a la hora y fecha actuales.

        Por último, la función devuelve las filas del DataFrame que cumplen con la máscara booleana utilizando el método loc de Pandas.
        """
        data_frame.loc[data_frame[time_column] == '-', time_column] = datetime.now()
        mask  = data_frame[time_column] < datetime.now()
        
        return data_frame.loc[mask]
    
    @staticmethod
    def check_next_update(data_frame: pandas.DataFrame, colum: DataFrameColum) -> bool:
        
        if data_frame.empty == True:
            return True 
        
        data_frame.loc[data_frame[colum.value] == '-', colum.value] = datetime.now()
        
        actual_execution_time = datetime.now() 
        data_frame = data_frame.head(1)        
        next_update_time = data_frame[colum.value]
        next_update_time = next_update_time.iloc[0]
        
        if actual_execution_time > next_update_time:
            return True
        else:
            return False
    
    @staticmethod
    def set_take_profit_by_percentage_profit(data_frame: pandas.DataFrame, min_percentage_profit: float, take_profit_value: float) ->  pandas.DataFrame:
        
        for ind in data_frame.index:

            actual_profit = data_frame[DataFrameColum.PERCENTAGE_PROFIT.value][ind]
        
            if actual_profit < min_percentage_profit:
                
                data_frame.loc[ind, DataFrameColum.TAKE_PROFIT.value] = take_profit_value
                
        return data_frame
    
    @staticmethod
    def set_stop_loss_by_percentage_profit(data_frame: pandas.DataFrame, min_percentage_profit: float, stop_loss_value: float) ->  pandas.DataFrame:
        
        for ind in data_frame.index:

            actual_profit = data_frame[DataFrameColum.PERCENTAGE_PROFIT.value][ind]
        
            if actual_profit < min_percentage_profit:
                
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = stop_loss_value
                
        return data_frame
    
    @staticmethod
    def set_take_profit_by_top_gainer(data_frame: pandas.DataFrame) ->  pandas.DataFrame:
        
        for ind in data_frame.index:

            actual_top_gainer = data_frame[DataFrameColum.TOP_GAINERS.value][ind]
        
            if actual_top_gainer > 55:
                
                data_frame.loc[ind, DataFrameColum.TAKE_PROFIT.value] = 1.4
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 1.4
                
            elif actual_top_gainer > 45:
                
                data_frame.loc[ind, DataFrameColum.TAKE_PROFIT.value] = 1.2
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 1.2
            
            elif actual_top_gainer > 35:
                
                data_frame.loc[ind, DataFrameColum.TAKE_PROFIT.value] = 1
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 1
            
            else:
                
                data_frame.loc[ind, DataFrameColum.TAKE_PROFIT.value] = 0.8
                data_frame.loc[ind, DataFrameColum.STOP_LOSS.value] = 0.8
                
        return data_frame
    
    @staticmethod
    def create_empty_df(columns:List):
        """
        Crea un dataframe vacío con las columnas especificadas.
        """
        return pandas.DataFrame(columns=columns)

    @staticmethod
    def insert_column(df:pandas.DataFrame, key_column:str, data_df:pandas.DataFrame, data_column) -> pandas.DataFrame:
        """
        Inserta datos en una columna en un dataframe y crea la columna si no existe.
        La columna key_column se utiliza como clave para unir los dataframes.
        """
        if data_column not in df.columns:
            df[data_column] = None
        df = df.merge(data_df[[key_column, data_column]], on=key_column)
        return df
    
    @staticmethod
    def insert_rows(df:pandas.DataFrame, data_df:pandas.DataFrame):
        """
        Inserta las filas de un dataframe de datos al final de un dataframe principal.
        """
        return df.append(data_df, ignore_index=True)
    
    @staticmethod
    def count_true_rows(df:pandas.DataFrame, column_name):
        """
        Devuelve el número de filas que son True en una columna de un dataframe.
        """
        return df[column_name].sum()
    
    @staticmethod
    def count_false_rows(df:pandas.DataFrame, column_name):
        """
        Devuelve el número de filas que son False en una columna de un dataframe.
        """
        return len(df) - df[column_name].sum()
    
    """
    @staticmethod
    def get_last_value(df:pandas.DataFrame, column_name):
        if df.empty == True:
            return None
        return df[column_name].iloc[-1]
    """
    
    @staticmethod
    def get_last_value(df:pandas.DataFrame, column_name:DataFrameColum):
        if df.empty == True:
            return None
        return df[column_name.value].iloc[-1]
    
    @staticmethod
    def get_total_rows(df):
        return df.shape[0]

    @staticmethod
    def compare_dataframes(df1, df2):
        """ Compara dos DataFrames y devuelve True si son iguales, False si no lo son. """
        if df1.equals(df2):
            return True
        else:
            return False

    @staticmethod
    def replace_rows_df_backup_with_df_for_index (df_master: pandas.DataFrame, df_slave: pandas.DataFrame ):
        """Las filas del dataframe SLAVE las sustituye en el dataframe MASTER"""
        for index, row in df_slave.iterrows():
            df_master.loc[index] = df_slave.loc[index]

        return df_master
    
    @staticmethod
    def clone_columns(df: pandas.DataFrame, suffix:str):
        
        new_columns = []
        for column in df.columns:
            new_column = column + '_'+suffix
            df[new_column] = df[column]
            new_columns.append(new_column)
        
        return df
    
    @staticmethod
    def verify_and_create_columns(df, list_comns):
        # Función para verificar y crear columnas si no existen en el DataFrame
        for columna in list_comns:
            if columna not in df.columns:
                df[columna] = None  # Asignar un valor predeterminado
        
        return df