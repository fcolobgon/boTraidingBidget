
import math
import numpy
from datetime import datetime, timedelta
from src.botrading.model.time_ranges import *

from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum


class MathCal_util:
    """  Aquí se almacenan funcionalidades para hacer calculos matematicos o algebra...  """

    def get_consecutive_timestamps(seconds):
        """Se devuelve dos timestamps consecutivos separados por x segundos"""
        now = datetime.now()
        #time_1 = datetime(now.year, now.month, now.day, now.hour, now.minute)
        date_current = datetime(now.year, now.month, 1, now.hour, now.minute)
        minutos, segundos = divmod(seconds, 60)
        horas, minutos = divmod(minutos, 60)
        dias, horas,  = divmod(horas, 24)

        date_previous = date_current - timedelta(days=dias, hours=horas, minutes=minutos, seconds=segundos)

        return date_previous.timestamp(), date_current.timestamp()

    """
    def reducir_float(precio, parte_entera= 1,parte_decinal=1): #! Estoy pensado de acomplarlo a la venta para ajustar al máximo 
        return math.floor(precio * 10**puntos) / 10**puntos - 0.01
    """
    
    @staticmethod
    def extract_decimal_part(number):

        if number == None:
            raise Exception("number is invalid")
        else:
            return round(
                number - MathCal_util.extract_whole_part(number),
                MathCal_util.length_decimal_part(number),
            )

    @staticmethod
    def extract_whole_part(number):
        """
        Usage:
            NumberUtils().extract_whole_part(1.101) returns '1'
        """
        if number is None:
            return 0
        else:
            return int(number)


    @staticmethod
    def length_decimal_part(number):
        """
        Usage:
            NumberUtils().length_decimal_part(1.00000000001000123) returns 17
        """
        if number == None:
            raise Exception("number is invalid")

        if str(number).find(".") >= 0:
            decimal_parte = str(number).split(".")[1]
            total_zero = str(decimal_parte).count("0")
            if len(decimal_parte) == total_zero:
                return 0
            return len(decimal_parte)
        else:
            return 0
    
    """
    def update_angle_values_current_vs_flat(data_frame: pandas.DataFrame, column_data:str, column_result:str, interval: str = '15min'):

        #interval = '15min' o '1h'

        for ind in data_frame.index:
            values = data_frame[column_data][ind]

            if data_frame[column_data][ind] != "-" and len(values) >= 2:
                values = pandas.array(data_frame[column_data][ind])
                degrees = MathCal_util.angle_prueba(values = values, interval=interval)
            else:
                degrees = None

            data_frame[column_result][ind] = degrees
            
        return data_frame 

    def gann_angle(values:list, num_last_values:float = -10):

        # cortar la lista por el final y mantener solo los últimos 10 elementos
        values_cut = values[num_last_values:]
        
        # convertir la lista en un DataFrame de Pandas
        df = pandas.DataFrame({'values': values_cut})

        # calcular el cambio de precio y el tiempo entre los puntos
        df['values_diff'] = df['values'].diff()
        df['time'] = numpy.arange(len(df))

        # calcular el ángulo de Gann
        df['angulo_radianes'] = numpy.arctan(df['values_diff'] / df['time'])

        # convertir el ángulo de Gann a grados y controlar los valores NaN
        angulos_grados = []
        for ind in range(len(df)):
            if not pandas.isna(numpy.isnan(df['angulo_radianes'][ind])) :
                angulos_grados.append(numpy.degrees(df['angulo_radianes'][ind]))

        return angulos_grados
    
    def angle_prueba (values, interval):
        # Resample de la serie de precios según el intervalo indicado
        values_series = pandas.Series(values[-2:], index=pandas.date_range(start=datetime.now(), periods=2, freq=interval))

        if interval == "15min":
            precio_resample = values_series.resample("15min").last()
        elif interval == "1h":
            precio_resample = values_series.resample("1h").last()
        else:
            print("Intervalo no válido")
            return None
        
        # Cálculo de la distancia en el eje x y en el eje y
        x_dist = len(precio_resample)
        y_dist = precio_resample[-1] - precio_resample[0]
        
        # Cálculo del ángulo
        angulo = numpy.arctan(y_dist / x_dist)
        angulo_degrees = numpy.degrees(angulo)
        
        return angulo_degrees
    """
    
    def calculate_angle(list_values, int_eje_x):

        # BTC: int_eje_x = 100 para 1h ; BTC: int_eje_x = 3 para 5m
        # Crear un array de valores X con números enteros consecutivos
        X = numpy.arange(0, len(list_values)*int_eje_x, int_eje_x)

        # Ajustar una línea recta a los datos de precios
        coeffs = numpy.polyfit(X, list_values, 1)

        # Obtener la pendiente de la línea recta
        slope = coeffs[0]

        # Calcular el ángulo de la pendiente en grados
        angle = numpy.degrees(numpy.arctan(slope))

        return angle

    def linear_regression(prices, dates):
        """
        Función que calcula la línea de regresión y el ángulo de inclinación para una serie de precios y fechas.

        Args:
            prices (list): Lista de precios.
            dates (list): Lista de fechas en formato "YYYY-MM-DD HH:MM:SS".

        Returns:
            tuple: Tupla que contiene la pendiente de la línea de regresión y el ángulo de inclinación en grados.
        """

        # Convierte la lista de fechas a un objeto DatetimeIndex.
        index = pandas.DatetimeIndex(dates)

        # Obtiene los valores x e y a partir del objeto DatetimeIndex y la lista de precios.
        x = index.astype(numpy.int64) // 10 ** 9
        y = numpy.array(prices)

        # Calcula la línea de regresión utilizando la función polyfit de NumPy.
        slope, intercept = numpy.polyfit(x, y, 1)

        # Calcula el ángulo de inclinación en radianes utilizando la función arctan de NumPy.
        rad_angle = numpy.arctan(slope)

        # Convierte el ángulo de inclinación a grados.
        deg_angle = numpy.degrees(rad_angle)

        # Retorna una tupla con la pendiente de la línea de regresión y el ángulo de inclinación en grados.
        return deg_angle
    
    def calcular_angulo_con_eje_horizontal(self,x1, y1, x2, y2):
        # Calcula las diferencias en x y y
        diff_x = x2 - x1
        diff_y = y2 - y1

        # Calcula el ángulo con el eje horizontal utilizando atan2
        angulo_radianes = math.atan2(diff_y, diff_x)
        angulo_grados = math.degrees(angulo_radianes)

        return angulo_grados
    
