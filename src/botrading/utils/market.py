from queue import Empty
import pandas
import numpy
import pandas_ta
import math
from datetime import datetime, timedelta
import time
import requests
import statistics

from src.botrading.utils import excel_util
from src.botrading.utils.enums.data_frame_colum import DataFrameColum


class Market:
    """  Aquí se almacenan funcionalidades que nos ayudan a saber el estado del mercado.    """

    @staticmethod
    def market_status_information (prices_history :dict, filtered_data_frame: pandas.DataFrame):
        """ Recogemos el prices_history que con valores de cualquier intervalo de tiempo. (15m, 1h, 1d)
            Y calculamos el profit de las posisiones -1 con -2 y -2 con -3, guardando este último en una nueva columna.
            Y Devolvemos cuantos profit positivos hay en el primer bloque, cuantos profits positivos del segundo bloque y dataframe con inf. 
        """
        filtered_data_frame['PRV_%_MKT'] = 0
        filtered_data_frame['CRRNT_%_MKT'] = 0
        filtered_data_frame['ASC_%_MKT'] = '-'

        for ind in filtered_data_frame.index:

            symbol = filtered_data_frame[DataFrameColum.SYMBOL.value][ind]
            prices_history_dic = prices_history[symbol]

            close = numpy.array(prices_history_dic['Close'].astype(float).values)

            if len(close) <= 3:
                current_profit = 0
                prev_profit = 0
            else:   
                last_price = close[-2]
                current_price = close[-1]
                current_profit =  (float(current_price)*100.0 / float(last_price))-100

                last_prev_price = close[-3]
                prev_price = close[-2]
                prev_profit =  (float(prev_price)*100.0 / float(last_prev_price))-100
                
            filtered_data_frame['CRRNT_%_MKT'][ind] = current_profit
            filtered_data_frame['PRV_%_MKT'][ind] = prev_profit

            if current_profit > prev_profit:
                filtered_data_frame['ASC_%_MKT'][ind] = 'ASC'
            elif current_profit < prev_profit:
                filtered_data_frame['ASC_%_MKT'][ind] = 'DESC'
            elif current_profit == prev_profit:
                filtered_data_frame['ASC_%_MKT'][ind] = 'EQUALS'

        count_positive_benefits_current = len(filtered_data_frame[filtered_data_frame['CRRNT_%_MKT'] > 0])
        #count_positive_benefits_current = (filtered_data_frame['CRRNT_%_MKT']> 0).sum()
        count_positive_benefits_previous = len(filtered_data_frame[filtered_data_frame['PRV_%_MKT'] > 0])
        #count_positive_benefits_previous = (filtered_data_frame['PRV_%_MKT']> 0).sum()

        return count_positive_benefits_current, count_positive_benefits_previous, filtered_data_frame


    @staticmethod
    def audit_market_status (prices_history :dict, filtered_data_frame: pandas.DataFrame):
        filtered_data_frame['PRV_%_MKT'] = 0
        filtered_data_frame['CRRNT_%_MKT'] = 0
        filtered_data_frame['ASC_%_MKT'] = '-'

        for ind in filtered_data_frame.index:

            symbol = filtered_data_frame[DataFrameColum.SYMBOL.value][ind]
            prices_history_dic = prices_history[symbol]

            close = numpy.array(prices_history_dic['Close'].astype(float).values)

            if len(close) <= 3:
                current_profit = 0
                prev_profit = 0
            else:   
                last_price = close[-2]
                current_price = close[-1]
                current_profit =  (float(current_price)*100.0 / float(last_price))-100

                last_prev_price = close[-3]
                prev_price = close[-2]
                prev_profit =  (float(prev_price)*100.0 / float(last_prev_price))-100

                if symbol == "BTCUSDT":
                    current_profit_btc = current_profit
                    asc_btc = current_profit > prev_profit

                    x1 = Market.precision_adjustment(value = close[-1])
                    x2 = x1*2

                    line_fija = ((x1, close[-2]), (x2, close[-2])) #fIJA
                    line_current = ((x1, close[-2]), (x2, close[-1]))

                    slope1 = Market.slope(line_fija[0][0], line_fija[0][1], line_fija[1][0], line_fija[1][1])
                    slope2 = Market.slope(line_current[0][0], line_current[0][1], line_current[1][0], line_current[1][1])

                    angle_btc = math.degrees(math.atan((slope2-slope1)/(1+(slope2*slope1))))
                
            filtered_data_frame['CRRNT_%_MKT'][ind] = current_profit
            filtered_data_frame['PRV_%_MKT'][ind] = prev_profit

            if current_profit > prev_profit:
                filtered_data_frame['ASC_%_MKT'][ind] = 'ASC'
            elif current_profit < prev_profit:
                filtered_data_frame['ASC_%_MKT'][ind] = 'DESC'
            elif current_profit == prev_profit:
                filtered_data_frame['ASC_%_MKT'][ind] = 'EQUALS'
        
        count_positive_benefits_current = 0
        count_negative_benefits_current = 0
        count_positive_benefits_current = len(filtered_data_frame[filtered_data_frame['CRRNT_%_MKT'] > 0])
        count_negative_benefits_current = len(filtered_data_frame[filtered_data_frame['CRRNT_%_MKT'] < 0])

        # Lista de datos
        data = [[datetime.now(), len(filtered_data_frame), count_positive_benefits_current, count_negative_benefits_current, current_profit_btc, asc_btc, angle_btc]]
        # Creación del DataFrame
        df_data = pandas.DataFrame(data, columns=['Time', 'how_many_coins', 'cnt_pstv_prft_crrnt', 'cnt_ngtv_prft_crrnt', 'Crrnt_%_BTC', 'Asc_BTC', 'Angle_BTC'])

        filtered_df_status_market = excel_util.load_dataframe(exel_name= 'StatusMarket')

        frames = [filtered_df_status_market, df_data]
        filtered_df_status_market = pandas.concat(frames)

        excel_util.save_data_frame( data_frame=filtered_df_status_market, exel_name="StatusMarket.xlsx")


    @staticmethod
    def update_linear_regression_prices(data_frame: pandas.DataFrame, column_result:str, prices_history_dic :dict, last_values:int=-10):
        """Mirar https://realpython.com/linear-regression-in-python/"""

        for ind in data_frame.index:

            symbol = data_frame[DataFrameColum.SYMBOL.value][ind]
            prices_history_only_symbol = prices_history_dic[symbol]

            close = prices_history_only_symbol['Close'].astype(float)
            close = close[last_values:]

            element_close = float(close.tail(1))

            value_precision = Market.precision_adjustment(value = element_close)

            longitud =  [x for x in range(0, len(close))]
            longitud_precision = [element + value_precision for element in longitud]

            long_series = pandas.Series (longitud_precision)

            data_frame[column_result][ind] = pandas_ta.linear_regression(x= long_series, y = close)['line']
        
        return data_frame

    @staticmethod
    def precision_adjustment(value: float) -> float: #! Versión 22-8-12
        value_final = 0.0

        if value <0:
            value= abs(value)
        
        value = numpy.format_float_positional (value)

        pos = str(value).find('.')
        decimal = str(value)[pos+1:]
        entera = str(value)[:pos]

        if len(str(entera)) > 1 and int(entera) > 9:
            value_entera = 1

            len_decimal = len(str(entera))
            value_entera = str(value_entera).ljust(len_decimal,"0")
            value_final = float (value_entera)

        elif len(str(entera)) <= 1 and int(entera) > 1:
            #hay decimales
            value_final = 1
        elif len(str(entera)) == 1 and int(entera) <= 1:
            value_x = 5
            len_decimal = len(decimal)

            if len_decimal <=1:
                value_x = "0." + str(value_x).rjust(len_decimal,"0")

            elif len_decimal >= 2 and len_decimal <= 15:
                value_x = "0." + str(value_x).rjust(2,"0")
            
            elif len_decimal >= 16:
                value_x = "0." + str(value_x).rjust(4,"0")
            value_final = float (value_x)
        
        return value_final

    @staticmethod
    def list_is_ascending(check_list:numpy=[], max_elemets:int = 2) -> bool:

        list_a = list(check_list[-max_elemets:])
        list_b = list(sorted(check_list[-max_elemets:]))

        if list_a == list_b:
            return True
        else:
            return False

    @staticmethod
    def list_is_descending(check_list:numpy=[], max_elemets:int = 2) -> bool:

        list_a = list(check_list[-max_elemets:])
        list_b = list(sorted(check_list[-max_elemets:],reverse = True))

        if list_a == list_b:
            return True
        else:
            return False

    @staticmethod
    def slope(x1, y1, x2, y2): # Line slope given two points:
        return (y2-y1)/(x2-x1)

    
    @staticmethod
    def update_angle_values_current_vs_flat(data_frame: pandas.DataFrame, column_data:str, column_result:str):

        for ind in data_frame.index:

            if data_frame[column_data][ind] != "-":
                values = pandas.array(data_frame[column_data][ind])

                x1 = Market.precision_adjustment(value = values[-1])
                x2 = x1*2

                line_fija = ((x1, values[-2]), (x2, values[-2])) #fIJA
                line_current = ((x1, values[-2]), (x2, values[-1]))

                slope1 = Market.slope(line_fija[0][0], line_fija[0][1], line_fija[1][0], line_fija[1][1])
                slope2 = Market.slope(line_current[0][0], line_current[0][1], line_current[1][0], line_current[1][1])

                degrees = math.degrees(math.atan((slope2-slope1)/(1+(slope2*slope1))))

            else:
                degrees = 0

            data_frame[column_result][ind] = degrees
            
        return data_frame 
    

    @staticmethod
    def get_usdt_market_status_with_average_price(time_sleep = 30):
        """Obtener la información del mercado de criptomonedas y luego 
        utilizar esa información para determinar el estado del mercado USDT."""
        
        all_coin_prices_1 = Market.get_all_prices()
        time.sleep (time_sleep)
        all_coin_prices_2 = Market.get_all_prices()

        bullish = 0
        bearish = 0
        all_coin = len (all_coin_prices_2)

        for key in all_coin_prices_1.keys() | all_coin_prices_2.keys(): 
            current_price = all_coin_prices_2.get(key, 0)

            total_market_price = all_coin_prices_1.get(key, 0) + all_coin_prices_2.get(key, 0)
            average_price = total_market_price/2

            if current_price >= average_price:
                bullish += 1
            else:
                bearish += 1

        return all_coin, bullish, bearish


    @staticmethod
    def get_all_prices():
        url = "https://api.binance.com/api/v3/ticker/price"

        response = requests.get(url)

        data = response.json()
        all_coin_prices = {coin['symbol']: float(coin['price']) for coin in data if coin['symbol'].endswith('USDT')}

        return all_coin_prices

    
    @staticmethod
    def get_market_status_standard_deviation():
        """se usa la desviación estándar como una medida de la volatilidad de los precios. Si la desviación estándar es baja (menor de 0,05), 
            se considera que el mercado es estable. Si la desviación estándar es moderada (menor de 0,1), se considera que el mercado es volátil. 
            Si la desviación estándar es alta (mayor de 0,1), se considera que el mercado es altamente volátil.
        """

        binance_api_url = "https://api.binance.com/api/v3/ticker/price"
        
        # Obtener información de precios de todas las monedas en Binance
        response = requests.get(binance_api_url)
        data = response.json()
        all_coin_prices = [float(coin['price']) for coin in data if coin['symbol'].endswith('USDT')]

        # Calcular la desviación estándar de los precios
        standard_deviation = statistics.stdev(all_coin_prices)

        # Determinar el estado del mercado en función de la volatilidad
        if standard_deviation < 0.05:
            market_status = 'Stable'
        elif standard_deviation < 0.1:
            market_status = 'Volatile'
        else:
            market_status = 'Highly Volatile'

        return market_status


    def get_market_status_predicting_future_value(): #! Pendiente de probar
        """
        Para calcular el estado del mercado de USDT en Python contra Binance, 
        puedes utilizar la API de Binance para obtener los precios y la volatilidad de USDT. 
        Luego, puedes aplicar un modelo de probabilidad y estadística como el mencionado anteriormente 
        para predecir el precio futuro y determinar el estado del mercado.
        """
        
        binance_api_url = "https://api.binance.com/api/v3/ticker/price"

        # Obtener información de precios y volatilidad de USDT en Binance
        response = requests.get(binance_api_url)
        data = response.json()
        df = pandas.DataFrame(data)

        usdt_price = [float(coin['price']) for coin in data if coin['symbol'].endswith('USDT')]
        usdt_volatility = numpy.std(usdt_price)

        # Predecir el precio futuro de USDT con un modelo de regresión lineal
        X = numpy.array(usdt_price).reshape(-1, 1)
        y = numpy.array(usdt_volatility).reshape(-1, 1)
        model = LinearRegression().fit(X, y)
        future_price = model.predict(numpy.array(usdt_price + 1).reshape(-1, 1))

        # Determinar el estado del mercado
        market_status = 'Bullish' if future_price > usdt_price else 'Bearish'

        return market_status


    def calculate_usdt_dominance(): #! Pendiente de probar
        """En este ejemplo, usdt_symbol es el símbolo de USDT (por ejemplo, "USDT") y 
        binance_api_url es la URL de la API de Binance para obtener la información de 
        precios y capitalización de mercado de las criptomonedas. La función devuelve el 
        porcentaje de dominación de USDT en el mercado.
        """

        binance_api_url = "https://api.binance.com/api/v3/ticker/price"

        response = requests.get(binance_api_url)
        data = response.json()
        all_coin_prices = {coin['symbol']: float(coin['price']) for coin in data if coin['symbol'].endswith('USDT')}

        for symbol, price in all_coin_prices.items():

            symbol = symbol.replace("USDT", "")

            response = requests.get(f"{binance_api_url}/{symbol}/circulating_supply")
            data = response.json()
            circulating_supply = data['circulating_supply']

            marketCap = float(price) * float(circulating_supply)
            
            {symbol: (price, marketCap)}

        all_coin_prices = {coin['symbol']: (coin['price'], coin['marketCap']) for coin in data}

        # Calcular la dominación de USDT
        usdt_price, usdt_market_cap = all_coin_prices['USDT']
        total_market_cap = sum(market_cap for _, market_cap in all_coin_prices.values())
        dominance = (usdt_market_cap / total_market_cap) * 100

        return dominance
    
    @staticmethod
    def get_market_status_standard_deviation():
        """se usa la desviación estándar como una medida de la volatilidad de los precios. Si la desviación estándar es baja (menor de 0,05), 
            se considera que el mercado es estable. Si la desviación estándar es moderada (menor de 0,1), se considera que el mercado es volátil. 
            Si la desviación estándar es alta (mayor de 0,1), se considera que el mercado es altamente volátil.
        """

        binance_api_url = "https://api.binance.com/api/v3/ticker/price"
        
        # Obtener información de precios de todas las monedas en Binance
        response = requests.get(binance_api_url)
        data = response.json()
        all_coin_prices = [float(coin['price']) for coin in data if coin['symbol'].endswith('USDT')]

        # Calcular la desviación estándar de los precios
        standard_deviation = statistics.stdev(all_coin_prices)

        # Determinar el estado del mercado en función de la volatilidad
        if standard_deviation < 0.05:
            market_status = 'Stable'
        elif standard_deviation < 0.1:
            market_status = 'Volatile'
        else:
            market_status = 'Highly Volatile'

        return market_status

    def calculate_usdt_dominance():
        """En este ejemplo, usdt_symbol es el símbolo de USDT (por ejemplo, "USDT") y 
        binance_api_url es la URL de la API de Binance para obtener la información de 
        precios y capitalización de mercado de las criptomonedas. La función devuelve el 
        porcentaje de dominación de USDT en el mercado.
        """

        binance_api_url = "https://api.binance.com/api/v3/ticker/price"

        response = requests.get(binance_api_url)
        data = response.json()
        all_coin_prices = {coin['symbol']: float(coin['price']) for coin in data if coin['symbol'].endswith('USDT')}

        for symbol, price in all_coin_prices.items():

            symbol = symbol.replace("USDT", "")

            response = requests.get(f"{binance_api_url}/{symbol}/circulating_supply")
            data = response.json()
            circulating_supply = data['circulating_supply']

            marketCap = float(price) * float(circulating_supply)
            
            {symbol: (price, marketCap)}

        all_coin_prices = {coin['symbol']: (coin['price'], coin['marketCap']) for coin in data}

        # Calcular la dominación de USDT
        usdt_price, usdt_market_cap = all_coin_prices['USDT']
        total_market_cap = sum(market_cap for _, market_cap in all_coin_prices.values())
        dominance = (usdt_market_cap / total_market_cap) * 100

        return dominance

    @staticmethod
    def update_angle_values_current_vs_flat_2(data_frame: pandas.DataFrame, column_data:str, column_result:str):

        for ind in data_frame.index:

            if data_frame[column_data][ind] != "-":
                values = pandas.array(data_frame[column_data][ind])[-2:]
                degrees = Market.angle(values)

            else:
                degrees = 0

            data_frame[column_result][ind] = degrees
            
        return data_frame 
    
    @staticmethod
    def angle(values): # ENTRADA [(x, y), (x, y)] [(Previous),(cuerrent)]
        


        x_diff = values[1][0] - values[0][0]
        y_diff = values[1][1] - values[0][1]
        angle = math.atan2(y_diff, x_diff)
        
        return math.degrees(angle)





