import time
import win32api

from binance.client import Client


class BinanceClientUtil:
    @staticmethod
    def synchronise_times(bnb_client: Client):
        """Es obligatorio lanzar esta función para hacer una sincronizción de
        tiempo BNB con nuestra maquina. Por este motivo es necesaria la ejecución
        de Visual studio con permisos de administrador.
        """
        gt = bnb_client.get_server_time()
        tt = time.gmtime(int((gt["serverTime"]) / 1000))
        win32api.SetSystemTime(tt[0], tt[1], 0, tt[2], tt[3], tt[4], tt[5], 0)
