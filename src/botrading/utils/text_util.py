
class textUtil:
    
    @staticmethod
    def convert_text_mode_test(base, quote, symbol):
        if '_' in symbol:
            partes = symbol.split('_')
            if len(partes) == 2:
                return 'S' + base + 'S' + quote + '_' + 'S' + partes[1]
        return None