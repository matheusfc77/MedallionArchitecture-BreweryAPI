import datetime
import pytz

class Now:
    '''
    Class for return the local datetime
    '''

    def now(self, timezone='America/Sao_Paulo'):
        return datetime.datetime.now(pytz.timezone(timezone)).isoformat()
    