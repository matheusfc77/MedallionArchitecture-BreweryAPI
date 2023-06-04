import requests
from math import ceil
from time import sleep

from app.utils.now import Now
from app.persist.IPersist import IPersist
from app.utils.FileUtils import FileUtils


class BreweryAPI(Now):
    '''
    Class to extract data of brewery API
    '''


    def __init__(self, persist):
        Now.__init__(self)

        if not IPersist in persist.__class__.__bases__:
            raise TypeError('Param [persist] needs to implement [IPersist]')
        
        self.persist = persist
        self.URL = 'https://api.openbrewerydb.org/v1/breweries'


    def _gen_list(self, max_value, per_page=50, limit_pages=-1):
        '''
        Create list for paged extraction

        Args
            int (max_value): number of records to extract
            int (per_page): number of records per page
            int (limit_pages): number of pages to extract

        Return
            list: list of tuples with (page_number, start_value, end_value)
        '''
        print('{} [EXTRACT] START GENERATE LIST'.format(self.now()))

        total_number_of_pages = ceil(max_value / per_page)
        number_of_pages = limit_pages if limit_pages > 0 else total_number_of_pages
        list_values_requests = [(page, page * per_page - per_page, page * per_page) for page in range(1, number_of_pages + 1)]

        print('{} [EXTRACT] END GENERATE LIST'.format(self.now()))

        return list_values_requests
    

    def _get_total_records(self):
        '''
        Checks number of API's records

        Raises
            SystemError: error in request
            TypeError: error in datatype returned
            ValueError: key [total] not exists in dictionary returned

        Return
            int: total records of the API
        '''
        print('{} [EXTRACT] START GETTING TOTAL RECORDS'.format(self.now()))

        key_total = 'total'
        rtn = requests.get(self.URL + '/meta')

        if rtn.status_code != 200: raise SystemError('Return error API: {}'.format(rtn.text))
        if not isinstance(rtn.json(), dict): raise TypeError('Unexpected data type returned. Expected: dicionary. Returned: {}. Info returned: {}'.format(type(rtn.json()), rtn.text))
        if key_total not in rtn.json().keys(): raise ValueError('Key [{}] not found'.format(key_total))

        print('{} [EXTRACT] END GETTING TOTAL RECORDS'.format(self.now()))

        return int(rtn.json()[key_total])
    

    def extract_records(self, path, filename, per_page=50, limit_pages=-1):
        '''
        Extract data of the API

        Args
            string (path): path to persist data
            string (filename): filename to persist data
            int (per_page): number of records per page
            int (limit_pages): number of pages to extract

        Raises
            SystemError: error in request
        '''
        print('{} [EXTRACT] START EXTRACTION RECORDS'.format(self.now()))

        total_records = self._get_total_records()
        list_pages = self._gen_list(total_records, per_page=per_page, limit_pages=limit_pages)
        
        folder = path + self.now().replace('-', '').replace(':', '').split('.')[0]
        FileUtils.create_folder(folder)

        for page, start, end in list_pages:
            rtn = requests.get(self.URL + '?per_page={}&page={}'.format(per_page, page))
            if rtn.status_code != 200: raise SystemError('Return error API: {}'.format(rtn.text))
            self.persist.write(folder + '/', '{}_{}_{}'.format(filename, start, end), rtn.json())
            sleep(.2)

        print('{} [EXTRACT] END EXTRACTION RECORDS'.format(self.now()))