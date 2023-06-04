from app.persist.IPersist import IPersist
from app.utils.now import Now

import json


class JSONPersist(IPersist, Now):
    '''
    Class to implements JSON persistence
    '''

    def __init__(self, verbose=True):
        Now.__init__(self)
        self.verbose = verbose


    def read(self, path, filename):
        '''
        Read JSON file

        Args
            string (path): path to read file
            string (filename): name of file

        Return
            list: list of dictionaries with file data
        '''
        if self.verbose: print('{} [PERSIST] START READING [{}/{}]'.format(self.now(), path, filename))

        with open(path + filename) as file:
            data = json.load(file)

        if self.verbose: print('{} [PERSIST] END READING [{}/{}]'.format(self.now(), path, filename))
        return data


    def write(self, path, filename, info):
        '''
        Persist JSON file

        Args
            string (path): path to read file
            string (filename): name of file
            list (info): list of dictionaries to persist
        '''
        if self.verbose: print('{} [PERSIST] START WRITING [{}{}]'.format(self.now(), path, filename))

        with open(path + '/' + filename + '.json', 'w') as outfile:
            json.dump(info, outfile) #, indent=4)

        if self.verbose: print('{} [PERSIST] END WRITING [{}{}]'.format(self.now(), path, filename))

