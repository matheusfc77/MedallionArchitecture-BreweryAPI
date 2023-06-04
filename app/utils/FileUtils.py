import os

class FileUtils:
    '''
    Class with useful methods for file manipulation
    '''

    def create_folder(path):
        if not os.path.isdir(path):
            os.mkdir(path)