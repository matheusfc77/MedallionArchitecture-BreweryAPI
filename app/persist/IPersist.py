
class IPersist:
    '''
    Class for standardizing data persistence
    '''

    def read(self, path, filename): raise NotImplementedError('Ipersist.read() not implemented')

    def write(self, path, filename, info): raise NotImplementedError('Ipersist.write() not implemented')