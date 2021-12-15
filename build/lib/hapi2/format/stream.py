from . import converters

class Stream:
    
    def __init__(self,basedir,header):
        self.__basedir__ = basedir
        self.__header__ = header
        self.__classname__ = header['content']['__class__']
        self.__format__ = header['content']['__format__']
        
    def __iter__(self):
        
                
    def __len__(self):
        raise NotImplementedError # implemented everywhere except transitions - inheritance?
