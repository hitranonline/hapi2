from abc import ABC, abstractmethod

class AbstractStreamer(ABC):

    def __init__(self,basedir,header):
        self.__basedir__ = basedir
        self.__header__ = header
        self.__classname__ = header['content']['class']
        self.__format__ = header['content']['format']
    
    @abstractmethod
    def __iter__(self,header):
        pass
