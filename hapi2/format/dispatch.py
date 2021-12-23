from .streamers import DotparStreamer, JSONStreamer

class FormatDispatcher:
    
    """ to add a custom data streamers, make a child class and update 
        the __REGISTERED_STREAMERS__ dict in the __init__"""
    
    __REGISTERED_STREAMERS__ = {
        'json': JSONStreamer,
        'text/hapi': DotparStreamer,
    }
    
    def getStreamer(self,basedir,header):
        fmt = header['content']['format']
        try:
            return self.__REGISTERED_STREAMERS__[fmt](
                basedir=basedir,header=header)
        except KeyError:
            raise Exception('unknown format "%s"'%fmt)
