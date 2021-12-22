from hapi2.format.streamer import AbstractStreamer

class JSONStreamer(AbstractStreamer):
    def __iter__(self):
        for item in self.__header__['content']['data']:
            yield item
