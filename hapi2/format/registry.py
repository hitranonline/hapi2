#from .converters import ConverterDotpar, ConverterJSON

__REGISTERED_STREAMERS__ = {
#    'text/hapi': Converter_Dotpar,
#    'json': Converter_JSON,
}
        
def register(cls,fmt):
    __REGISTERED_STREAMERS__[fmt] = cls
    #return cls
        
#def register(fmt,converter):
#    """
#    Link the user-defined converter to the format Fmt
#    """
#    __REGISTERED_CONVERTERS__[fmt] = converter
