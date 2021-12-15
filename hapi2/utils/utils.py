import zlib
import json
import pickle
import struct
# ___________________________________
# COMPRESSION

def compress_zlib(data):
    return zlib.compress(data)

def decompress_zlib(archive):
    return zlib.decompress(archive)

# ___________________________________
# BINARY PACKING
    
def pack_pickle(lst):
    return pickle.dumps(lst)
    
def unpack_pickle(buf):
    return pickle.loads(buf)

def pack_float(lst):
    # 4-byte float
    return struct.pack('%df'%len(lst),*lst)
    
def unpack_float(buf):
    # 4-byte float
    return struct.unpack('%df'%(len(buf)/4),buf) 
    
def pack_double(lst):
    # 8-byte double
    return struct.pack('%dd'%len(lst),*lst)
    
def unpack_double(buf):
    # 8-byte double
    return struct.unpack('%dd'%(len(buf)/8),buf) 

# ___________________________________
# JSON PACKING
    
def unpack_one(buf):
    """
    Expect single value from the json string.
    """
    pass

def unpack_many(buf):
    """
    Expect many values from the json string.
    """
    if not buf:
        return []
    try:
        vals = json.loads(buf)
        if type(vals) not in [tuple,list]:
            vals = [vals]
        return vals
    except ValueError:        
        return [buf]
    except TypeError:
        return [str(buf)]


