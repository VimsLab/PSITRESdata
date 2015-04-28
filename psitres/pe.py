from __future__ import print_function
import traceback
import multiprocessing as mp
import threading
from joblib import delayed


def _worker((callee, args, kwargs)):
    try:
        args, kwargs = list(args), dict(kwargs)
        if isinstance(callee, Local): 
            callee = callee()
        for i, v in enumerate(args): 
            if isinstance(v, Local): 
                args[i] = v()
        for k, v in kwargs.iteritems():
            if isinstance(v, Local): 
                kwargs[k] = v()        
        return callee(*args, **kwargs)
    except:
        traceback.print_exc()
        raise
    
    
def _initializer(initializer, initargs, local_items):
    for id_, obj in local_items:
        Local._init_pair(id_, obj) 
    if initializer is not None:
        initializer(*initargs)
        
        
class ParFor(object):
    def __init__(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        self._args = processes, initializer, initargs, maxtasksperchild
    def __call__(self, iterable):
        if not hasattr(iterable, '__len__'):
            iterable = tuple(iterable)
        processes, initializer, initargs, maxtasksperchild = self._args
        initializer, initargs = _initializer, (initializer, initargs, Local._items())
        pool = mp.Pool(processes, initializer, initargs, maxtasksperchild)        
        try:
            r = pool.map(_worker, iterable)
            pool.close()
            return r
        except:
            pool.terminate()
            raise
        finally:
            pool.join()
            
            
def mp_print(*args, **kwargs):
    ident = []
    ident.append(mp.current_process().ident)
    ident.append(threading.current_thread().ident)
    print(ident, *args, **kwargs)        


class Namespace(object):
    pass


class Local(object):
    _nmspc = Namespace()
    _lck = threading.RLock()
    
    @classmethod
    def _items(cls):
        return tuple((id_, ref) for (id_, (ref, _)) in cls._nmspc.__dict__.items())
    @classmethod
    def _init_pair(cls, id_, obj):
        with cls._lck:
            try:
                ref = cls.get(id_)
                if obj is not ref:
                    raise RuntimeError('id collision')
                cls.add_ref(id_)
            except AttributeError:
                ref, cnt = obj, threading.Semaphore(0)
                setattr(cls._nmspc, str(id_), (ref, cnt))        
    
    @classmethod
    def from_pair(cls, id_, obj):
        cls._init_pair(id_, obj)
        return cls(id_)
    @classmethod
    def from_id(cls, id_):
        obj = cls.get(id_)
        return cls.from_pair(id_, obj)
    @classmethod
    def from_obj(cls, obj):
        id_ = id(obj)
        return cls.from_pair(id_, obj)
        
    @classmethod
    def add_ref(cls, id_):
        ref, cnt = getattr(cls._nmspc, str(id_))
        cnt.release()
    @classmethod
    def remove_ref(cls, id_):
        ref, cnt = getattr(cls._nmspc, str(id_))
        if not cnt.acquire(False):
            delattr(cls._nmspc, str(id_))
    @classmethod
    def get(cls, id_):
        ref, cnt = getattr(cls._nmspc, str(id_))
        return ref
    
    def __init__(self, id_):
        self._id = id_
    def __call__(self):
        return self.get(self._id)
    def __del__(self):
        self.remove_ref(self._id)
    
    def __reduce__(self):
        return callattr, (Local, 'from_id', self._id)
    def __reduce_ex__(self, protocol):
        return self.__reduce__()
    
    
def callattr(object, name, *args, **kwargs):
    return getattr(object, name)(*args, **kwargs)

