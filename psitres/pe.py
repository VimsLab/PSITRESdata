from __future__ import print_function
import traceback
import multiprocessing as mp
import threading
import joblib
import functools


def delayed(*args, **kwargs):
    return joblib.delayed(*args, **kwargs)
try:
    delayed = functools.wraps(joblib.delayed)(delayed)
except AttributeError:
    pass


def _callattr(object_, name, *args, **kwargs):
    return getattr(object_, name)(*args, **kwargs)


class _Namespace(object):
    pass
    
    
def _initializer(initializer, initargs, local_items):
    for id_, obj in local_items:
        Local._init_pair(id_, obj) 
    if initializer is not None:
        initializer(*initargs)
        
        
def _worker((callee, args, kwargs)):
    try:
        args, kwargs = list(args), dict(kwargs)
        if isinstance(callee, Local): 
            callee = callee.get()
        for i, v in enumerate(args): 
            if isinstance(v, Local): 
                args[i] = v.get()
        for k, v in kwargs.iteritems():
            if isinstance(v, Local): 
                kwargs[k] = v.get()        
        return callee(*args, **kwargs)
    except:
        traceback.print_exc()
        raise
    
    
def _mp_print(*args, **kwargs):
    ident = []
    ident.append(mp.current_process().ident)
    ident.append(threading.current_thread().ident)
    print(ident, *args, **kwargs)    
    
    
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
            
            
class Local(object):
    _nmspc = _Namespace()
    _lck = threading.RLock()
    
    @classmethod
    def _items(cls):
        return tuple((id_, ref) for (id_, (ref, _)) in cls._nmspc.__dict__.iteritems())
    @classmethod
    def _init_pair(cls, id_, obj):
        with cls._lck:
            try:
                ref = cls._get(id_)
                if obj is not ref:
                    raise RuntimeError('id collision')
                cls._add_ref(id_)
            except AttributeError:
                ref, cnt = obj, threading.Semaphore(0)
                setattr(cls._nmspc, str(id_), (ref, cnt))            
    @classmethod
    def _from_pair(cls, id_, obj):
        cls._init_pair(id_, obj)
        return cls(id_)
    
    @classmethod
    def from_id(cls, id_):
        obj = cls._get(id_)
        return cls._from_pair(id_, obj)
    @classmethod
    def from_obj(cls, obj):
        id_ = None
        for id_, ref in cls._items():
            if obj is ref:
                break
        else:
            id_ = id(obj)
        return cls._from_pair(id_, obj) 
        
    @classmethod
    def _add_ref(cls, id_):
        _, cnt = getattr(cls._nmspc, str(id_))
        cnt.release()
    @classmethod
    def _remove_ref(cls, id_):
        _, cnt = getattr(cls._nmspc, str(id_))
        if not cnt.acquire(False):
            delattr(cls._nmspc, str(id_))
    @classmethod
    def _get(cls, id_):
        ref, _ = getattr(cls._nmspc, str(id_))
        return ref
    
    def __init__(self, id_):
        self._id = id_
    def get(self):
        return self._get(self._id)
    def __del__(self):
        self._remove_ref(self._id)
    
    def __reduce__(self):
        return _callattr, (Local, 'from_id', self._id)
    def __reduce_ex__(self, protocol):
        return self.__reduce__()

