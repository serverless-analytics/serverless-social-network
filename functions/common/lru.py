from collections import OrderedDict
import sys
from pympler import asizeof
import logging

class LruCache:
    def __init__(self, capacity=100, name='lru_cache'):
        '''
        capacity: cache size in bytes
        '''
        self.cache = OrderedDict()
        self.capacity = int(capacity)
        self.available = int(capacity)
        self.name = name
        pass


    def evict(self, size):
        assert(size >=0)
        assert(size < self.capacity)
        freed = 0 
        while freed < size:
            key, value = self.cache.popitem(last = False)
            obj_size = asizeof.asizeof(value)
            freed += obj_size
            with open(f'{self.name}.cache.log', 'a') as fd:
                fd.write(f'lru,evict,{key},NA,{obj_size},{self.available},{self.capacity}\n')
            if len(self.cache) == 0: return freed
        return freed 
        

    def put(self, key, value):
        # FIXME: for now I assume the value of each key
        # is python built in tpye. remember to fix the
        # size calculation. 
        size = asizeof.asizeof(value)
        assert(size < self.capacity)
        if self.available < size:
            freed = self.evict(size - self.available)
            if freed < size: 
                logging.warning(f'LRUCache: cannot cache {key}, unable to free {size - self.available}')
                return
            self.available += freed
            logging.warning(f'{self.available},{self.capacity}')
            assert((self.available >= 0) and (self.available <= self.capacity)) 
        self.cache[key] = value
        self.cache.move_to_end(key)
        self.available -= size
        assert((self.available >= 0) and (self.available <= self.capacity)) 
        with open(f'{self.name}.cache.log', 'a') as fd:
            fd.write(f'lru,put,{key},NA,{size},{self.available},{self.capacity}\n')


    def get(self, key):
        if key not in self.cache:
            with open(f'{self.name}.cache.log', 'a') as fd:
                fd.write(f'lru,get,{key},miss,-1,{self.available},{self.capacity}\n')
            return -1; 
        self.cache.move_to_end(key)
        with open(f'{self.name}.cache.log', 'a') as fd:
            fd.write(f'lru,get,{key},hit,{asizeof.asizeof(self.cache[key])},{self.available},{self.capacity}\n')
        return self.cache[key]
