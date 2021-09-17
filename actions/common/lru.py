
from collections import OrderedDict
import sys

class LruCache:
    def __init__(self, capacity=100):
        '''
        capacity: cache size in bytes
        '''
        self.cache = OrderedDict()
        self.capacity = int(capacity)
        self.available = int(capacity)
        pass


    def evict(self, size):
        freed = 0 
        while freed < size:
            key, value = self.cache.popitem(last = False)
            freed += sys.getsizeof(value)
        return freed 
        

    def put(self, key, value):
        # FIXME: for now I assume the value of each key
        # is python built in tpye. remember to fix the
        # size calculation. 
        with open('cache-log.log', 'a') as fd:
            fd.write(f'lru-put {key}, {value}\n')
        size = sys.getsizeof(value)
        if self.available < size:
            freed = self.evict(size - self.available)
            assert(freed >= size)
            self.available += freed
        self.cache[key] = value
        self.cache.move_to_end(key)


    def get(self, key):
        if key not in self.cache:
            with open('cache-log.log', 'a') as fd:
                fd.write(f'lru-get miss {key}\n')
            return -1; 
        self.cache.move_to_end(key)
        with open('cache-log.log', 'a') as fd:
            fd.write(f'lru-get hit {key}, {self.cache[key]}\n')
        return self.cache[key]
