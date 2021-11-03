from collections import OrderedDict
import sys
from pympler import asizeof
import logging
from common.utils import get_timestamp_ms


class LruCache:

    def __init__(self, capacity=100, name='lru_cache'):
        '''
        capacity: cache size in bytes
        '''
        self.cache = OrderedDict()
        self.capacity = int(capacity)
        self.available = int(capacity)
        self.name = name
        self.first_eviction = 0
        self.miss_count = 0
        self.hit_count = 0
        self.miss_byte_count = 0
        self.hit_byte_count = 0
        self.n_access = 0
        
        pass


    def evict(self, size):
        assert(size >=0)
        assert(size < self.capacity)
        freed = 0 
        if not self.first_eviction:
            self.first_eviction = get_timestamp_ms()

        evicted = []
        while freed < size:
            key, item = self.cache.popitem(last = False)
            obj_size = item['size'] #asizeof.asizeof(value)
            freed += obj_size
            evicted.append(key)
            #with open(f'{self.name}.cache.log', 'a') as fd:
            #    fd.write(f'lru,evict,{key},NA,{obj_size},{self.available},{self.capacity}\n')
            #logging.warning(f'lru evic {key},NA,{obj_size}, freed: {freed}, available: {self.available}, capacity {self.capacity}, requested size: {size}')
            if len(self.cache) == 0: return self.capacity
        return freed, evicted
        

    def put(self, key, value, size):
        # FIXME: for now I assume the value of each key
        # is python built in tpye. remember to fix the
        # size calculation. 
        #size = asizeof.asizeof(value)
        assert(size < self.capacity)
        self.miss_byte_count += size
        self.miss_count += 1

        evicted = []
        if self.available < size:
            freed, evicted = self.evict(size - self.available)
            if freed < (size - self.available): 
                logging.warning(f'LRUCache: cannot cache {key}, unable to free {size - self.available}')
                return
            elif freed == self.capacity:
                self.available = freed
            else:
                self.available += freed
            assert((self.available >= 0) and (self.available <= self.capacity)) 
        self.cache[key] = {'value': value, 'size': size}
        self.cache.move_to_end(key)
        self.available -= size
        assert((self.available >= 0) and (self.available <= self.capacity)) 
        #with open(f'{self.name}.cache.log', 'a') as fd:
        #    fd.write(f'lru,put,{key},NA,{size},{self.available},{self.capacity}\n')
        return evicted



    def get(self, key):
        self.n_access += 1
        if key not in self.cache:
            #with open(f'{self.name}.cache.log', 'a') as fd:
            #    fd.write(f'lru,get,{key},miss,-1,{self.available},{self.capacity}\n')
            return -1; 
        self.cache.move_to_end(key)
        #with open(f'{self.name}.cache.log', 'a') as fd:
        #    fd.write(f'lru,get,{key},hit,{asizeof.asizeof(self.cache[key])},{self.available},{self.capacity}\n')
        self.hit_byte_count += self.cache[key]['size'] #asizeof.asizeof(self.cache[key])
        self.hit_count += 1
        return self.cache[key]


    def get_status(self):
        return {'timestamp': get_timestamp_ms(), 'name': self.name, 
                'first_eviction': self.first_eviction, 'n_access': self.n_access,
                'miss_count': self.miss_count, 'miss_byte_count': self.miss_byte_count, 
                'hit_count': self.hit_count, 'hit_byte_count': self.hit_byte_count}
