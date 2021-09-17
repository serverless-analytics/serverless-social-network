'''
This is the implementation of the color round robin policy
'''
import itertools

class ColorRoundRobin:
    def __init__(self, servers):
        self.servers = servers;
        self.it_servers = itertools.cycle(servers)
        self.localityhint_to_servers = {}

    def get_member(self, locality_hint):
        if locality_hint not in self.localityhint_to_servers:
            self.localityhint_to_servers[locality_hint] = next(self.it_servers)
        return self.localityhint_to_servers[locality_hint]
