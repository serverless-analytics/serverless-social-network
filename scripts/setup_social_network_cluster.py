import sys
import subprocess as sp
import json

def get_workers_list(fpath, n_workers):
    with open(fpath, 'r') as fd:
        t_members = json.load(fd)['CacheList']
        return t_members[0:int(n_workers)]

def get_workers(wlist):
    workers = []
    for winfo in wlist:
        addr = f'func@{winfo["address"].replace("http://", "").split(":")[0]}'
        port = f'{int(winfo["address"].replace("http://", "").split(":")[1]) - 15000}'
        workers.append({'addr': addr, 'port': port})
    return workers


# only start dask
def setup_dask_componenets(workers_fp, n_workers):
    print('restart dask components: workers and scheduler')

    workers = get_workers(get_workers_list(workers_fp, n_workers))
    print('change dask configuration on all dask scheduler .....')
    
    for wrk in workers:
        print(wrk)
        #sp.run(f'ssh {wrk["addr"]} -p {wrk["port"]}  sudo chown func:func /home/func;', shell=True, check=False)
        #sp.run(f'ssh {wrk["addr"]} -p {wrk["port"]}  rm -rf /home/func/dask-distributed-pallette;', shell=True, check=False)
        sp.run(f'scp -P {wrk["port"]}  funcsinstall.sh {wrk["addr"]}:/home/func/funcsinstall.sh', shell=True, check=False)
        sp.run(f'ssh {wrk["addr"]} -p {wrk["port"]}  chmod 777 /home/func/funcsinstall.sh', shell=True, check=False)
        sp.run(f'ssh {wrk["addr"]} -p {wrk["port"]}  /home/func/funcsinstall.sh;', shell=True, check=False)


        #print(wrk)

    return


workers_fp = sys.argv[1]
n_workers = sys.argv[2]
setup_dask_componenets(workers_fp, n_workers)


