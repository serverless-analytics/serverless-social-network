import configparser
import socket
import subprocess


def init_config(config_path):
    config = configparser.ConfigParser()

    config['DB'] = {
        'DB_PROVIDER': 'CouchDB',
        'DB_USERNAME': 'whisk_admin',
        'DB_PASSWORD': 'some_passw0rd',
        'DB_PROTOCOL': 'http',
        #'DB_HOST': 'rfonseca-dask.westus2.cloudapp.azure.com', #'172.17.0.1',
        'DB_HOST': '172.17.0.1',
        'DB_PORT': '5984'
    }

    config['MinIO'] = {
        #'ENDPOINT': 'rfonseca-dask.westus2.cloudapp.azure.com', #socket.gethostname(), # + '.ece.cornell.edu:9001',
        'ENDPOINT': socket.gethostname(), # + '.ece.cornell.edu:9001',
        'BUCKET': 'playground',
        'ACCESS_KEY': '5VCTEQOQ0GR0NV1T67GN',
        'SECRET_KEY': '8MBK5aJTR330V1sohz4n1i7W5Wv/jzahARNHUzi3'
    }

    WSK = 'Azure'
    #APIHOST = subprocess.check_output(
    #    WSK + ' property get --apihost', shell=True).split()[3].decode("utf-8")
    #AUTH_KEY = subprocess.check_output(
    #    WSK + ' property get --auth', shell=True).split()[2].decode("utf-8")
    #NAMESPACE = subprocess.check_output(
    #    WSK + ' property get --namespace', shell=True).split()[2].decode("utf-8")

    config['OpenWhisk'] = {
        'WSK': WSK,
        'APIHOST': 'http://localhost:7071',  #APIHOST,
        #'APIHOST': 'http://func-rfdask.westus2.cloudapp.azure.com:37000',
        'AUTH_KEY': '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP', #AUTH_KEY,
        'NAMESPACE': '_', #NAMESPACE,
    }

    with open(config_path, 'w') as configfile:
        config.write(configfile)
