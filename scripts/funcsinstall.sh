#!/usr/bin/bash


#sudo apt update; 
#sudo add-apt-repository ppa:deadsnakes/ppa; 
#sudo apt install python3.7 python3.7-venv -y;

#sudo chown func:func /home/func; 
#git clone https://github.com/serverless-analytics/dask-distributed.git -b chain-color /home/func/dask-distributed-pallette;
#cd /home/func/dask-distributed-pallette; 
#python3.7 -m venv venv37; 
#. venv37/bin/activate; 
#cd /home/func/dask-distributed-pallette;  
#pip install numpy==1.19.1 pandas==1.3.2 colorama httpio requests 'fsspec>=0.3.3'; 
#pip install -r requirements2.txt; 
#python setup.py install --force; 
#pip install -e .;

cd /home/func; 
echo -e '#!/bin/bash' > installsocialnetwork.sh; 
echo "cd /azure-functions-host-msr;" >> installsocialnetwork.sh; 
echo "rm -rf /azure-functions-host-msr/serverless-social-network;" >> installsocialnetwork.sh; 
echo "git clone https://github.com/serverless-analytics/serverless-social-network.git -b funcs;" >> installsocialnetwork.sh; 
echo "cd serverless-social-network/functions;" >> installsocialnetwork.sh; 
echo "cp -rf common /azure-functions-host-msr/dask-distributed;" >> installsocialnetwork.sh; 
echo "rm -rf *.json common;" >> installsocialnetwork.sh; 
echo "cp -rf * /azure-functions-host-msr/dask-distributed/phase2_worker;" >> installsocialnetwork.sh; 
echo "cd /azure-functions-host-msr/azure-functions-python-worker;" >> installsocialnetwork.sh; 
echo ". env/bin/activate;" >> installsocialnetwork.sh; 
echo "cd /azure-functions-host-msr/serverless-social-network;" >> installsocialnetwork.sh; 
echo "pip install -r requirements.txt" >> installsocialnetwork.sh; 
chmod 777 installsocialnetwork.sh;
sudo docker cp installsocialnetwork.sh serverlessdask:/azure-functions-host-msr/installsocialnetwork.sh;
sudo docker exec -d serverlessdask /azure-functions-host-msr/installsocialnetwork.sh;

