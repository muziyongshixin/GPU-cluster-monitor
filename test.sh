#!/bin/sh
#comments


remote_file_path="/home/xxx/icst0/clusterThreadCount/mygpustat.py"


DATE=$(date +%y-%m-%d-%H-%M-%S)
echo "one check is started====================================================$DATE"
for I in {101..110}; do
    cur_ip="192.31.32."$I
    echo -e "\033[45;37m check GPU on ===================================================== \033[0m" "\033[46;37m $cur_ip \033[0m"
    ssh $cur_ip "python  $remote_file_path "
done
 

python3 get_together.py
#rm  icst*.json



