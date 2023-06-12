NV="v1.8"
OV="v1.7"

clear
docker build -t services-combustion-pct1:$NV .
docker image save -o ../services-combustion-pct1-$NV.tar services-combustion-pct1:$NV
ssh surya@10.7.1.116 -p 24036 "cp ~/CombustionOpt/zipped/services-combustion-pct1-$OV.tar ~/CombustionOpt/zipped/services-combustion-pct1-$NV.tar"
rsync -Pavre "ssh -p 24036" ../services-combustion-pct1-$NV.tar surya@10.7.1.116:~/CombustionOpt/zipped/.
rsync -Pavre "ssh -p 24036" run_service_tar.sh surya@10.7.1.116:~/CombustionOpt/zipped/.
ssh surya@10.7.1.116 -p 24036 "cd CombustionOpt/zipped/; ./run_service_tar.sh"