$NV = "v1.8"
$OV = "v1.7"

clear
docker build -t services-combustion-pct1:$NV .
docker image save -o ../services-combustion-pct1-$NV.tar services-combustion-pct1:$NV
ssh ichsan@10.7.1.116 -p 24016 "cp ~/CombustionOpt/zipped/services-combustion-pct1-$OV.tar ~/CombustionOpt/zipped/services-combustion-pct1-$NV.tar"
rsync -Pavre "ssh -p 24016" ../services-combustion-pct1-$NV.tar ichsan@10.7.1.116:~/CombustionOpt/zipped/.
rsync -Pavre "ssh -p 24016" run_service_tar.sh ichsan@10.7.1.116:~/CombustionOpt/zipped/.
ssh ichsan@10.7.1.116 -p 24016 "cd CombustionOpt/zipped/; ./run_service_tar.sh"