clear
docker build -t services-combustion-pct1:v1.7 .
docker image save -o ../services-combustion-pct1-v1.7.tar services-combustion-pct1:v1.7
# ssh ichsan@10.7.1.116 -p 24017 'cp ~/CombustionOpt/zipped/services-combustion-pct1-v1.6.1.tar ~/CombustionOpt/zipped/services-combustion-pct1-v1.7.tar'
rsync -Pavre "ssh -p 24017" ../services-combustion-pct1-v1.7.tar ichsan@10.7.1.116:~/CombustionOpt/zipped/.
rsync -Pavre "ssh -p 24017" run_service_tar.sh ichsan@10.7.1.116:~/CombustionOpt/zipped/.
ssh ichsan@10.7.1.116 -p 24017 'cd CombustionOpt/zipped/; ./run_service_tar.sh'