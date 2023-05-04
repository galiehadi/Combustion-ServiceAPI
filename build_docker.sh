clear
docker build -t services-combustion-pct1:v1.6.1 .
docker image save -o ../services-combustion-pct1-v1.6.1.tar services-combustion-pct1:v1.6.1
ssh ichsan@10.7.1.116 -p 24016 'cp ~/CombustionOpt/zipped/services-combustion-pct1-v1.6.0.tar ~/CombustionOpt/zipped/services-combustion-pct1-v1.6.1.tar'
rsync -Pavre "ssh -p 24016" ../services-combustion-pct1-v1.6.1.tar ichsan@10.7.1.116:~/CombustionOpt/zipped/.
rsync -Pavre "ssh -p 24016" run_service_tar.sh ichsan@10.7.1.116:~/CombustionOpt/zipped/.
ssh ichsan@10.7.1.116 -p 24016 'cd CombustionOpt/zipped/; ./run_service_tar.sh'
