clear
docker build -t services-combustion-pct1:v1.5.5 .
docker image save -o ../services-combustion-pct1-v1.5.5.tar services-combustion-pct1:v1.5.5
# ssh ichsan@10.7.1.116 -p 24016 'cp ~/CombustionOpt/zipped/services-combustion-pct1-v1.5.4.tar ~/CombustionOpt/zipped/services-combustion-pct1-v1.5.5.tar'
rsync -Pavre "ssh -p 24016" ../services-combustion-pct1-v1.5.5.tar ichsan@10.7.1.116:~/CombustionOpt/zipped/.
rsync -Pavre "ssh -p 24016" run_service_tar.sh ichsan@10.7.1.116:~/CombustionOpt/zipped/.
ssh ichsan@10.7.1.116 -p 24016 'cd CombustionOpt/zipped/; ./run_service_tar.sh'
