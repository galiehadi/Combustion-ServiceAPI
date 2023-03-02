# docker stop services-combustion-rbg1
# docker rm services-combustion-rbg1
# docker image rm  services-combustion-rbg1:v1.5
# docker image load -i services-combustion-rbg1-v1.5.tar
# docker run -itd --name services-combustion-rbg1 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-rbg1:v1.5

clear
docker build -t services-combustion-pct1:v1.5.4 .
docker image save -o ../services-combustion-pct1-v1.5.4.tar services-combustion-pct1:v1.5.4
ssh surya@10.7.1.116 -p 24016 'cp ~/CombustionOpt/zipped/services-combustion-pct1-v1.5.3.tar ~/CombustionOpt/zipped/services-combustion-pct1-v1.5.4.tar'
rsync -Pavre "ssh -p 24016" ../services-combustion-pct1-v1.5.4.tar surya@10.7.1.116:~/CombustionOpt/zipped/.
rsync -Pavre "ssh -p 24016" run_service_tar.sh surya@10.7.1.116:~/CombustionOpt/zipped/.
ssh surya@10.7.1.116 -p 24016 'cd CombustionOpt/zipped/; ./run_service_tar.sh'
