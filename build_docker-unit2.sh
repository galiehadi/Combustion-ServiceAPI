# docker stop services-combustion-rbg1
# docker rm services-combustion-rbg1
# docker image rm  services-combustion-rbg1:v1.5
# docker image load -i services-combustion-rbg1-v1.5.tar
# docker run -itd --name services-combustion-rbg1 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-rbg1:v1.5

docker build -t services-combustion-rbg2:v1.5.4 .
docker image save -o ../services-combustion-rbg2-v1.5.4.tar services-combustion-rbg2:v1.5.4
ssh root@10.7.1.116 -p 24020 'cp ~/CombustionOpt/zipped/services-combustion-rbg2-v1.5.3.tar ~/CombustionOpt/zipped/services-combustion-rbg2-v1.5.4.tar'
rsync -Pavre "ssh -p 24020" ../services-combustion-rbg2-v1.5.4.tar root@10.7.1.116:~/CombustionOpt/zipped/.
rsync -Pavre "ssh -p 24020" run_service_tar.sh root@10.7.1.116:~/CombustionOpt/zipped/.
ssh root@10.7.1.116 -p 24020 'cd CombustionOpt/zipped/; ./run_service_tar.sh'
