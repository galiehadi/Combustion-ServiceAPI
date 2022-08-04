# docker stop services-combustion-rbg1
# docker rm services-combustion-rbg1
# docker image rm  services-combustion-rbg1:v1.5
# docker image load -i services-combustion-rbg1-v1.5.tar
# docker run -itd --name services-combustion-rbg1 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-rbg1:v1.5

docker build -t services-combustion-rbg1:v1.5 .
docker image save -o ../services-combustion-rbg1-v1.5.tar services-combustion-rbg1:v1.5
rsync -Pavre "ssh -p 24019" ../services-combustion-rbg1-v1.5.tar root@10.7.1.116:~/CombustionOpt/zipped/.
ssh root@10.7.1.116 -p 24019 'cd CombustionOpt/zipped/; ./run_service_tar.sh'
