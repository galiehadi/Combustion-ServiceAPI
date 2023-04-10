docker stop services-combustion-rbg1
docker rm services-combustion-rbg1
docker image rm  services-combustion-rbg1:v1.5.6
docker image load -i services-combustion-rbg1-v1.5.6.tar
docker run -itd --name services-combustion-rbg1 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-rbg1:v1.5.6
