docker stop services-combustion-rbg2
docker rm services-combustion-rbg2
docker image rm  services-combustion-rbg2:v1.5.4
docker image load -i services-combustion-rbg2-v1.5.4.tar
docker run -itd --name services-combustion-rbg2 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-rbg2:v1.5.4
