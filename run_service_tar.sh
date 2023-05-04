docker stop services-combustion-pct1
docker rm services-combustion-pct1
docker image rm  services-combustion-pct1:v1.6.1
docker image load -i services-combustion-pct1-v1.6.1.tar
docker run -itd --name services-combustion-pct1 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-pct1:v1.6.1
