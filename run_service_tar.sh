docker stop services-combustion-pct1
docker rm services-combustion-pct1
docker image rm  services-combustion-pct1:v1.7
docker image load -i services-combustion-pct1-v1.7.tar
docker run -itd --name services-combustion-pct2 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-pct2:v1.7
