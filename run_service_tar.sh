NV="v1.8"

docker stop services-combustion-pct1
docker rm services-combustion-pct1
docker image rm  services-combustion-pct1:$NV
docker image load -i services-combustion-pct1-$NV.tar
docker run -itd --name services-combustion-pct1 --restart unless-stopped --memory="300M" -p 0.0.0.0:8083:8083 services-combustion-pct1:$NV
