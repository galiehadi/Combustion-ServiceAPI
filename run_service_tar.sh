docker stop service-copt-amg1
docker rm service-copt-amg1
docker image rm  service-copt-amg1:v1.16
docker image load -i service-copt-amg1-v1.16.tar
docker run -itd --name service-copt-amg1 --restart unless-stopped --memory="500M" -p 0.0.0.0:8083:8083 service-copt-amg1:v1.16
