docker build -t services-combustion-rbg1:v1.5 .
docker image save -o ../services-combustion-rbg1-v1.5.tar services-combustion-rbg1:v1.5
rsync -Pavre "ssh -p 24019" ../services-combustion-rbg1-v1.5.tar root@10.7.1.116:~/CombustionOpt/zipped/.