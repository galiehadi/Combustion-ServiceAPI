clear
docker build -t service-copt-amg1:v1.16 .
docker image save -o ../service-copt-amg1-v1.16.tar service-copt-amg1:v1.16

ssh ichsan@10.7.1.116 -p 24023 'cd /home/ichsan/SourceCode/zipped/; cp service-copt-amg1-v1.15.tar service-copt-amg1-v1.16.tar'
rsync -Pavre "ssh -p 24023" ../service-copt-amg1-v1.16.tar ichsan@10.7.1.116:/home/ichsan/SourceCode/zipped/.
rsync -Pavre "ssh -p 24023" run_service_tar.sh ichsan@10.7.1.116:/home/ichsan/SourceCode/zipped/.
ssh ichsan@10.7.1.116 -p 24023 'cd /home/ichsan/SourceCode/zipped/; ./run_service_tar.sh'
