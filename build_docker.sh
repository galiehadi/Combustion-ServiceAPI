clear
docker build -t services-combustion-rbg1:v1.5.5 .
docker image save -o ../services-combustion-rbg1-v1.5.5.tar services-combustion-rbg1:v1.5.5
ssh ichsan@10.7.1.116 -p 24019 'cp ~/SourceCode/zipped/services-combustion-rbg1-v1.5.4.tar ~/SourceCode/zipped/services-combustion-rbg1-v1.5.5.tar'
rsync -Pavre "ssh -p 24019" ../services-combustion-rbg1-v1.5.5.tar ichsan@10.7.1.116:~/SourceCode/zipped/.
rsync -Pavre "ssh -p 24019" run_service_tar.sh ichsan@10.7.1.116:~/SourceCode/zipped/.
ssh ichsan@10.7.1.116 -p 24019 'cd SourceCode/zipped/; ./run_service_tar.sh'
