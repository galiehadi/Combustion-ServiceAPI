clear
docker build -t services-combustion-rbg2:v1.5.6 .
docker image save -o ../services-combustion-rbg2-v1.5.6.tar services-combustion-rbg2:v1.5.6
ssh ichsan@10.7.1.116 -p 24020 'cp ~/SourceCode/zipped/services-combustion-rbg2-v1.5.5.tar ~/SourceCode/zipped/services-combustion-rbg2-v1.5.6.tar'
rsync -Pavre "ssh -p 24020" ../services-combustion-rbg2-v1.5.6.tar ichsan@10.7.1.116:~/SourceCode/zipped/.
rsync -Pavre "ssh -p 24020" run_service_tar.sh ichsan@10.7.1.116:~/SourceCode/zipped/.
ssh ichsan@10.7.1.116 -p 24020 'cd SourceCode/zipped/; ./run_service_tar.sh'
