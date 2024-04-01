kill $(lsof -i:8082 |awk '{if(NR==2) print $2}')
