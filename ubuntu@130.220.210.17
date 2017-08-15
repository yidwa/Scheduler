#! ps -p 6498 -o %cpu,%mem, | awk '{if(NR>1)print}'#! get the pid that listen on port for storm
#varlist=()
#port=()
#port+=('6700')
#port+=('6701')
#port+=('6702')
#port+=('6703')

#for j in {0..3}
#do
#        varlist+=($(sudo lsof -i :${port[$j]} | grep LISTEN | awk -F ' ' '{print $2}'))
#done
#varlist+=($(sudo lsof -i :6701 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6702 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6703 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6704 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6705 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6706 | grep LISTEN | awk -F ' ' '{print $2}'))
##varlist+=($(sudo lsof -i :6707 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6708 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6709 | grep LISTEN | awk -F ' ' '{print $2}'))
#varlist+=($(sudo lsof -i :6710 | grep LISTEN | awk -F ' ' '{print $2}'))

#! get cpu and memory usage for single process

for k in {0..3}
do
       varlist=()
       port=()
       port+=('6700')
       port+=('6701')
       port+=('6702')
       port+=('6703')

       for j in {0..3}
	do
        	varlist+=($(sudo lsof -i :${port[$j]} | grep LISTEN | awk -F ' ' '{print $2}'))
	done

	echo "$(date)" >> /home/ubuntu/script/performance

	for i in {0..3}
	do
        	t="${varlist[$i]}"
		if [ "$t" !=  "" ]
		then 
	        	cpu=$(ps -p $t  -o %cpu,%mem| awk -F ' ' '{print $1}'| awk '{if(NR>1)print}')
        		mem=$(ps -p $t  -o %cpu,%mem| awk -F ' ' '{print $2}'| awk '{if(NR>1)print}')
#			echo "$cpu, $mem"
                	echo "${port[$i]}", "$cpu", "$mem" >> /home/ubuntu/script/performance
		else
			echo  "${port[$i]}", "", "" >> /home/ubuntu/script/performance

#	else 
#		echo "null"
		fi


#	if ("${varlist[$i]}" !=  "");
#	then
#		cpu=$(ps -p ${varlist[$i]}  -o %cpu,%mem| awk -F ' ' '{print $1}'| awk '{if(NR>1)print}')
#		mem=$(ps -p ${varlist[$i]}  -o %cpu,%mem| awk -F ' ' '{print $2}'| awrk '{if(NR>1)print}')
#		echo "${port[$i]}", "$cpu", "$mem" >> /home/ubuntu/script/performance
#	else 
#		echo "none"
#	fi
	done
        
       echo "$(free -m | awk 'NR==2{print $3,$2,$3*100/$2}')" >> /home/ubuntu/script/performance
       echo "$(top -bn1 | grep "Cpu(s)" |  awk '{print $2+$4"%"}')" >> /home/ubuntu/script/performance
#for i i
#!	echo "done thread"
      sleep 10

done


