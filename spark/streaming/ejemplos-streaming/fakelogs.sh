#!/usr/bin/env sh
rm /tmp/logdata
touch /tmp/logdata
tail -f /tmp/logdata | nc -lk 7777 &
TAIL_NC_PID=$!
cont=10000000
until [ $cont -lt 1 ];
do
    echo Vuelta numero: $cont
    cont=`expr $cont - 1`
    cat ./files/fake_logs/log1.log >> /tmp/logdata
    #sleep 1
    cat ./files/fake_logs/log2.log >> /tmp/logdata
    #sleep 1
    cat ./files/fake_logs/log1.log >> /tmp/logdata
    #sleep 1
    cat ./files/fake_logs/log1.log >> /tmp/logdata
    #sleep 1
    cat ./files/fake_logs/log2.log >> /tmp/logdata
done
kill $TAIL_NC_PID
