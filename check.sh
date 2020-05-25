#!/bin/bash
if [ `curl -s 'http://petscan.wmflabs.org/?psid=16306546&format=csv' | grep -c 'Magnus_Manske'` == '0' ]; then
	echo 'restarting petscan'
	sudo killall petscan_rs
else
	echo 'no restart required'
fi
