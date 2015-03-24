#!/bin/bash
poweroffPi() {
	for i in `seq 2 4`; do
		/usr/bin/ssh 192.168.0.11$i 'cat /etc/hostname;echo 'powering down';sudo poweroff'
	done
	echo 'finally: pi01 powering down'
	sudo poweroff
}
poweroffPi
