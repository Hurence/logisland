#!/bin/bash

:

rm /tmp/*.pid


/etc/init.d/nginx start
service sshd start



CMD=${1:-"exit 0"}
if [[ "$CMD" == "-d" ]];
then
	service sshd stop
	/usr/sbin/sshd -D -d
else
	/bin/bash -c "$*"
fi