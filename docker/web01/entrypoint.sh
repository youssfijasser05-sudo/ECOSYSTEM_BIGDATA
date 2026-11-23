#!/bin/sh
# Démarre la journalisation, le serveur SSH, puis Nginx au premier plan
rm -f /run/rsyslogd.pid
rsyslogd
/usr/sbin/sshd
exec nginx -g 'daemon off;'
