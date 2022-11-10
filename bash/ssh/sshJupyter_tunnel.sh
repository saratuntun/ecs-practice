#!/bin/bash
source 'bash/.export_credentials.sh'
export user_id=mangyinm
export tunnel_local_port=5000
echo 'ssh tunnelling...'
ssh -i ~/.ssh/kafka_id_rsa -L $tunnel_local_port:localhost:8888 $user_id@$machine_ip -NT