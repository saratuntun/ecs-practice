echo "Starting kafka stream..."
source './bash/.export_credentials.sh'
ssh -i ~/.ssh/kafka_id_rsa -L 9092:localhost:9092 tunnel@$kafka_ip -NT