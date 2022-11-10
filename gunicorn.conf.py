# ./gunicorn.conf.py
workers = 5     # Define the number of processes that are simultaneously started to process requests
worker_class = "gevent"   # The gevent library is used to support asynchronous request processing and improve throughput
bind = "0.0.0.0:3000" # Relax the Listening IP to facilitate the communication between Dockers and the host