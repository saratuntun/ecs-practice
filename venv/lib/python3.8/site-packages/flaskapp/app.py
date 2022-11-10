from flask import Flask
from Inference.Inference import model
import time
import os

# Load the model
model_path = os.getcwd() + "/Inference/Models/"
recommender = model(model_path=model_path+"KNN_model.sav",
                    items_path=model_path+"items.txt", users_path=model_path+"users.py")
app_cache = os.getcwd() + "/app_cache.txt"

app = Flask(__name__)
# Respond with an ordered comma separated list of up to 20 movie IDs
# in a single line from highest to lowest recommendation
@app.route("/recommend/<user_id>")
def recommend_to_user(user_id):
    if not user_id.isdigit():
        return f'invalid user ID'

    time_start = time.time()

    result = recommender.recommend(int(user_id))

    time_end = time.time()
    latency = time_end - time_start
    app_log(latency)
    return f'{",".join(result)}'



def app_log(latency):
    try:
        f = open(app_cache, "r")
    except:
        return "read log file failure"

    lines = f.readlines()

    last_lat, last_count = lines[len(lines)-1].strip().split(',')
    last_count = int(last_count)

    new_lat = latency
    new_count = last_count+1

    try:
        f = open(app_cache, "a")
    except:
        return "write log file failure"

    f.write(str(new_lat)+","+str(new_count)+"\n")
    f.close()