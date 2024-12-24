#!/usr/bin/env python3
import json
import math
import sys

from kafka import KafkaConsumer

topic3 = sys.argv[3]

K = 32
submissionPoints = 0
elo = {}
up = {}
points = 0
best_contri = []
count = 0
end_count = 0
diff = ""
result = {}
result["user_elo_rating"] = {}


consumer = KafkaConsumer(
    topic3, value_deserializer=lambda m: json.loads(m.decode("ascii"))
)
for message in consumer:
    if "end" in message.value:
        break

    data = message.value
    topic = message.value[0]
    #print(data)
    #print(message.value[0])

    # ## CALCULATE ELO

    ## CONSUMER 1

    if topic == "problem":
        runtime_bonus = 10000 / int(data[8])  # float
        diff = data[4]  # float
        status = data[6]  # float
        user = data[1]

    ## CONSUMER 2

    if topic == "competition":
        runtime_bonus = 10000 / int(data[9])
        diff = data[5]
        status = data[7]
        user = data[2]

    ## Calculate Elo

    if topic == "problem" or topic == "competition":
        if diff == "Hard":
            diff_score = 1
        elif diff == "Medium":
            diff_score = 0.7
        elif diff == "Easy":
            diff_score = 0.3

        if status == "Passed":
            status_score = 1
        elif status == "TLE":
            status_score = 0.2
        else:
            status_score = -0.3

        submissionPoints = (
            float(float(K)) * float(status_score * diff_score) + runtime_bonus
        )

        if user not in elo:
            elo[user] = 1200
        # elo[user] += math.floor(submissionPoints)
        elo[user] += submissionPoints

        # print(f"{topic} : {user} : {elo[user]}")
        ## TODO: check for addition of ELO Rating
        result["user_elo_rating"][user] = math.floor(elo[user])

    # ## Upvote Counting
    ## CONSUMER 3
    if topic == "solution":
        data = message.value
        user = data[1]
        upvotes = int(data[4])

        if user not in up:
            up[user] = 0
        up[user] += upvotes


for votes in up:
    if up[votes] > points:
        best_contri.clear()
        best_contri.append(votes)
        points = up[votes]
    elif up[votes] == points:
        best_contri.append(votes)
result["best_contributor"] = best_contri
result["best_contributor"] = sorted(result["best_contributor"])

result["user_elo_rating"] = dict(sorted(result["user_elo_rating"].items()))
result = dict(sorted(result.items()))
print(json.dumps(result, indent=4))

consumer.close()
