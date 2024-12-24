#!/usr/bin/env python3
import json
import math
import sys

from kafka import KafkaConsumer

topic1 = sys.argv[1]
topic2 = sys.argv[2]
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
    topic1, topic2, topic3, value_deserializer=lambda m: json.loads(m.decode("ascii"))
)
for message in consumer:
    if "end" in message.value:
        end_count += 1
        if end_count == 3:
            break
        else:
            continue
    count += 1

    data = message.value
    topic = message.topic
    print(message.value[0])

    # ## CALCULATE ELO

    ## CONSUMER 1

    if topic == "problem":
        runtime_bonus = 10000 / int(data[7])  # float
        diff = data[3]  # float
        status = data[5]  # float
        user = data[0]

    ## CONSUMER 2

    if topic == "competition":
        runtime_bonus = 10000 / int(data[8])
        diff = data[4]
        status = data[6]
        user = data[1]

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
        user = data[0]
        upvotes = int(data[3])

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
