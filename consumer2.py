#!/usr/bin/env python3
import json
import math
import sys

from kafka import KafkaConsumer

# Sample Input
# ['c_0003', 'u_018', 'cp_00008', 'Heaps', 'Easy', 'cs_00005', 'Passed', 'Go', '411', '7']
topic = sys.argv[2]
status_score = 0
diff_score = 0
result = {}

consumer = KafkaConsumer(
    topic, value_deserializer=lambda m: json.loads(m.decode("ascii"))
)
for message in consumer:
    if "end" in message.value:
        break

    data = message.value
    time_penalty = 0.25 * float(data[9])
    runtime_bonus = 10000 / int(data[8])
    bonus = float(max(1, (1 + runtime_bonus - time_penalty)))
    diff = data[4]
    if diff == "Hard":
        diff_score = 3
    elif diff == "Medium":
        diff_score = 2
    elif diff == "Easy":
        diff_score = 1

    status = data[6]
    if status == "Passed":
        status_score = 100
    elif status == "TLE":
        status_score = 20
    else:
        status_score = 0

    submissionPoints = float(status_score * diff_score * bonus)
    submissionPoints = int(math.floor(submissionPoints))

    cid = data[0]
    user = data[1]

    if cid not in result:
        result[cid] = {}
    if user not in result[cid]:
        result[cid][user] = 0
    result[cid][user] += submissionPoints
    result[cid] = dict(sorted(result[cid].items()))

    # print(result[cid])
    # print(type(result[cid]))

    # print(result[cid][user])

# comp = result.keys()
# print(comp)

result = dict(sorted(result.items()))

print(json.dumps(result, indent=4))
consumer.close()
