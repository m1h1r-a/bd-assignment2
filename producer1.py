#!/usr/bin/env python3
import json
import sys

from kafka import KafkaProducer

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode("ascii"))
problem = sys.argv[1]
competition = sys.argv[2]
solution = sys.argv[3]
# count = 0


for line in sys.stdin:
    line = line.strip().split()
    if line[0] == "problem":
        producer.send(problem, line[1:])
        # count += 1
    elif line[0] == "competition":
        producer.send(competition, line[1:])
        # count += 1
    elif line[0] == "EOF":
        producer.send(problem, "end")
        producer.send(competition, "end")
        producer.send(solution, "end")
        break

    producer.send(solution, line)

# print(f"count in producer: {count}")
producer.flush()
