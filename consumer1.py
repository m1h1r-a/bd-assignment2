#!/usr/bin/env python3
import json
import sys

from kafka import KafkaConsumer

# Sample INput:
# ['u_019', 'p_00006', 'DP', 'Hard', 's_00005', 'Passed', 'C', '1408']

topic = sys.argv[1]
problem = {}
languages = {}
best_lang = []
diff_cat = []
maxLang = 0
passed = {}
minDiff = 0


consumer = KafkaConsumer(
    topic, value_deserializer=lambda m: json.loads(m.decode("ascii"))
)
for message in consumer:
    if "end" in message.value:
        break

    data = message.value
    lang = data[6]
    if lang not in languages:
        languages[lang] = 0
    languages[lang] += 1

    category = data[2]
    if category not in passed:
        passed[category] = [0, 0, 0]
    passed[category][1] += 1
    if data[5] == "Passed":
        passed[category][0] += 1

    # print(data)


# processing most common language:
for language in languages:
    if languages[language] > maxLang:
        best_lang.clear()
        best_lang.append(language)
        maxLang = languages[language]
        # print(f"greater : {best_lang}")
    elif languages[language] == maxLang:
        best_lang.append(language)
        # print(f"equal : {best_lang}")
best_lang = sorted(best_lang)

# print(f"Total submissions: {sum(languages.values())}")
totalSubmissions = sum(languages.values())

iter = 1
for val in passed:
    passed[val][2] = passed[val][0] / passed[val][1]

    if iter == 1:
        minDiff = passed[val][2]
        diff_cat.append(val)
        iter += 1
    elif passed[val][2] < minDiff:
        diff_cat.clear()
        diff_cat.append(val)
        minDiff = passed[val][2]
    elif passed[val][2] == minDiff:
        diff_cat.append(val)
diff_cat = sorted(diff_cat)

result = {}
result["most_used_language"] = best_lang
result["most_difficult_category"] = diff_cat

print(json.dumps(result, indent=4))

# problem=dict(sorted(problem.items()))
# print(json.dumps(problem, indent=4))
consumer.close()
