import json

f = open("splitterAndHassherIGuess/decode.json", "r")
data = json.load(f)
print(data)
f.close()