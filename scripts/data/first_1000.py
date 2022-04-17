import json

inpf = open("04-04-2022.json", 'r')
data = json.load(inpf)
inpf.close()

outf = open("1000_records.json", 'w')
json.dump(data[:50000], outf)
outf.close()
