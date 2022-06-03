#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []

with open('query.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:len(data)-1]:
        line.strip()
        row = line.split("\t")
        
        # skip the rows where speed is missing
        print(row)
        x = row[0]
        y = row[1]
        count = row[2]
        if count is None or count == "":
            continue 
        try:
            latitude, longitude = map(float, (y, x))
            count = count.replace('\n','')
            count = int(count)*10/598.0
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'count': count
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data.geojson", "w") as f:
    f.write('%s' % collection)