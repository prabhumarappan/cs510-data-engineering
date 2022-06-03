#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []

with open('query.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    coords = []
    for line in data[1:len(data)-1]:
        line.strip()
        row = line.split("\t")
        
        # skip the rows where speed is missing
        print(row)
        x = row[0]
        y = row[1]
        try:
            latitude, longitude = map(float, (y, x))
            coords.append([longitude, latitude])
        except ValueError:
            continue
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coords
        },
    })


with open("data.geojson", "w") as f:
    json.dump(features[0], f)