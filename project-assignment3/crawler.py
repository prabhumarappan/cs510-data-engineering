import requests as r
from bs4 import BeautifulSoup
import re

stop_event_api = "http://www.psudataeng.com:8000/getStopEvents/"

def get_trip_number(t):
    rp = "\d+"
    m = re.search(rp, t)
    if m:
        return int(t[m.start():m.end()])
    return None

def get_stop_event_data(soup):
    headings = soup.find_all('h3')
    tables = soup.find_all('table')

    min_iterator = min(len(headings), len(tables))

    data = []

    for i in range(min_iterator):
        heading = headings[i].text
        table = tables[i]

        trip_id = get_trip_number(heading)
        rows = table.find_all('tr')
        header = [x.text for x in rows[0].find_all('th')]
        rows = rows[1:]

        for r in rows:
            row = [x.text for x in r.find_all('td')]
            single_stop_point = {}
            for hitr in range(len(header)):
                key = header[hitr]
                value = row[hitr]
                if value == '':
                    value = None
                else:
                    if key in ['vehicle_number', 'leave_time', 'train', 'route_number', 'direction', 'stop_time', 'arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load', 'maximum_speed', 'data_source', 'schedule_status']:
                        value = int(value)
                    elif key == 'service_key':
                        continue
                    else:
                        value = float(value)
                single_stop_point[key] = value
            single_stop_point['trip_id'] = trip_id
            data.append(single_stop_point)
        
    return data

def crawl_stop_event_page():
    html_text = r.get(stop_event_api).text

    soup = BeautifulSoup(html_text, 'html.parser')

    data = get_stop_event_data(soup)
    
    return data
    
if __name__ == "__main__":
    data = crawl_stop_event_page()
    print(len(data))
    fo = open('sample.json','w')
    import json
    json.dump(data, fo)
    fo.close()