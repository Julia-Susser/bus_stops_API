import sqlite3
import urllib.error
import ssl
import json
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen

conn = sqlite3.connect('location.sqlite')
cur = conn.cursor()
def delete():
    d = input("Delete?")
    if d == "yes":
        cur.execute("DROP TABLE IF EXISTS locats")
    conn.commit()

def how_many():
    num = input("How many different locations do you want to read? ")
    if len(num) < 1:
        print("not stopping till the end!")
        return None
    try:
        return int(num)
    except ValueError:
        print("not valid input. Getting 100 locations.")
        return 100


delete()
cur.executescript(
"""
CREATE TABLE IF NOT EXISTS locats(
    long_lat TEXT UNIQUE,
    url TEXT,
    used INTEGER
);
CREATE TABLE IF NOT EXISTS bad_apples(
    long_lat TEXT UNIQUE,
    url TEXT
);
UPDATE locats SET used = NULL
"""
)
conn.commit()



def put_in( lat2, long2):
    long_lat = long2 + "/" + lat2
    cur.execute("SELECT url FROM locats WHERE long_lat = ? LIMIT 1", (long_lat,))
    row = cur.fetchone()
    if row is not None:
        return 2

    cur.execute("SELECT url FROM bad_apples WHERE long_lat = ? LIMIT 1", (long_lat,))
    row = cur.fetchone()
    if row is not None:
        return 3

    apikey = False
    parts = dict()
    if apikey == False:
        apikey = "d4254f4e-c1f4-4506-9513-857e387e8bb9"
        url = "http://bustime.mta.info/api/where/stops-for-location.json?"

    parts["lat"] = lat2
    parts["lon"] = long2
    parts["latSpan"] = 0.005
    parts["lonSpan"] = 0.005
    parts["key"] = apikey
    url = url + urllib.parse.urlencode(parts)

    raw = urllib.request.urlopen(url)
    file = raw.read().decode()

    js_file = json.loads(file)


    data = js_file["data"]["stops"]

    if len(data) > 1:
        cur.execute("INSERT OR IGNORE INTO locats(long_lat, url) VALUES (?, ?)", (long_lat, url))
        conn.commit()
        print("\n\nLongitude: {}, Latitude: {}".format(long2, lat2))
        print(url)
        return 0
    else:
        cur.execute("INSERT OR IGNORE INTO bad_apples(long_lat, url) VALUES (?, ?)", (long_lat, url))
        conn.commit()
        return 1



lat1 = 0
long1 = 0
stop_all = 0
num = how_many()
count = 0

for x in range(100):
    if count == num:
        print("\n\nFINISHED\n\n")
        break
    if stop_all == 3:
        "No longer finding anything! Stopping all"
        break
    lat1 = 0
    fails = 0
    success = 0
    if long1 == 0:
        long1 = 74000
    else:
        long1 = long1 - 1
    llon1 = str(long1)[:2]
    llon2 = str(long1)[2:5]
    llon = [llon1, llon2]
    longy = ".".join(llon)
    long2 = "-" + str(longy)
    for x in range(1000):
        if count == num:

            break
        if fails == 300:
            stop_all += 1
            print("did not find anything".upper())
            break
        if fails == 10 and success > 0:
            print("\n\n{} successes\n\n".format(success).upper())
            print("Done!\n".format(long2).upper())
            break
        if lat1 == 0:
            lat1 = 41000
        else:
            lat1 = lat1 - 1
        llat1 = str(lat1)[:2]
        llat2 = str(lat1)[2:5]
        llat = [llat1, llat2]
        llatty = ".".join(llat)
        lat2 = str(llatty)
        work = put_in(lat2, long2)
        if work == 3:
            print("bad apple")
        if work == 2:
            print("already_found\n")
        if work == 1:
            fails += 1
            if fails % 10 == 0:
                print("\nfailing, but working!\n".upper())
        if work == 0:
            success += 1
            fails = 0
            count += 1
