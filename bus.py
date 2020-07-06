import sqlite3
import urllib.error
import ssl
import json
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen


def how_many():
    num = input("How many different locations do you want to read? ")
    try:
        if int(num) < 1:
            print("not valid. we will extract 5 rows")
            num= 5
        return int(num)
    except ValueError:
        print("not valid input. we will extract 5 rows")
        return 5




conn = sqlite3.connect('stops.sqlite')
cur = conn.cursor()
hey = sqlite3.connect('location.sqlite')
js = hey.cursor()

def get_data():

    js.execute("""SELECT long_lat, url FROM locats
WHERE used = NULL IN (SELECT long_lat FROM locats ORDER BY RANDOM() LIMIT 1)""")
    row = js.fetchone()
    if row == None:
        print("its over. no more locations to extract".upper())
        data = "its over"
        long = None
        lat = None
        url = None
        long_lat = None
    else:
        long_lat = row[0]
        url = row[1]
        ls = long_lat.split("/")

        long = ls[0]
        lat = ls[1]
        raw = urllib.request.urlopen(url)
        file = raw.read().decode()

        js_file = json.loads(file)


        data = js_file["data"]["stops"]


    return data, long, lat, url, long_lat

def new_tables():
    cur.executescript(
    """

    CREATE TABLE IF NOT EXISTS stops(
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name TEXT UNIQUE,
        bus_stop_id TEXT UNIQUE,
        exact_long INTEGER,
        exact_lat INTEGER
    );
    CREATE TABLE IF NOT EXISTS near_stop(
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name_id INTEGER,
        long_lat_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS long_lat(
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        long INTEGER,
        lat INTEGER
    )

    """
    )
    conn.commit()


def delete():
    delete = input("Do you want to start fresh? ")
    if delete == "yes":
        print("starting fresh".upper())
        cur.executescript(
        """
        DROP TABLE IF EXISTS long_lat;
        DROP TABLE IF EXISTS near_stop;
        DROP TABLE IF EXISTS stops
        """
        )
        js.execute("UPDATE locats SET used = NULL")

    new_tables()


delete()
num = how_many()
fails = 0
fails_list = []
count = 0

while True:

    if count == num:
        print("Finished!".upper())
        break

    if fails == 3:
        print(fails_list)
        break
    data, long, lat, url, long_lat = get_data()
    if data == "its over":
        break
    js.execute('UPDATE locats SET used=-1 WHERE long_lat=?', (long_lat,) )
    hey.commit()

    cur.execute("SELECT id FROM long_lat WHERE long = ? AND lat = ? LIMIT 1", (long, lat))
    row = cur.fetchone()
    if row is not None:
        continue



    if len(data) < 1:
        fails += 1
        print("There are no stops at the location requested! UH OH!")
        ls2 = [long, lat]
        long_lat2 = "/".join(ls2)
        fails_list.append(long_lat2)
        continue


    print("ROW {}".format(count + 1))
    print("{} stops are at long: {} and lat: {}".format(len(data), long, lat))

    cur.execute('''INSERT OR IGNORE INTO long_lat (long, lat)
        VALUES ( ?, ? )''', ( long, lat ))
    cur.execute("""SELECT id FROM long_lat WHERE long = ? AND lat = ?""", (long, lat))

    try:
        near_id = cur.fetchone()[0]
    except:
        print("Could not get id!")

    for x in data:
        name = x["name"].lower()
        bus_id = x["id"]
        exact_long = x["lon"]
        exact_lat = x["lat"]
        print("\n\nName: {}\nStop ID: {}\nLongitude: {}\nLatitude: {}".format(name, bus_id, exact_long, exact_lat) + "\n\n")
        name = name.upper()
        cur.execute("""INSERT OR IGNORE INTO stops(name, exact_long, exact_lat, bus_stop_id) VALUES(?, ?, ?, ?)""", (name, exact_long, exact_lat, bus_id))
        cur.execute(
        """SELECT id FROM stops WHERE name = ?""", (name,)
        )
        try:
            name_id = cur.fetchone()[0]
        except:
            print("Could not get id!")
            continue

        cur.execute(
        """SELECT id FROM near_stop WHERE name_id = ? AND long_lat_id = ? LIMIT 1""", (name_id, near_id)
        )
        row = cur.fetchone()
        if row is not None:
            continue
        cur.execute("INSERT INTO near_stop(name_id, long_lat_id) VALUES(?,?)", (name_id, near_id))
        conn.commit()≥




    count += 1





print("\n\n\n")
hey.commit()
conn.commit()
hey.close()
conn.close()
