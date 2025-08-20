import os
import time
import mysql.connector
import googlemaps
import folium
from dotenv import load_dotenv
from shapely.geometry import Point, Polygon

# -------------------- Load Environment --------------------
load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_API_KEY"))

# -------------------- Database Connection --------------------
def get_db_connection():
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        connect_timeout=60,
        connection_timeout=60
    )
    conn.ping(reconnect=True, attempts=3, delay=5)
    return conn

# -------------------- Google Maps Helpers --------------------
def geocode_address(address):
    try:
        result = gmaps.geocode(address)
        if result:
            loc = result[0]['geometry']['location']
            return loc['lat'], loc['lng']
    except Exception as e:
        print("Geocode error:", e)
    return None, None

def get_distance_and_time(origin, destination):
    try:
        res = gmaps.distance_matrix([origin], [destination], mode="driving")
        if res['rows'][0]['elements'][0]['status'] == 'OK':
            d = res['rows'][0]['elements'][0]
            return d['distance']['text'], d['duration']['text']
    except Exception as e:
        print("Distance error:", e)
    return None, None

def get_direction_link(origin_lat, origin_lng, dest_lat, dest_lng):
    return f"https://www.google.com/maps/dir/?api=1&origin={origin_lat},{origin_lng}&destination={dest_lat},{dest_lng}&travelmode=driving"

# -------------------- Zones --------------------
def load_zones():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, title, coordinates FROM zones WHERE status = 1")
    zones = []
    for row in cursor.fetchall():
        try:
            coords = [
                tuple(map(float, pt.replace("(", "").replace(")", "").strip().split(',')))
                for pt in row['coordinates'].split(',') if pt.count(',') == 1
            ]
            zones.append({'id': row['id'], 'title': row['title'], 'polygon': Polygon(coords)})
        except:
            continue
    cursor.close()
    conn.close()
    return zones

# -------------------- Rider Helpers --------------------
def get_available_riders(order_lat, order_lng, max_distance_km=10):
    """
    Returns sorted list of available riders within max_distance_km from the order location.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, title, lats, longs FROM tbl_rider WHERE rstatus = 0")
    riders = cursor.fetchall()
    cursor.close()
    conn.close()

    nearby_riders = []
    for r in riders:
        try:
            dist_text, eta_text = get_distance_and_time(
                (float(r['lats']), float(r['longs'])),
                (order_lat, order_lng)
            )
            if dist_text:
                dist_val = float(dist_text.replace(" km", "").replace(",", ""))
                if dist_val <= max_distance_km:
                    nearby_riders.append({
                        "id": r["id"],
                        "title": r["title"],
                        "distance": dist_text,
                        "eta": eta_text,
                        "route_link": get_direction_link(r['lats'], r['longs'], order_lat, order_lng)
                    })
        except Exception as e:
            print(f"Error with rider {r['title']}: {e}")

    # Sort by distance
    nearby_riders.sort(key=lambda x: float(x["distance"].replace(" km", "").replace(",", "")))
    return nearby_riders

def insert_rider_notifications(order_id, rider_ids, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    for rid in rider_ids:
        cursor.execute("""
            INSERT INTO tbl_notification (uid, datetime, title, description, related_id, type)
            VALUES (%s, NOW(), %s, %s, %s, %s)
        """, (rid, "New Order Available", f"Please accept Order #{order_id}", order_id, table_name))
    conn.commit()
    cursor.close()
    conn.close()

def simulate_rider_acceptance(order_id, rider_ids):
    time.sleep(1)
    accepted = rider_ids[0]
    conn = get_db_connection()
    cursor = conn.cursor()
    for rid in rider_ids[1:]:
        cursor.execute("DELETE FROM tbl_notification WHERE uid = %s AND related_id = %s", (rid, order_id))
    conn.commit()
    cursor.close()
    conn.close()
    return accepted

def assign_order(order_id, rider_id, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {table_name} SET rid = %s, order_status = 1 WHERE id = %s", (rider_id, order_id))
    cursor.execute("UPDATE tbl_rider SET rstatus = 1 WHERE id = %s", (rider_id,))
    conn.commit()
    cursor.close()
    conn.close()

def notify_user(uid, order_id, name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tbl_notification (uid, datetime, title, description)
        VALUES (%s, NOW(), %s, %s)
    """, (uid, "Order Assigned!", f"{name}, your Order #{order_id} has been assigned."))
    conn.commit()
    cursor.close()
    conn.close()

def find_zone(lat, lng, zones):
    point = Point(lat, lng)
    for z in zones:
        if z['polygon'].contains(point):
            return z['id'], z['title']
    return None, None

# -------------------- Main Logic --------------------
def process_order_table(table_name):
    zones = load_zones()
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {table_name} WHERE order_status = 0")
    orders = cursor.fetchall()
    cursor.close()
    conn.close()

    order_map = folium.Map(location=[18.6, 73.75], zoom_start=12)
    assigned_orders = []
    assigned, not_assigned = 0, 0

    for order in orders:
        full_address = f"{order['address']}, {order['landmark']}"
        lat, lng = geocode_address(full_address)
        if not lat or not lng:
            print(f"Skipping Order #{order['id']} (Invalid address)")
            continue

        zone_id, zone_title = find_zone(lat, lng, zones)

        nearby_riders = get_available_riders(lat, lng)
        if nearby_riders:
            nearest = nearby_riders[0]  #  under radius rider
            insert_rider_notifications(order['id'], [nearest['id']], table_name)
            accepted_id = simulate_rider_acceptance(order['id'], [nearest['id']])
            assign_order(order['id'], accepted_id, table_name)
            notify_user(order['uid'], order['id'], order['name'])

            folium.Marker(
                location=[lat, lng],
                popup=f"Order #{order['id']} → {nearest['title']}\n{nearest['distance']}, {nearest['eta']}",
                icon=folium.Icon(color="green")
            ).add_to(order_map)

            order['assigned_rider_name'] = nearest['title']
            order['zone'] = zone_title
            order['distance'] = nearest['distance']
            order['eta'] = nearest['eta']
            order['route_link'] = nearest['route_link']
            assigned_orders.append(order)
            assigned += 1
        else:
            not_assigned += 1

    order_map.save("order_assignment_map.html")
    return assigned, not_assigned, assigned_orders

