import os
import time
import mysql.connector
import googlemaps
import folium
import json
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
    cursor.execute("SELECT id, zone_name, zone_data FROM tbl_delivery_zones WHERE is_active = 1")
    zones = []
    for row in cursor.fetchall():
        try:
            zone_data = json.loads(row['zone_data'])
            if zone_data.get('type') == 'polygon':
                coords = [(c[1], c[0]) for c in zone_data['coordinates']] # Correcting lat/lng order
                zones.append({'id': row['id'], 'title': row['zone_name'], 'polygon': Polygon(coords)})
        except Exception as e:
            print(f"Error parsing zone {row['zone_name']}: {e}")
            continue
    cursor.close()
    conn.close()
    return zones

def find_zone(lat, lng, zones):
    point = Point(lat, lng)
    for z in zones:
        if z['polygon'].contains(point):
            return z['id'], z['title']
    return None, None

# -------------------- Rider Helpers --------------------

def get_available_riders(order_lat, order_lng, max_distance_km=10):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    # This query correctly joins tbl_rider with tbl_rider_availability
    cursor.execute("""
        SELECT 
            tr.id, 
            tr.title, 
            tra.current_lat AS lats, 
            tra.current_lng AS longs
        FROM 
            tbl_rider AS tr
        JOIN 
            tbl_rider_availability AS tra ON tr.id = tra.rider_id
        WHERE 
            tra.is_online = 1 
            AND tra.is_available = 1 
            AND tra.active_order_count < tra.max_capacity
    """)
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

    nearby_riders.sort(key=lambda x: float(x["distance"].replace(" km", "").replace(",", "")))
    return nearby_riders

def log_assignment(order_id, rider_id, status='pending'):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO tbl_rider_assignments (order_id, rider_id, status, assignment_time)
            VALUES (%s, %s, %s, NOW())
        """, (order_id, rider_id, status))
        conn.commit()
    except Exception as e:
        print(f"Error logging assignment: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_rider_notification(rider_id, order_id, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        msg = f"New Order Available. Please accept Order #{order_id} from table {table_name}"
        cursor.execute("INSERT INTO tbl_rnoti (rid, msg, date) VALUES (%s, %s, NOW())", (rider_id, msg))
        conn.commit()
    except Exception as e:
        print(f"Error inserting rider notification: {e}")
    finally:
        cursor.close()
        conn.close()

def simulate_rider_acceptance(order_id, rider_id):
    time.sleep(1) # Simulate network delay and rider decision
    conn = get_db_connection()
    cursor = conn.cursor()
    # Update the assignment status to 'accepted' and log response time
    cursor.execute("""
        UPDATE tbl_rider_assignments
        SET status = 'accepted', response_time = NOW()
        WHERE order_id = %s AND rider_id = %s
    """, (order_id, rider_id))
    conn.commit()
    cursor.close()
    conn.close()
    return rider_id

def assign_order(order_id, rider_id, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Update main order table
        cursor.execute(f"UPDATE {table_name} SET rid = %s, order_status = 1 WHERE id = %s", (rider_id, order_id))
        
        # Update rider availability status
        cursor.execute("UPDATE tbl_rider_availability SET active_order_count = active_order_count + 1 WHERE rider_id = %s", (rider_id,))
        
        # Log the delivery
        cursor.execute("""
            INSERT INTO tbl_delivery (store_id, title, de_digit, status, rider_id, rider_response, response_time)
            VALUES (%s, 'Home delivery', 0, 1, %s, 'accepted', NOW())
        """, (1, rider_id)) # Assuming store_id is 1 for now, this should be dynamic
        
        conn.commit()
    except Exception as e:
        print(f"Error assigning order in DB: {e}")
        conn.rollback()
    finally:
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
            nearest = nearby_riders[0]
            
            # Log assignment to tbl_rider_assignments
            log_assignment(order['id'], nearest['id'], 'pending')
            
            # Notify the nearest rider
            insert_rider_notification(nearest['id'], order['id'], table_name)
            
            # Simulate acceptance
            accepted_id = simulate_rider_acceptance(order['id'], nearest['id'])
            
            # Finalize assignment
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

