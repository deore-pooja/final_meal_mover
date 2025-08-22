import os
import time
import mysql.connector
import googlemaps
import folium
import json
import random
from dotenv import load_dotenv
from shapely.geometry import Point, Polygon
from datetime import datetime, date

# -------------------- Load Environment --------------------
load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_API_KEY"))

# -------------------- Database Connection --------------------
def get_db_connection():
    """Establishes and returns a MySQL database connection."""
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
    """Geocodes an address to latitude and longitude."""
    try:
        result = gmaps.geocode(address)
        if result:
            loc = result[0]['geometry']['location']
            return loc['lat'], loc['lng']
    except Exception as e:
        print(f"Geocode error for address '{address}': {e}")
    return None, None

def get_distance_and_time(origin, destination):
    """Calculates driving distance and duration between two points."""
    try:
        res = gmaps.distance_matrix([origin], [destination], mode="driving")
        if res['rows'][0]['elements'][0]['status'] == 'OK':
            d = res['rows'][0]['elements'][0]
            return d['distance']['text'], d['duration']['text']
    except Exception as e:
        print(f"Distance error from {origin} to {destination}: {e}")
    return None, None

def get_direction_link(origin_lat, origin_lng, dest_lat, dest_lng):
    """Generates a Google Maps direction link."""
    return f"https://www.google.com/maps/dir/{origin_lat},{origin_lng}/{dest_lat},{dest_lng}/"

# -------------------- Zones & Routes --------------------
def load_zones():
    """Loads active delivery zones from tbl_delivery_zones."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, zone_name, zone_data FROM tbl_delivery_zones WHERE is_active = 1")
    zones = []
    for row in cursor.fetchall():
        try:
            zone_data = json.loads(row['zone_data'])
            if zone_data.get('type') == 'polygon':
                coords = [(c[1], c[0]) for c in zone_data['coordinates']]
                zones.append({'id': row['id'], 'title': row['zone_name'], 'polygon': Polygon(coords)})
        except Exception as e:
            print(f"Error parsing zone {row['zone_name']}: {e}")
            continue
    cursor.close()
    conn.close()
    return zones

def find_zone(lat, lng, zones):
    """Finds the zone a given point is in."""
    point = Point(lat, lng)
    for z in zones:
        if z['polygon'].contains(point):
            return z['id'], z['title']
    return None, None

def load_active_routes():
    """Loads active rider routes from tbl_rider_routes."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM tbl_rider_routes WHERE is_active = 1")
        routes = cursor.fetchall()
        return routes
    except Exception as e:
        print(f"Error loading routes: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def find_riders_on_route(order_location, routes):
    """Finds riders whose routes include the order location."""
    on_route_riders = []
    order_point = Point(order_location)
    for r in routes:
        try:
            route_data = json.loads(r['route_data'])
            if route_data['type'] == 'Polygon':
                route_polygon = Polygon(route_data['coordinates'][0])
                if route_polygon.contains(order_point):
                    on_route_riders.append(r['rider_id'])
        except Exception as e:
            print(f"Error processing route data for rider {r['rider_id']}: {e}")
    return on_route_riders

def get_rejected_riders(order_id):
    """Gets a list of riders who have rejected this specific order."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT rider_id FROM tbl_rider_rejections WHERE order_id = %s", (order_id,))
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching rejected riders for order {order_id}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# -------------------- Rider Helpers --------------------
def get_available_riders(order_lat, order_lng, order_id, max_distance_km=10):
    """
    Finds and sorts available riders, excluding those who have rejected the order.
    Prioritizes riders whose route covers the order's location.
    """
    rejected_riders = get_rejected_riders(order_id)
    on_route_riders = find_riders_on_route((order_lat, order_lng), load_active_routes())
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    rejected_riders_tuple = tuple(rejected_riders) if rejected_riders else (0,)
    
    sql_query = """
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
            AND tr.id NOT IN ({})
        ORDER BY 
            (6371 * acos(
                cos(radians(%s)) * cos(radians(tra.current_lat)) * cos(radians(tra.current_lng) - radians(%s)) + sin(radians(%s)) * sin(radians(tra.current_lat))
            )) ASC;
    """.format(','.join(['%s'] * len(rejected_riders_tuple)))

    try:
        cursor.execute(sql_query, rejected_riders_tuple + (order_lat, order_lng, order_lat))
        riders = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching riders from DB: {e}")
        riders = []
    finally:
        cursor.close()
        conn.close()

    nearby_riders = []
    for r in riders:
        on_route = r['id'] in on_route_riders
        try:
            dist_text, eta_text = get_distance_and_time(
                (float(r['lats']), float(r['longs'])),
                (order_lat, order_lng)
            )
            if dist_text:
                nearby_riders.append({
                    "id": r["id"],
                    "title": r["title"],
                    "distance": dist_text,
                    "eta": eta_text,
                    "route_link": get_direction_link(r['lats'], r['longs'], order_lat, order_lng),
                    "on_route": on_route
                })
        except Exception as e:
            print(f"Error with rider {r['title']}: {e}")

    nearby_riders.sort(key=lambda x: (not x['on_route'], float(x["distance"].replace(" km", "").replace(",", ""))))
    
    return nearby_riders

def log_assignment(order_id, rider_id, table_name):
    """Logs a new pending assignment in tbl_rider_assignments."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO tbl_rider_assignments (order_id, rider_id, status, assignment_time, order_table)
            VALUES (%s, %s, %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE rider_id = %s, status = 'pending', assignment_time = NOW(), response_time = NULL
        """, (order_id, rider_id, 'pending', table_name, rider_id))
        conn.commit()
    except Exception as e:
        print(f"Error logging assignment: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_rider_notification(rider_id, order_id, table_name, product_details=""):
    """Inserts a new notification for the rider in tbl_notification."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        msg = f"New Order Available. Please accept Order #{order_id} from table {table_name}. Items: {product_details}"
        cursor.execute("""
            INSERT INTO tbl_notification (uid, datetime, title, description, related_id, type) 
            VALUES (%s, NOW(), %s, %s, %s, %s)
        """, (rider_id, "New Order Available", msg, order_id, table_name))
        conn.commit()
    except Exception as e:
        print(f"Error inserting rider notification: {e}")
    finally:
        cursor.close()
        conn.close()

def get_subscribe_order_products(order_id):
    """Fetches product details for a subscription order."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT ptitle, pquantity FROM tbl_subscribe_order_product WHERE oid = %s", (order_id,))
        products = cursor.fetchall()
        product_list = [f"{p['pquantity']}x {p['ptitle']}" for p in products]
        return ", ".join(product_list)
    except Exception as e:
        print(f"Error fetching products for subscribe order {order_id}: {e}")
        return "N/A"
    finally:
        cursor.close()
        conn.close()

def simulate_rider_response(order_id, rider_id, table_name):
    """
    Simulates rider response (acceptance or rejection)
    For this example, we'll simulate a 90% acceptance rate.
    """
    time.sleep(1) 
    
    status = 'accepted'
    reason = None
    if random.random() < 0.1:
        status = 'rejected'
        reason = "Rider busy"
        log_rider_rejection(rider_id, order_id, reason)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE tbl_rider_assignments
            SET status = %s, response_time = NOW(), response_reason = %s
            WHERE order_id = %s AND rider_id = %s
        """, (status, reason, order_id, rider_id))
        conn.commit()
    except Exception as e:
        print(f"Error simulating rider response: {e}")
    finally:
        cursor.close()
        conn.close()
    return status

def log_rider_rejection(rider_id, order_id, reason):
    """Logs a rider rejection in tbl_rider_rejections."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO tbl_rider_rejections (rider_id, order_id, rejection_time, reason)
            VALUES (%s, %s, NOW(), %s)
        """, (rider_id, order_id, reason))
        conn.commit()
    except Exception as e:
        print(f"Error logging rejection: {e}")
    finally:
        cursor.close()
        conn.close()

def assign_order(order, rider_id, table_name):
    """
    Finalizes the order assignment in all relevant tables.
    Uses a transaction to ensure atomicity.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        conn.start_transaction()
        
        cursor.execute(f"UPDATE {table_name} SET rid = %s, order_status = 1 WHERE id = %s", (rider_id, order['id']))
        
        cursor.execute("UPDATE tbl_rider_availability SET active_order_count = active_order_count + 1 WHERE rider_id = %s", (rider_id,))
        
        cursor.execute("""
            INSERT INTO tbl_delivery (store_id, title, status, rider_id, rider_response, response_time)
            VALUES (%s, 'Home delivery', 1, %s, 'accepted', NOW())
        """, (order['store_id'], rider_id))
        
        now = datetime.now()
        cursor.execute("""
            INSERT INTO tbl_rider_performance (rider_id, `date`, `hour`, orders_assigned, orders_accepted)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                orders_assigned = orders_assigned + 1,
                orders_accepted = orders_accepted + 1
        """, (rider_id, now.date(), now.hour, 1, 1))

        conn.commit()
    except Exception as e:
        print(f"Error assigning order in DB: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def notify_user(uid, order_id, name):
    """Inserts a notification for the user in tbl_notification."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO tbl_notification (uid, datetime, title, description)
            VALUES (%s, NOW(), %s, %s)
        """, (uid, "Order Assigned!", f"{name}, your Order #{order_id} has been assigned."))
        conn.commit()
    except Exception as e:
        print(f"Error notifying user: {e}")
    finally:
        cursor.close()
        conn.close()

# -------------------- Main Logic --------------------
def process_order_table(table_name):
    """
    Processes all unassigned orders in a given table.
    """
    zones = load_zones()
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    if table_name == "tbl_subscribe_order":
        today_date_str = date.today().strftime('%Y-%m-%d')
        cursor.execute(f"SELECT * FROM {table_name} WHERE order_status = 0 AND odate = %s", (today_date_str,))
    else:
        cursor.execute(f"SELECT * FROM {table_name} WHERE order_status = 0 AND o_type = 'Delivery'")
    
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
            print(f"Skipping Order #{order.get('id', 'N/A')} from {table_name} (Invalid address)")
            continue

        zone_id, zone_title = find_zone(lat, lng, zones)

        nearby_riders = get_available_riders(lat, lng, order['id'])
        
        final_rider = None
        for rider in nearby_riders:
            log_assignment(order['id'], rider['id'], table_name)
            
            product_details = ""
            if table_name == "tbl_subscribe_order":
                product_details = get_subscribe_order_products(order['id'])

            insert_rider_notification(rider['id'], order['id'], table_name, product_details)
            
            response_status = simulate_rider_response(order['id'], rider['id'], table_name)
            
            if response_status == 'accepted':
                final_rider = rider
                assign_order(order, final_rider['id'], table_name)
                break 

        if final_rider:
            user_name = order.get('name', 'User') 
            notify_user(order['uid'], order['id'], user_name)

            folium.Marker(
                location=[lat, lng],
                popup=f"Order #{order['id']} → {final_rider['title']}\n{final_rider['distance']}, {final_rider['eta']}",
                icon=folium.Icon(color="green")
            ).add_to(order_map)

            order['assigned_rider_name'] = final_rider['title']
            order['zone'] = zone_title
            order['distance'] = final_rider['distance']
            order['eta'] = final_rider['eta']
            order['route_link'] = final_rider['route_link']
            assigned_orders.append(order)
            assigned += 1
        else:
            print(f"No rider accepted order #{order['id']} from {table_name}.")
            not_assigned += 1

    order_map.save("order_assignment_map.html")
    return assigned, not_assigned, assigned_orders

