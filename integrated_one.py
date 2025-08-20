import os
import math
import mysql.connector
import googlemaps
import folium
from flask import Flask, jsonify
from dotenv import load_dotenv
from shapely.geometry import Point, Polygon

# -------------------- Environment & Setup --------------------
load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_API_KEY"))
app = Flask(__name__)

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

# -------------------- Helpers --------------------
def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb/2)**2
    return 2 * r * math.asin(math.sqrt(a))

# -------------------- Geocoding & Distance --------------------
def geocode_address(address, order_id=None):
    """Geocode an address, fallback to Pune if failed (keeps order inside service area checks)."""
    if not address or not isinstance(address, str) or address.strip() == "":
        print(f" Order #{order_id}: Empty/invalid address")
        return None, None

    clean_address = " ".join(address.strip().split())
    try:
        result = gmaps.geocode(clean_address)
        if result and "geometry" in result[0]:
            loc = result[0]["geometry"]["location"]
            return float(loc.get("lat")), float(loc.get("lng"))
        else:
            print(f" Order #{order_id}: Geocode failed → '{clean_address}'")
    except Exception as e:
        print(f" Geocode API error for Order #{order_id}: {e}")

    # Fallback: Pune city center
    fallback_lat, fallback_lng = 18.5204, 73.8567
    print(f" Order #{order_id}: Using fallback {fallback_lat},{fallback_lng}")
    return fallback_lat, fallback_lng

def driving_distance(origin, destination):
    """
    Returns (km_float, distance_text, duration_text).
    If Google API fails, returns (None, None, None).
    """
    try:
        res = gmaps.distance_matrix([origin], [destination], mode="driving")
        el = res['rows'][0]['elements'][0]
        if el['status'] != 'OK':
            return None, None, None
        dist_text = el['distance']['text']            # e.g., "5.4 km"
        dur_text  = el['duration']['text']            # e.g., "14 mins"
        km = float(dist_text.replace("km", "").replace(",", "").strip())
        return km, dist_text, dur_text
    except Exception as e:
        print(" distance_matrix error:", e)
        return None, None, None

def get_direction_link(origin_lat, origin_lng, dest_lat, dest_lng):
    return f"https://www.google.com/maps/dir/?api=1&origin={origin_lat},{origin_lng}&destination={dest_lat},{dest_lng}&travelmode=driving"

# -------------------- Zone Logic --------------------
def load_zones():
    """Load all active zones and convert into shapely polygons (lng, lat for shapely)."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, title, coordinates FROM zones WHERE status = 1")
    zones = []
    for row in cursor.fetchall():
        try:
            coords = []
            # Expecting coordinates like "(lat,lng);(lat,lng);..."
            for pt in row['coordinates'].split(";"):
                latlng = pt.strip().replace("(", "").replace(")", "").split(",")
                if len(latlng) == 2:
                    lat, lng = map(float, latlng)
                    coords.append((lng, lat))  # shapely: (x=lng, y=lat)
            if coords:
                zones.append({
                    "id": row["id"],
                    "title": row["title"],
                    "polygon": Polygon(coords)
                })
        except Exception as e:
            print(f" Error parsing zone {row['id']}: {e}")
    cursor.close()
    conn.close()
    return zones

def find_zone(lat, lng, zones):
    """Return (zone_id, zone_title) that contains the point, else (None, None)."""
    pt = Point(lng, lat)
    for z in zones:
        if z["polygon"].contains(pt):
            return z["id"], z["title"]
    return None, None

def get_active_delivery_zone(zone_id):
    """Delivery zone meta (center+radius+time window)."""
    if not zone_id:
        return None
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, zone_name, center_lat, center_lng, radius_km, delivery_time_min, delivery_time_max
        FROM tbl_delivery_zones
        WHERE id = %s AND is_active = 1
    """, (zone_id,))
    zone = cursor.fetchone()
    cursor.close()
    conn.close()
    return zone

def is_within_zone(lat, lng, zone_meta):
    """Check if (lat,lng) is inside delivery zone radius."""
    if not zone_meta:
        return False
    d_km = haversine_km(lat, lng, zone_meta["center_lat"], zone_meta["center_lng"])
    return d_km <= float(zone_meta["radius_km"] or 0)

# -------------------- Rider Logic --------------------
def get_available_riders(zone_id=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT r.id, r.title, ra.current_lat, ra.current_lng, ra.is_available,
               ra.active_order_count, ra.max_capacity,
               COALESCE(rp.acceptance_rate, 0) AS acceptance_rate,
               COALESCE(rp.avg_delivery_time, 30) AS avg_delivery_time
        FROM tbl_rider r
        JOIN tbl_rider_availability ra ON r.id = ra.rider_id
        LEFT JOIN tbl_rider_performance rp ON r.id = rp.rider_id
        WHERE r.status = 1
          AND ra.is_available = 1
          AND ra.active_order_count < ra.max_capacity
    """)
    riders = cursor.fetchall()
    cursor.close()
    conn.close()

    if zone_id:
        allowed = get_riders_by_route(zone_id)
        riders = [r for r in riders if r["id"] in allowed]

    return riders

def get_riders_by_route(zone_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT DISTINCT rider_id FROM tbl_rider_routes WHERE zone_id = %s", (zone_id,))
    ids = [r["rider_id"] for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return ids

# -------------------- Order Preparation --------------------
def get_preparation_time_summary(item_string):
    if not item_string:
        return 0, []
    items = [i.strip().lower() for i in item_string.split(",") if i.strip()]
    total, details = 0, []
    for item in items:
        prep_time = 10
        total += prep_time
        details.append(f"{item.title()} ({prep_time} min)")
    return total, details

# -------------------- Assignment & Notifications --------------------
def assign_order(order_id, rider_id, table_name, score, zone_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {table_name} SET rid=%s, order_status=1 WHERE id=%s", (rider_id, order_id))
    cursor.execute("UPDATE tbl_rider SET rstatus=1 WHERE id=%s", (rider_id,))
    cursor.execute("UPDATE tbl_rider_availability SET active_order_count=active_order_count+1 WHERE rider_id=%s", (rider_id,))
    cursor.execute("""
        INSERT INTO tbl_rider_assignment (order_id, rider_id, assigned_at, score)
        VALUES (%s, %s, NOW(), %s)
    """, (order_id, rider_id, score))
    cursor.execute("""
        INSERT INTO tbl_delivery (order_id, rider_id, rider_response, status, assigned_at, zone_id)
        VALUES (%s, %s, 'pending', 'assigned', NOW(), %s)
    """, (order_id, rider_id, zone_id))
    conn.commit()
    cursor.close()
    conn.close()

def insert_rider_notifications(order_id, rider_ids, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    for rid in rider_ids:
        cursor.execute("""
            INSERT INTO tbl_notification (uid, datetime, title, description, related_id, type)
            VALUES (%s, NOW(), 'New Order Available', %s, %s, %s)
        """, (rid, f"Please accept Order #{order_id}", order_id, table_name))
        cursor.execute("""
            INSERT INTO tbl_rnoti (rid, msg, date)
            VALUES (%s, %s, NOW())
        """, (rid, f"Order #{order_id} is available"))
    conn.commit()
    cursor.close()
    conn.close()

def notify_user(uid, order_id, name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tbl_notification (uid, datetime, title, description)
        VALUES (%s, NOW(), 'Order Assigned!', %s)
    """, (uid, f"{name}, your Order #{order_id} has been assigned."))
    conn.commit()
    cursor.close()
    conn.close()

def log_rider_rejection(order_id, rider_id, reason):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tbl_rider_rejections (order_id, rider_id, rejection_time, reason, created_at)
        VALUES (%s, %s, NOW(), %s, NOW())
    """, (order_id, rider_id, reason))
    conn.commit()
    cursor.close()
    conn.close()

# -------------------- Core Assignment Logic (nearest rider only) --------------------
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
        full_address = f"{order.get('address','')} {order.get('landmark','')} India"
        lat, lng = geocode_address(full_address, order_id=order['id'])
        if not lat or not lng:
            print(f" Skipping Order #{order['id']} (no valid coordinates)")
            not_assigned += 1
            continue

        zone_id, zone_title = find_zone(lat, lng, zones)
        zone_meta = get_active_delivery_zone(zone_id)
        if not zone_meta or not is_within_zone(lat, lng, zone_meta):
            print(f" Order #{order['id']} outside service zone. Skipping.")
            not_assigned += 1
            continue

        riders = get_available_riders(zone_id)
        if not riders:
            # Fallback: search all available riders city-wide
            riders = get_available_riders(None)

        if not riders:
            print(f" No available riders in system for Order #{order['id']}")
            not_assigned += 1
            continue

        # (Optional) prep time calc — kept for completeness
        if table_name == "tbl_normal_order":
            total_time, _ = get_preparation_time_summary(order.get('items'))
        else:
            c2_conn = get_db_connection()
            c2 = c2_conn.cursor(dictionary=True)
            c2.execute("SELECT ptitle FROM tbl_subscribe_order_product WHERE oid=%s", (order['id'],))
            products = c2.fetchall()
            c2.close()
            c2_conn.close()
            names = [p['ptitle'] for p in products]
            total_time, _ = get_preparation_time_summary(",".join(names))

        # ---------- NEAREST RIDER SELECTION (no ETA constraint) ----------
        best = {
            "rider": None,
            "km": float("inf"),
            "dist_text": None,
            "eta_text": None,
            "link": None
        }

        for r in riders:
            r_lat, r_lng = float(r['current_lat']), float(r['current_lng'])

            km, dist_text, eta_text = driving_distance((r_lat, r_lng), (lat, lng))
            if km is None:
                # fall back to straight-line
                km = haversine_km(r_lat, r_lng, lat, lng)
                dist_text = f"{km:.2f} km"
                eta_text = "n/a"

            if km < best["km"]:
                best.update({
                    "rider": r,
                    "km": km,
                    "dist_text": dist_text,
                    "eta_text": eta_text,
                    "link": get_direction_link(r_lat, r_lng, lat, lng)
                })

        if best["rider"]:
            assign_order(order['id'], best["rider"]['id'], table_name, score=1.0/(best["km"]+1e-6), zone_id=zone_id)
            insert_rider_notifications(order['id'], [best["rider"]['id']], table_name)
            notify_user(order.get('uid', 0), order['id'], order.get('name', 'User'))

            folium.Marker(
                location=[lat, lng],
                popup=f"Order #{order['id']} → {best['rider']['title']} ({best['dist_text']}, {best['eta_text']})",
                icon=folium.Icon(color="green")
            ).add_to(order_map)

            order.update({
                "assigned_rider_name": best["rider"]['title'],
                "zone": zone_title,
                "distance": best["dist_text"],
                "eta": best["eta_text"],
                "route_link": best["link"]
            })
            assigned_orders.append(order)
            assigned += 1
        else:
            print(f" No suitable rider for Order #{order['id']}")
            not_assigned += 1

    order_map.save("order_assignment_map.html")
    return assigned, not_assigned, assigned_orders

# -------------------- Flask Routes --------------------
@app.route('/')
def home():
    return jsonify({"message": "API is working!"})

@app.route('/assign_orders', methods=['GET'])
def assign_orders():
    try:
        a1, n1, list1 = process_order_table("tbl_normal_order")
        a2, n2, list2 = process_order_table("tbl_subscribe_order")

        details = []
        for o in list1 + list2:
            details.append({
                "order_id": o["id"],
                "user_name": o.get("name", "Unknown"),
                "zone": o.get("zone"),
                "assigned_rider": o.get("assigned_rider_name"),
                "distance": o.get("distance"),
                "eta": o.get("eta"),
                "google_maps_link": o.get("route_link")
            })

        return jsonify({
            "assigned": a1 + a2,
            "not_assigned": n1 + n2,
            "message": "Order assignment completed. Map saved as order_assignment_map.html",
            "details": details
        })
    except Exception as e:
        print("Error in /assign_orders:", e)
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)


# import os
# import time
# import mysql.connector
# import googlemaps
# import folium
# from flask import Flask, jsonify
# from dotenv import load_dotenv
# from shapely.geometry import Point, Polygon

# # Load environment variables
# load_dotenv()
# gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_API_KEY"))
# app = Flask(__name__)

# # -------------------- Utility Functions --------------------

# def get_db_connection():
#     conn = mysql.connector.connect(
#         host=os.getenv("DB_HOST"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#         database=os.getenv("DB_NAME"),
#         connect_timeout=60,
#         connection_timeout=60
#     )
#     conn.ping(reconnect=True, attempts=3, delay=5)
#     return conn

# def geocode_address(address):
#     try:
#         result = gmaps.geocode(address)
#         if result:
#             loc = result[0]['geometry']['location']
#             return loc['lat'], loc['lng']
#     except:
#         pass
#     return None, None

# def get_distance_and_time(origin, destination):
#     try:
#         res = gmaps.distance_matrix([origin], [destination], mode="driving")
#         if res['rows'][0]['elements'][0]['status'] == 'OK':
#             d = res['rows'][0]['elements'][0]
#             return d['distance']['text'], d['duration']['text']
#     except Exception as e:
#         print("Distance error:", e)
#     return None, None

# def get_direction_link(origin_lat, origin_lng, dest_lat, dest_lng):
#     return f"https://www.google.com/maps/dir/?api=1&origin={origin_lat},{origin_lng}&destination={dest_lat},{dest_lng}&travelmode=driving"

# def get_preparation_time_summary(item_string):
#     if not item_string or not isinstance(item_string, str):
#         return 0, []
#     items = [i.strip().lower() for i in item_string.split(',')]
#     total_time = 0
#     details = []
#     for item in items:
#         default_time = 10
#         total_time += default_time
#         details.append(f"{item.title()} ({default_time} min)")
#     return total_time, details

# def find_zone(lng, lat, zones):
#     point = Point(lng, lat)
#     for z in zones:
#         if z['polygon'].contains(point):
#             return z['id'], z['title']
#     return None, None

# def load_zones():
#     conn = get_db_connection()
#     cursor = conn.cursor(dictionary=True)
#     cursor.execute("SELECT id, title, coordinates FROM zones WHERE status = 1")
#     zones = []
#     for row in cursor.fetchall():
#         try:
#             coords = [
#                 tuple(map(float, pt.replace("(", "").replace(")", "").strip().split(',')))
#                 for pt in row['coordinates'].split(',') if pt.count(',') == 1
#             ]
#             zones.append({'id': row['id'], 'title': row['title'], 'polygon': Polygon(coords)})
#         except:
#             continue
#     cursor.close()
#     conn.close()
#     return zones

# def get_active_delivery_zone(zone_id):
#     conn = get_db_connection()
#     cursor = conn.cursor(dictionary=True)
#     cursor.execute("""
#         SELECT id, zone_name, zone_data, radius_km, delivery_time_min, delivery_time_max
#         FROM tbl_delivery_zones
#         WHERE id = %s AND is_active = 1
#     """, (zone_id,))
#     zone = cursor.fetchone()
#     cursor.close()
#     conn.close()
#     return zone

# def is_within_zone(lat, lng, zone_data, radius_km):
#     center_lat = zone_data['center_lat']
#     center_lng = zone_data['center_lng']
#     distance = ((lat - center_lat)**2 + (lng - center_lng)**2)**0.5 * 111  # Approx km
#     return distance <= radius_km

# def validate_eta(eta_str, zone_meta):
#     try:
#         eta_min = int(eta_str.replace(' mins', '').replace(' min', '').strip())
#         return zone_meta['delivery_time_min'] <= eta_min <= zone_meta['delivery_time_max']
#     except:
#         return False

# # -------------------- Rider Logic --------------------

# def get_available_riders(zone_id=None):
#     conn = get_db_connection()
#     cursor = conn.cursor(dictionary=True)
#     query = """
#         SELECT r.id, r.title, ra.current_lat, ra.current_lng, ra.is_available,
#                ra.active_order_count, ra.max_capacity,
#                rp.acceptance_rate, rp.avg_delivery_time, rp.rejection_count
#         FROM tbl_rider r
#         JOIN tbl_rider_availability ra ON r.id = ra.rider_id
#         LEFT JOIN tbl_rider_performance rp ON r.id = rp.rider_id
#         WHERE r.status = 1 AND ra.is_available = 1 AND ra.active_order_count < ra.max_capacity
#     """
#     cursor.execute(query)
#     riders = cursor.fetchall()
#     cursor.close()
#     conn.close()

#     if zone_id:
#         route_riders = get_riders_by_route(zone_id)
#         riders = [r for r in riders if r['id'] in route_riders]

#     return riders

# def get_riders_by_route(zone_id):
#     conn = get_db_connection()
#     cursor = conn.cursor(dictionary=True)
#     cursor.execute("SELECT DISTINCT rider_id FROM tbl_rider_routes WHERE zone_id = %s", (zone_id,))
#     rider_ids = [r['rider_id'] for r in cursor.fetchall()]
#     cursor.close()
#     conn.close()
#     return rider_ids

# def calculate_rider_score(rider, dist_km):
#     acceptance_rate = rider.get('acceptance_rate') or 0
#     avg_delivery_time = rider.get('avg_delivery_time') or 30
#     score = (
#         0.5 * acceptance_rate +
#         0.3 * (1 / (dist_km + 1)) +
#         0.2 * (1 / (avg_delivery_time + 1))
#     )
#     return score

# # -------------------- Assignment & Notification --------------------

# def assign_order(order_id, rider_id, table_name, score, zone_id):
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     cursor.execute(f"UPDATE {table_name} SET rid = %s, order_status = 1 WHERE id = %s", (rider_id, order_id))
#     cursor.execute("UPDATE tbl_rider SET rstatus = 1 WHERE id = %s", (rider_id,))
#     cursor.execute("UPDATE tbl_rider_availability SET active_order_count = active_order_count + 1 WHERE rider_id = %s", (rider_id,))
#     cursor.execute("""
#         INSERT INTO tbl_rider_assignment (order_id, rider_id, assigned_at, score)
#         VALUES (%s, %s, NOW(), %s)
#     """, (order_id, rider_id, score))
#     cursor.execute("""
#         INSERT INTO tbl_delivery (order_id, rider_id, rider_response, status, assigned_at, zone_id)
#         VALUES (%s, %s, %s, %s, NOW(), %s)
#     """, (order_id, rider_id, 'pending', 'assigned', zone_id))
#     conn.commit()
#     cursor.close()
#     conn.close()

# def insert_rider_notifications(order_id, rider_ids, table_name):
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     for rid in rider_ids:
#         cursor.execute("""
#             INSERT INTO tbl_notification (uid, datetime, title, description, related_id, type)
#             VALUES (%s, NOW(), %s, %s, %s, %s)
#         """, (rid, "New Order Available", f"Please accept Order #{order_id}", order_id, table_name))
#         cursor.execute("""
#             INSERT INTO tbl_rnoti (rid, msg, date)
#             VALUES (%s, %s, NOW())
#         """, (rid, f"Order #{order_id} is available",))
#     conn.commit()
#     cursor.close()
#     conn.close()

# def notify_user(uid, order_id, name):
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     cursor.execute("""
#         INSERT INTO tbl_notification (uid, datetime, title, description)
#         VALUES (%s, NOW(), %s, %s)
#     """, (uid, "Order Assigned!", f"{name}, your Order #{order_id} has been assigned."))
#     conn.commit()
#     cursor.close()
#     conn.close()

# # -------------------- Core Assignment Logic --------------------

# def log_rider_rejection(order_id, rider_id, reason):
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     cursor.execute("""
#         INSERT INTO tbl_rider_rejections (order_id, rider_id, rejection_time, reason, created_at)
#         VALUES (%s, %s, NOW(), %s, NOW())
#     """, (order_id, rider_id, reason))
#     conn.commit()
#     cursor.close()
#     conn.close()

# def process_order_table(table_name):
#     zones = load_zones()
#     conn = get_db_connection()
#     cursor = conn.cursor(dictionary=True)
#     cursor.execute(f"SELECT * FROM {table_name} WHERE order_status = 0")
#     orders = cursor.fetchall()
#     cursor.close()
#     conn.close()

#     order_map = folium.Map(location=[18.6, 73.75], zoom_start=12)
#     assigned_orders = []
#     assigned, not_assigned = 0, 0

#     for order in orders:
#         full_address = f"{order['address']}, {order['landmark']}"
#         lat, lng = geocode_address(full_address)
#         if not lat or not lng:
#             print(f"Skipping Order #{order['id']} (Invalid address)")
#             continue

#         zone_id, zone_title = find_zone(lat, lng, zones)
#         zone_meta = get_active_delivery_zone(zone_id)

#         if not zone_meta:
#             print(f"Zone #{zone_id} inactive or undefined. Skipping Order #{order['id']}")
#             continue

#         if not is_within_zone(lat, lng, zone_meta['zone_data'], zone_meta['radius_km']):
#             print(f"Order #{order['id']} outside zone radius. Skipping.")
#             continue

#         riders = get_available_riders(zone_id)

#         if table_name == "tbl_normal_order":
#             total_time, prep_details = get_preparation_time_summary(order.get('items'))
#         else:
#             conn = get_db_connection()
#             cursor = conn.cursor(dictionary=True)
#             cursor.execute("SELECT ptitle FROM tbl_subscribe_order_product WHERE oid = %s", (order['id'],))
#             products = cursor.fetchall()
#             cursor.close()
#             conn.close()
#             item_names = [p['ptitle'] for p in products]
#             total_time, prep_details = get_preparation_time_summary(','.join(item_names))

#         best_score, nearest_rider = -1, None
#         best_dist, best_eta, best_link = None
#         rejected_riders = []

#         for r in riders:
#             r_lat, r_lng = float(r['current_lat']), float(r['current_lng'])
#             dist, eta = get_distance_and_time((r_lat, r_lng), (lat, lng))

#             if dist and eta:
#                 dist_km = float(dist.replace(' km', '').replace(',', ''))
#                 if not validate_eta(eta, zone_meta):
#                     rejected_riders.append((r['id'], "eta_invalid"))
#                     continue
#                 score = calculate_rider_score(r, dist_km)
#                 if score > best_score:
#                     nearest_rider = r
#                     best_score = score
#                     best_dist = dist
#                     best_eta = eta
#                     best_link = get_direction_link(r_lat, r_lng, lat, lng)
#                 else:
#                     rejected_riders.append((r['id'], "low_score"))
#             else:
#                 rejected_riders.append((r['id'], "distance_eta_unavailable"))

#         if nearest_rider:
#             assign_order(order['id'], nearest_rider['id'], table_name, best_score, zone_id)
#             insert_rider_notifications(order['id'], [nearest_rider['id']], table_name)
#             notify_user(order.get('uid', 0), order['id'], order.get('name', 'User'))

#             folium.Marker(
#                 location=[lat, lng],
#                 popup=f"Order #{order['id']} → {nearest_rider['title']}\n{best_dist}, {best_eta}",
#                 icon=folium.Icon(color="green")
#             ).add_to(order_map)

#             order['assigned_rider_name'] = nearest_rider['title']
#             order['zone'] = zone_title
#             order['distance'] = best_dist
#             order['eta'] = best_eta
#             order['route_link'] = best_link
#             assigned_orders.append(order)
#             assigned += 1

#             # Log rejections for all non-selected riders
#             for rider_id, reason in rejected_riders:
#                 if rider_id != nearest_rider['id']:
#                     log_rider_rejection(order['id'], rider_id, reason)
#         else:
#             not_assigned += 1
#             print(f"No available rider for Order #{order['id']}")
#             for rider_id, reason in rejected_riders:
#                 log_rider_rejection(order['id'], rider_id, reason)

#     order_map.save("order_assignment_map.html")
#     return assigned, not_assigned, assigned_orders

# # -------------------- Flask Routes --------------------

# @app.route('/')
# def home():
#     return jsonify({"message": "API is working!"})

# @app.route('/assign_orders', methods=['GET'])
# def assign_orders():
#     try:
#         a1, n1, list1 = process_order_table("tbl_normal_order")
#         a2, n2, list2 = process_order_table("tbl_subscribe_order")

#         detailed_assignments = []
#         for order in list1 + list2:
#             detailed_assignments.append({
#                 "order_id": order["id"],
#                 "user_name": order.get("name", "Unknown"),
#                 "zone": order.get("zone"),
#                 "assigned_rider": order.get("assigned_rider_name"),
#                 "distance": order.get("distance"),
#                 "eta": order.get("eta"),
#                 "google_maps_link": order.get("route_link")
#             })

#         return jsonify({
#             "assigned": a1 + a2,
#             "not_assigned": n1 + n2,
#             "message": "Order assignment completed. Map saved as order_assignment_map.html",
#             "details": detailed_assignments
#         })
#     except Exception as e:
#         print("Error in /assign_orders:", e)
#         return jsonify({"error": str(e)})

# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 10000))
#     app.run(host='0.0.0.0', port=port, debug=True)

