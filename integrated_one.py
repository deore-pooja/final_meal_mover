import os
import re
import json
import time
import mysql.connector
import googlemaps
import folium
from flask import Flask, jsonify
from dotenv import load_dotenv
from shapely.geometry import Point, Polygon

# -------------------- Setup --------------------
load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_API_KEY"))
app = Flask(__name__)

# -------------------- DB --------------------
def get_db_connection():
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        connect_timeout=60,
        connection_timeout=60,
        autocommit=False,
    )
    # try to ensure the session is alive
    try:
        conn.ping(reconnect=True, attempts=3, delay=5)
    except Exception:
        pass
    return conn

# -------------------- Google helpers --------------------
def geocode_address(address):
    try:
        result = gmaps.geocode(address)
        if result:
            loc = result[0]["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"])
    except Exception as e:
        print(f"[ERROR] Geocoding failed for '{address}': {e}")
    return None, None

def get_distance_and_time(origin, destination):
    """
    Returns (distance_text, duration_text) e.g., ('3.4 km', '12 mins')
    """
    try:
        res = gmaps.distance_matrix([origin], [destination], mode="driving")
        elem = res["rows"][0]["elements"][0]
        if elem["status"] == "OK":
            return elem["distance"]["text"], elem["duration"]["text"]
    except Exception as e:
        print("[ERROR] Distance calculation error:", e)
    return None, None

def get_direction_link(origin_lat, origin_lng, dest_lat, dest_lng):
    return (
        "https://www.google.com/maps/dir/?api=1"
        f"&origin={origin_lat},{origin_lng}"
        f"&destination={dest_lat},{dest_lng}"
        "&travelmode=driving"
    )

# -------------------- Domain helpers --------------------
def get_preparation_time_summary(item_string):
    if not item_string or not isinstance(item_string, str):
        return 0, []
    items = [i.strip().lower() for i in item_string.split(",") if i.strip()]
    total_time, details = 0, []
    for item in items:
        default_time = 10
        total_time += default_time
        details.append(f"{item.title()} ({default_time} min)")
    return total_time, details

def _parse_coordinates_blob(coord_blob_or_text):
    """
    Accepts BLOB/bytes or string like:
      "(18.6011, 73.7194),(18.5907, 73.7113),(...)"
    Returns list of (lng, lat) tuples for Shapely (lon first!)
    """
    if isinstance(coord_blob_or_text, (bytes, bytearray)):
        text = coord_blob_or_text.decode("utf-8", errors="ignore")
    else:
        text = str(coord_blob_or_text)

    # Grab all "(lat, lon)" pairs robustly
    pairs = re.findall(r"\(\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*\)", text)
    if not pairs:
        # maybe JSON like [[lat, lon],[...]]
        try:
            arr = json.loads(text)
            pairs = [(str(a[0]), str(a[1])) for a in arr if isinstance(a, (list, tuple)) and len(a) == 2]
        except Exception:
            pairs = []

    coords = []
    for lat_str, lng_str in pairs:
        lat = float(lat_str)
        lng = float(lng_str)
        coords.append((lng, lat))  # shapely expects (x=lng, y=lat)
    return coords

def load_zones():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, title, coordinates FROM zones WHERE status = 1")
    zones = []
    for row in cursor.fetchall():
        try:
            coords = _parse_coordinates_blob(row["coordinates"])
            if len(coords) < 3:
                raise ValueError("Not enough coordinates to form a polygon")
            zones.append(
                {"id": row["id"], "title": row["title"], "polygon": Polygon(coords)}
            )
        except Exception as e:
            print(f"[ERROR] Zone parsing failed for zone {row.get('id')}: {e}")
            continue
    cursor.close()
    conn.close()
    print(f"[INFO] Loaded {len(zones)} active zones")
    return zones

def find_zone(lat, lng, zones):
    """
    Returns (zone_id, zone_title) if point is inside or on boundary of a polygon.
    """
    point = Point(lng, lat)  # (x=lng, y=lat)
    for z in zones:
        # covers() includes boundary; contains() excludes boundary
        if z["polygon"].covers(point):
            return z["id"], z["title"]
    return None, None

def get_zone_meta(zone_id):
    """
    Metadata only. No geometry is read from this table.
    """
    if not zone_id:
        return None
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id, zone_name, delivery_fee, min_order_amount,
               delivery_time_min, delivery_time_max
        FROM tbl_delivery_zones
        WHERE id = %s AND is_active = 1
        """,
        (zone_id,),
    )
    zone = cursor.fetchone()
    cursor.close()
    conn.close()
    return zone

def _parse_duration_to_minutes(duration_text):
    """
    Converts '1 hour 5 mins' / '25 mins' to total minutes (int).
    """
    text = duration_text.lower()
    mins = 0
    m = re.search(r"(\d+)\s*hour", text)
    if m:
        mins += int(m.group(1)) * 60
    m = re.search(r"(\d+)\s*min", text)
    if m:
        mins += int(m.group(1))
    if mins == 0:
        # last resort: any number in string
        m = re.search(r"(\d+)", text)
        mins = int(m.group(1)) if m else 0
    return mins

def validate_eta(eta_text, zone_meta):
    try:
        eta_min = _parse_duration_to_minutes(eta_text)
        lo = int(zone_meta.get("delivery_time_min") or 0)
        hi = int(zone_meta.get("delivery_time_max") or 10**9)
        return lo <= eta_min <= hi
    except Exception as e:
        print("[ERROR] ETA validation failed:", e)
        return False

def _parse_distance_km(distance_text):
    """
    Handles '3.4 km' and '850 m'
    """
    t = distance_text.lower().replace(",", "").strip()
    if "km" in t:
        num = re.search(r"([0-9]*\.?[0-9]+)", t)
        return float(num.group(1)) if num else 1e9
    if "m" in t:
        num = re.search(r"([0-9]*\.?[0-9]+)", t)
        meters = float(num.group(1)) if num else 1e12
        return meters / 1000.0
    # fallback
    num = re.search(r"([0-9]*\.?[0-9]+)", t)
    return float(num.group(1)) if num else 1e9

# -------------------- Rider Logic --------------------
def get_available_riders(zone_id=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT r.id, r.title, ra.current_lat, ra.current_lng, ra.is_available,
               ra.active_order_count, ra.max_capacity,
               rp.acceptance_rate, rp.avg_delivery_time, rp.rejection_count
        FROM tbl_rider r
        JOIN tbl_rider_availability ra ON r.id = ra.rider_id
        LEFT JOIN tbl_rider_performance rp ON r.id = rp.rider_id
        WHERE r.status = 1
          AND ra.is_available = 1
          AND ra.active_order_count < ra.max_capacity
    """
    cursor.execute(query)
    riders = cursor.fetchall()
    cursor.close()
    conn.close()
    print(f"[INFO] Found {len(riders)} available riders (zone_id={zone_id})")

    if zone_id:
        route_ids = get_riders_by_route(zone_id)
        riders = [r for r in riders if r["id"] in route_ids]
        print(f"[INFO] Riders after route filter: {len(riders)}")
    return riders

def get_riders_by_route(zone_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT DISTINCT rider_id FROM tbl_rider_routes WHERE zone_id = %s", (zone_id,)
    )
    rider_ids = [r["rider_id"] for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rider_ids

def calculate_rider_score(rider, dist_km):
    acceptance_rate = float(rider.get("acceptance_rate") or 0.0)
    avg_delivery_time = float(rider.get("avg_delivery_time") or 30.0)
    score = (0.5 * acceptance_rate) + (0.3 * (1.0 / (dist_km + 1.0))) + (0.2 * (1.0 / (avg_delivery_time + 1.0)))
    return score

# -------------------- Assignment & Notification --------------------
def assign_order(order_id, rider_id, table_name, score, zone_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {table_name} SET rid = %s, order_status = 1 WHERE id = %s",
            (rider_id, order_id),
        )
        cursor.execute("UPDATE tbl_rider SET rstatus = 1 WHERE id = %s", (rider_id,))
        cursor.execute(
            "UPDATE tbl_rider_availability SET active_order_count = active_order_count + 1 WHERE rider_id = %s",
            (rider_id,),
        )
        cursor.execute(
            """
            INSERT INTO tbl_rider_assignment (order_id, rider_id, assigned_at, score)
            VALUES (%s, %s, NOW(), %s)
            """,
            (order_id, rider_id, score),
        )
        cursor.execute(
            """
            INSERT INTO tbl_delivery (order_id, rider_id, rider_response, status, assigned_at, zone_id)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            """,
            (order_id, rider_id, "pending", "assigned", zone_id),
        )
        conn.commit()
        print(f"[INFO] Assigned Order #{order_id} → Rider #{rider_id}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] assign_order rollback for order {order_id}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def insert_rider_notifications(order_id, rider_ids, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for rid in rider_ids:
            cursor.execute(
                """
                INSERT INTO tbl_notification (uid, datetime, title, description, related_id, type)
                VALUES (%s, NOW(), %s, %s, %s, %s)
                """,
                (rid, "New Order Available", f"Please accept Order #{order_id}", order_id, table_name),
            )
            cursor.execute(
                "INSERT INTO tbl_rnoti (rid, msg, date) VALUES (%s, %s, NOW())",
                (rid, f"Order #{order_id} is available"),
            )
        conn.commit()
        print(f"[INFO] Notifications sent to riders: {rider_ids}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] insert_rider_notifications rollback: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def notify_user(uid, order_id, name):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO tbl_notification (uid, datetime, title, description) VALUES (%s, NOW(), %s, %s)",
            (uid, "Order Assigned!", f"{name}, your Order #{order_id} has been assigned."),
        )
        conn.commit()
        print(f"[INFO] User #{uid} notified for Order #{order_id}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] notify_user rollback: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def log_rider_rejection(order_id, rider_id, reason):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO tbl_rider_rejections (order_id, rider_id, rejection_time, reason, created_at)
            VALUES (%s, %s, NOW(), %s, NOW())
            """,
            (order_id, rider_id, reason),
        )
        conn.commit()
        print(f"[WARN] Rider #{rider_id} rejected Order #{order_id} (Reason: {reason})")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] log_rider_rejection rollback: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# -------------------- Core Assignment --------------------
def process_order_table(table_name):
    zones = load_zones()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {table_name} WHERE order_status = 0")
    orders = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"[INFO] Found {len(orders)} pending orders in {table_name}")

    order_map = folium.Map(location=[18.6, 73.75], zoom_start=12)
    assigned_orders = []
    assigned, not_assigned = 0, 0

    for order in orders:
        print(f"\n[PROCESS] Order #{order['id']}")
        full_address = f"{order.get('address','')}, {order.get('landmark','')}".strip(", ")
        lat, lng = geocode_address(full_address)
        if lat is None or lng is None:
            print(f"[WARN] Skipping Order #{order['id']} → Invalid address")
            continue

        zone_id, zone_title = find_zone(lat, lng, zones)
        if not zone_id:
            print(f"[WARN] Order #{order['id']} not in any active zone")
            continue

        zone_meta = get_zone_meta(zone_id) or {}
        riders = get_available_riders(zone_id)
        if not riders:
            print(f"[WARN] No available riders for Order #{order['id']}")
            not_assigned += 1
            continue

        # prep time (not used for scoring, but available)
        if table_name == "tbl_normal_order":
            total_time, prep_details = get_preparation_time_summary(order.get("items"))
        else:
            conn = get_db_connection()
            c2 = conn.cursor(dictionary=True)
            c2.execute("SELECT ptitle FROM tbl_subscribe_order_product WHERE oid = %s", (order["id"],))
            products = c2.fetchall()
            c2.close()
            conn.close()
            item_names = [p["ptitle"] for p in products]
            total_time, prep_details = get_preparation_time_summary(",".join(item_names))

        best_score, best_dist_txt, best_eta_txt, best_link = -1, None, None, None
        nearest_rider = None
        rejected_riders = []

        for r in riders:
            try:
                r_lat, r_lng = float(r["current_lat"]), float(r["current_lng"])
            except (TypeError, ValueError):
                rejected_riders.append((r["id"], "bad_location"))
                continue

            dist_txt, eta_txt = get_distance_and_time((r_lat, r_lng), (lat, lng))
            if not (dist_txt and eta_txt):
                rejected_riders.append((r["id"], "distance_eta_unavailable"))
                continue

            dist_km = _parse_distance_km(dist_txt)
            if zone_meta and not validate_eta(eta_txt, zone_meta):
                rejected_riders.append((r["id"], "eta_invalid"))
                continue

            score = calculate_rider_score(r, dist_km)
            if score > best_score:
                best_score = score
                nearest_rider = r
                best_dist_txt = dist_txt
                best_eta_txt = eta_txt
                best_link = get_direction_link(r_lat, r_lng, lat, lng)
            else:
                rejected_riders.append((r["id"], "low_score"))

        if nearest_rider:
            assign_order(order["id"], nearest_rider["id"], table_name, best_score, zone_id)
            insert_rider_notifications(order["id"], [nearest_rider["id"]], table_name)
            notify_user(order.get("uid", 0), order["id"], order.get("name", "User"))

            folium.Marker(
                location=[lat, lng],
                popup=f"Order #{order['id']} → {nearest_rider['title']}\n{best_dist_txt}, {best_eta_txt}",
                icon=folium.Icon(color="green"),
            ).add_to(order_map)

            order["assigned_rider_name"] = nearest_rider["title"]
            order["zone"] = zone_title
            order["distance"] = best_dist_txt
            order["eta"] = best_eta_txt
            order["route_link"] = best_link
            assigned_orders.append(order)
            assigned += 1

            for rider_id, reason in rejected_riders:
                if rider_id != nearest_rider["id"]:
                    log_rider_rejection(order["id"], rider_id, reason)
        else:
            not_assigned += 1
            print(f"[WARN] No rider selected for Order #{order['id']}")
            for rider_id, reason in rejected_riders:
                log_rider_rejection(order["id"], rider_id, reason)

    order_map.save("order_assignment_map.html")
    return assigned, not_assigned, assigned_orders

# -------------------- Flask Routes --------------------
@app.route("/")
def home():
    return jsonify({"message": "API is working!"})

@app.route("/assign_orders", methods=["GET"])
def assign_orders():
    try:
        a1, n1, list1 = process_order_table("tbl_normal_order")
        a2, n2, list2 = process_order_table("tbl_subscribe_order")

        detailed_assignments = []
        for order in list1 + list2:
            detailed_assignments.append(
                {
                    "order_id": order["id"],
                    "user_name": order.get("name", "Unknown"),
                    "zone": order.get("zone"),
                    "assigned_rider": order.get("assigned_rider_name"),
                    "distance": order.get("distance"),
                    "eta": order.get("eta"),
                    "google_maps_link": order.get("route_link"),
                }
            )

        return jsonify(
            {
                "assigned": a1 + a2,
                "not_assigned": n1 + n2,
                "message": "Order assignment completed. Map saved as order_assignment_map.html",
                "details": detailed_assignments,
            }
        )
    except Exception as e:
        print("[ERROR] in /assign_orders:", e)
        return jsonify({"error": str(e)}), 500

# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)






# import os
# import time
# import json
# import mysql.connector
# import googlemaps
# import folium
# from flask import Flask, jsonify
# from dotenv import load_dotenv
# from shapely.geometry import Point, Polygon, shape

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
#     except Exception as e:
#         print(f"[ERROR] Geocoding failed for '{address}':", e)
#     return None, None

# def get_distance_and_time(origin, destination):
#     try:
#         res = gmaps.distance_matrix([origin], [destination], mode="driving")
#         if res['rows'][0]['elements'][0]['status'] == 'OK':
#             d = res['rows'][0]['elements'][0]
#             return d['distance']['text'], d['duration']['text']
#     except Exception as e:
#         print("[ERROR] Distance calculation error:", e)
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

# def find_zone(lat, lng, zones):
#     point = Point(lng, lat)   # (lng, lat)
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
#                 for pt in row['coordinates'].split(';') if pt.strip()
#             ]
#             zones.append({'id': row['id'], 'title': row['title'], 'polygon': Polygon(coords)})
#         except Exception as e:
#             print(f"[ERROR] Zone parsing failed for zone {row['id']}: {e}")
#             continue
#     cursor.close()
#     conn.close()
#     print(f"[INFO] Loaded {len(zones)} active zones")
#     return zones

# def get_active_delivery_zone(zone_id):
#     if not zone_id:
#         return None
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

#     if zone and isinstance(zone.get("zone_data"), str):
#         try:
#             zone["zone_data"] = json.loads(zone["zone_data"])
#         except Exception:
#             zone["zone_data"] = {}
#     return zone

# def is_within_zone(lat, lng, zone_data, radius_km=None):
#     """
#     Check if user location lies inside polygon (preferred), 
#     or within radius fallback if polygon not available.
#     """
#     try:
#         point = Point(lng, lat)  # shapely expects (lng, lat)

#         # If polygon data exists
#         if zone_data and "type" in zone_data:
#             polygon = shape(zone_data)
#             return polygon.contains(point)

#         # Fallback: radius check
#         if radius_km and "center_lat" in zone_data and "center_lng" in zone_data:
#             center_lat = float(zone_data['center_lat'])
#             center_lng = float(zone_data['center_lng'])
#             distance = ((lat - center_lat)**2 + (lng - center_lng)**2)**0.5 * 111  # rough km
#             return distance <= radius_km

#     except Exception as e:
#         print(f"[ERROR] Zone check failed: {e}")
#     return False

# def validate_eta(eta_str, zone_meta):
#     try:
#         eta_min = int(eta_str.replace(' mins', '').replace(' min', '').strip())
#         return zone_meta['delivery_time_min'] <= eta_min <= zone_meta['delivery_time_max']
#     except Exception as e:
#         print("[ERROR] ETA validation failed:", e)
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
#     print(f"[INFO] Found {len(riders)} available riders (zone_id={zone_id})")

#     if zone_id:
#         route_riders = get_riders_by_route(zone_id)
#         riders = [r for r in riders if r['id'] in route_riders]
#         print(f"[INFO] Riders after route filter: {len(riders)}")

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
#     print(f"[INFO] Assigned Order #{order_id} → Rider #{rider_id}")

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
#     print(f"[INFO] Notifications sent to riders: {rider_ids}")

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
#     print(f"[INFO] User #{uid} notified for Order #{order_id}")

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
#     print(f"[WARN] Rider #{rider_id} rejected Order #{order_id} (Reason: {reason})")

# def process_order_table(table_name):
#     zones = load_zones()
#     conn = get_db_connection()
#     cursor = conn.cursor(dictionary=True)
#     cursor.execute(f"SELECT * FROM {table_name} WHERE order_status = 0")
#     orders = cursor.fetchall()
#     cursor.close()
#     conn.close()

#     print(f"[INFO] Found {len(orders)} pending orders in {table_name}")

#     order_map = folium.Map(location=[18.6, 73.75], zoom_start=12)
#     assigned_orders = []
#     assigned, not_assigned = 0, 0

#     for order in orders:
#         print(f"\n[PROCESS] Order #{order['id']}")

#         full_address = f"{order['address']}, {order['landmark']}"
#         lat, lng = geocode_address(full_address)
#         if not lat or not lng:
#             print(f"[WARN] Skipping Order #{order['id']} → Invalid address")
#             continue

#         zone_id, zone_title = find_zone(lat, lng, zones)
#         if not zone_id:
#             print(f"[WARN] Order #{order['id']} not in any active zone")
#             continue

#         zone_meta = get_active_delivery_zone(zone_id)
#         if not zone_meta:
#             print(f"[WARN] Zone #{zone_id} inactive for Order #{order['id']}")
#             continue

#         if not is_within_zone(lat, lng, zone_meta['zone_data'], zone_meta['radius_km']):
#             print(f"[WARN] Order #{order['id']} outside zone radius")
#             continue

#         riders = get_available_riders(zone_id)
#         if not riders:
#             print(f"[WARN] No available riders for Order #{order['id']}")
#             not_assigned += 1
#             continue

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
#         best_dist, best_eta, best_link = None, None, None
#         rejected_riders = []

#         for r in riders:
#             r_lat, r_lng = float(r['current_lat']), float(r['current_lng'])
#             dist, eta = get_distance_and_time((r_lat, r_lng), (lat, lng))

#             if dist and eta:
#                 try:
#                     dist_km = float(dist.replace(' km', '').replace(',', ''))
#                 except:
#                     dist_km = 999999
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

#             for rider_id, reason in rejected_riders:
#                 if rider_id != nearest_rider['id']:
#                     log_rider_rejection(order['id'], rider_id, reason)
#         else:
#             not_assigned += 1
#             print(f"[WARN] No rider selected for Order #{order['id']}")
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
#         print("[ERROR] in /assign_orders:", e)
#         return jsonify({"error": str(e)})

# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 10000))
#     app.run(host='0.0.0.0', port=port, debug=True)
