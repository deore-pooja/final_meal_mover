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

# Load environment variables
load_dotenv()
gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_API_KEY"))
app = Flask(__name__)

# -------------------- Utility Functions --------------------

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

def geocode_address(address):
    try:
        if not address or not address.strip():
            return None, None
        result = gmaps.geocode(address)
        if result:
            loc = result[0]['geometry']['location']
            return loc['lat'], loc['lng']
        print(f"[WARN] Geocoding returned empty for '{address}'")
    except Exception as e:
        print(f"[ERROR] Geocoding failed for '{address}':", e)
    return None, None

def _parse_distance_text_to_km(text):
    """
    Accepts '2.3 km' or '850 m' and returns float km.
    """
    if not text:
        return None
    try:
        t = text.strip().lower()
        if 'km' in t:
            return float(t.replace('km', '').strip())
        if 'm' in t:
            meters = float(t.replace('m', '').strip())
            return meters / 1000.0
    except Exception:
        pass
    return None

def get_distance_and_time(origin, destination):
    try:
        # origin/destination must be (lat, lng)
        res = gmaps.distance_matrix([origin], [destination], mode="driving")
        elem = res['rows'][0]['elements'][0]
        if elem['status'] == 'OK':
            d_txt = elem['distance']['text']
            dur_txt = elem['duration']['text']
            return d_txt, dur_txt
        print("[WARN] DistanceMatrix element status:", elem.get('status'))
    except Exception as e:
        print("[ERROR] Distance calculation error:", e)
    return None, None

def get_direction_link(origin_lat, origin_lng, dest_lat, dest_lng):
    return f"https://www.google.com/maps/dir/?api=1&origin={origin_lat},{origin_lng}&destination={dest_lat},{dest_lng}&travelmode=driving"

def get_preparation_time_summary(item_string):
    if not item_string or not isinstance(item_string, str):
        return 0, []
    items = [i.strip().lower() for i in item_string.split(',') if i.strip()]
    total_time = 0
    details = []
    for item in items:
        default_time = 10
        total_time += default_time
        details.append(f"{item.title()} ({default_time} min)")
    return total_time, details

def _try_parse_zone_coords_text(coord_text):
    """
    Try several textual formats and return list of (lng, lat) tuples for Polygon.
    Supported:
      - "(18.52,73.8567),(18.53,73.86),..."   (lat,lng) pairs with/without parentheses
      - "18.52,73.8567; 18.53,73.86; ..."     (lat,lng) separated by semicolons
      - "18.52 73.8567 | 18.53 73.86"         (lat lng) separated by pipes/spaces
    """
    pts = []

    s = coord_text.strip()

    # 1) Look for pairs inside parentheses
    paren_pairs = re.findall(r'\(?\s*([+-]?\d+\.?\d*)\s*,\s*([+-]?\d+\.?\d*)\s*\)?', s)
    if paren_pairs and len(paren_pairs) >= 3:
        for lat_str, lng_str in paren_pairs:
            lat = float(lat_str)
            lng = float(lng_str)
            pts.append((lng, lat))  # Polygon wants (x=lng, y=lat)
        return pts

    # 2) Semicolon-separated "lat,lng"
    if ';' in s:
        chunks = [c.strip() for c in s.split(';') if c.strip()]
        for ch in chunks:
            if ',' in ch:
                lat_str, lng_str = [x.strip() for x in ch.split(',', 1)]
                lat, lng = float(lat_str), float(lng_str)
                pts.append((lng, lat))
        if len(pts) >= 3:
            return pts

    # 3) Space or pipe separated "lat lng"
    chunks = re.split(r'[|;]', s)
    temp = []
    for ch in chunks:
        nums = re.findall(r'([+-]?\d+\.?\d*)', ch)
        if len(nums) >= 2:
            lat, lng = float(nums[0]), float(nums[1])
            temp.append((lng, lat))
    if len(temp) >= 3:
        return temp

    return []

def find_zone(lat, lng, zones):
    """
    lat,lng in degrees. Shapely expects Point(lng, lat) and Polygon with (lng,lat) vertices.
    """
    point = Point(lng, lat)
    for z in zones:
        if z['polygon'].contains(point):
            return z['id'], z['title']
    return None, None

def load_zones():
    """
    Loads active zones from `zones` table and builds Shapely Polygons.
    Supports:
      - zones.coordinates as GeoJSON Polygon
      - zones.coordinates as JSON array of [lat,lng] pairs
      - zones.coordinates as various text formats (see _try_parse_zone_coords_text)
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, title, coordinates FROM zones WHERE status = 1")
    zones = []
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    for row in rows:
        coords_raw = row['coordinates']
        polygon = None

        if not coords_raw:
            print(f"[WARN] Zone {row['id']} has empty coordinates; skipping")
            continue

        try:
            # Try JSON first
            data = json.loads(coords_raw)
            pts = []

            # GeoJSON: {"type": "Polygon", "coordinates": [[[lng,lat],...]]}
            if isinstance(data, dict) and 'coordinates' in data:
                coords = data['coordinates']
                if coords and isinstance(coords, list):
                    ring = coords[0]
                    for lng, lat in ring:
                        pts.append((float(lng), float(lat)))
            # List of pairs: [[lat,lng], [lat,lng], ...] or [[lng,lat], ...]
            elif isinstance(data, list) and len(data) >= 3:
                # Detect if first pair looks like [lat,lng] or [lng,lat]
                first = data[0]
                if isinstance(first, (list, tuple)) and len(first) >= 2:
                    a, b = float(first[0]), float(first[1])
                    # Heuristic: latitude in India ~ 8..37; longitude ~ 68..97
                    # If a looks like lat, treat as [lat,lng] => convert to (lng,lat)
                    if -90 <= a <= 90 and -180 <= b <= 180:
                        for el in data:
                            la, ln = float(el[0]), float(el[1])
                            pts.append((ln, la))
                    else:
                        for el in data:
                            ln, la = float(el[0]), float(el[1])
                            pts.append((ln, la))

            if not pts:
                # Fallback: parse plain text
                pts = _try_parse_zone_coords_text(coords_raw)

            if len(pts) < 3:
                print(f"[WARN] Zone parsing produced <3 points for zone {row['id']}; skipping")
                continue

            polygon = Polygon(pts)
            if not polygon.is_valid:
                polygon = polygon.buffer(0)  # attempt fix
            zones.append({'id': row['id'], 'title': row['title'], 'polygon': polygon})
        except Exception as e:
            print(f"[ERROR] Zone parsing failed for zone {row['id']}: {e}")
            continue

    print(f"[INFO] Loaded {len(zones)} active zones")
    return zones

def get_active_delivery_zone(zone_id):
    """
    Returns zone meta with parsed zone_data JSON (expects center_lat/center_lng) and numeric radii + time bounds.
    """
    if not zone_id:
        return None
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, zone_name, zone_data, radius_km, delivery_time_min, delivery_time_max, is_active
        FROM tbl_delivery_zones
        WHERE id = %s
    """, (zone_id,))
    zone = cursor.fetchone()
    cursor.close()
    conn.close()

    if not zone:
        return None
    if not zone.get('is_active'):
        return None

    # Parse zone_data JSON safely
    zd = zone.get('zone_data')
    try:
        if isinstance(zd, str):
            zone['zone_data'] = json.loads(zd)
        elif zd is None:
            zone['zone_data'] = {}
    except Exception as e:
        print(f"[WARN] zone_data JSON parse failed for zone {zone_id}: {e}")
        zone['zone_data'] = {}

    # Coerce numeric fields
    for k in ('radius_km', 'delivery_time_min', 'delivery_time_max'):
        try:
            if zone[k] is not None:
                zone[k] = float(zone[k]) if 'radius' in k else int(zone[k])
        except Exception:
            pass
    return zone

def is_within_zone(lat, lng, zone_data, radius_km):
    """
    Haversine-lite (flat-earth approx) distance from center in km, using center_lat/center_lng from zone_data.
    """
    try:
        center_lat = float(zone_data.get('center_lat'))
        center_lng = float(zone_data.get('center_lng'))
        if center_lat is None or center_lng is None:
            print("[WARN] zone_data missing center_lat/center_lng; treating as outside radius")
            return False
        distance = ((lat - center_lat)**2 + (lng - center_lng)**2)**0.5 * 111  # Approx km
        return distance <= float(radius_km)
    except Exception as e:
        print("[ERROR] Zone radius check failed:", e)
        return False

def validate_eta(eta_str, zone_meta):
    try:
        # eta like '23 mins' or '1 hour 5 mins' (we handle the first int found)
        nums = re.findall(r'\d+', eta_str or '')
        if not nums:
            return False
        eta_min = int(nums[0])
        tmin = int(zone_meta.get('delivery_time_min') or 0)
        tmax = int(zone_meta.get('delivery_time_max') or 10**9)
        return tmin <= eta_min <= tmax
    except Exception as e:
        print("[ERROR] ETA validation failed:", e)
        return False

# -------------------- Rider Logic --------------------

def get_available_riders(zone_id=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT r.id, r.title, ra.current_lat, ra.current_lng, ra.is_available,
               ra.active_order_count, ra.max_capacity,
               COALESCE(rp.acceptance_rate, 0) AS acceptance_rate,
               COALESCE(rp.avg_delivery_time, 30) AS avg_delivery_time,
               COALESCE(rp.rejection_count, 0) AS rejection_count
        FROM tbl_rider r
        JOIN tbl_rider_availability ra ON r.id = ra.rider_id
        LEFT JOIN tbl_rider_performance rp ON r.id = rp.rider_id
        WHERE r.status = 1 AND ra.is_available = 1 AND ra.active_order_count < ra.max_capacity
    """
    cursor.execute(query)
    riders = cursor.fetchall()
    cursor.close()
    conn.close()
    print(f"[INFO] Found {len(riders)} available riders (pre-filter, zone_id={zone_id})")

    if zone_id:
        route_riders = get_riders_by_route(zone_id)
        riders = [r for r in riders if r['id'] in route_riders]
        print(f"[INFO] Riders after route filter: {len(riders)}")

    return riders

def get_riders_by_route(zone_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT DISTINCT rider_id FROM tbl_rider_routes WHERE zone_id = %s", (zone_id,))
    rider_ids = [r['rider_id'] for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rider_ids

def calculate_rider_score(rider, dist_km):
    acceptance_rate = float(rider.get('acceptance_rate') or 0)
    avg_delivery_time = float(rider.get('avg_delivery_time') or 30)
    if dist_km is None:
        dist_km = 1e6
    score = (
        0.5 * acceptance_rate +
        0.3 * (1 / (dist_km + 1)) +
        0.2 * (1 / (avg_delivery_time + 1))
    )
    return score

# -------------------- Assignment & Notification --------------------

def assign_order(order_id, rider_id, table_name, score, zone_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {table_name} SET rid = %s, order_status = 1 WHERE id = %s", (rider_id, order_id))
    cursor.execute("UPDATE tbl_rider SET rstatus = 1 WHERE id = %s", (rider_id,))
    cursor.execute("UPDATE tbl_rider_availability SET active_order_count = active_order_count + 1 WHERE rider_id = %s", (rider_id,))
    cursor.execute("""
        INSERT INTO tbl_rider_assignment (order_id, rider_id, assigned_at, score)
        VALUES (%s, %s, NOW(), %s)
    """, (order_id, rider_id, score))
    cursor.execute("""
        INSERT INTO tbl_delivery (order_id, rider_id, rider_response, status, assigned_at, zone_id)
        VALUES (%s, %s, %s, %s, NOW(), %s)
    """, (order_id, rider_id, 'pending', 'assigned', zone_id))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[INFO] Assigned Order #{order_id} → Rider #{rider_id} (score={score:.4f})")

def insert_rider_notifications(order_id, rider_ids, table_name):
    if not rider_ids:
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    for rid in rider_ids:
        cursor.execute("""
            INSERT INTO tbl_notification (uid, datetime, title, description, related_id, type)
            VALUES (%s, NOW(), %s, %s, %s, %s)
        """, (rid, "New Order Available", f"Please accept Order #{order_id}", order_id, table_name))
        cursor.execute("""
            INSERT INTO tbl_rnoti (rid, msg, date)
            VALUES (%s, %s, NOW())
        """, (rid, f"Order #{order_id} is available",))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[INFO] Notifications sent to riders: {rider_ids}")

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
    print(f"[INFO] User #{uid} notified for Order #{order_id}")

# -------------------- Core Assignment Logic --------------------

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
    print(f"[WARN] Rider #{rider_id} rejected Order #{order_id} (Reason: {reason})")

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
        address = (order.get('address') or "").strip()
        landmark = (order.get('landmark') or "").strip()
        full_address = f"{address}, {landmark}".strip(", ").strip()

        lat, lng = geocode_address(full_address)
        if lat is None or lng is None:
            print(f"[WARN] Skipping Order #{order['id']} → Invalid address '{full_address}'")
            not_assigned += 1
            continue

        zone_id, zone_title = find_zone(lat, lng, zones)
        if not zone_id:
            print(f"[WARN] Order #{order['id']} point not inside any active zone polygon")
            not_assigned += 1
            continue

        zone_meta = get_active_delivery_zone(zone_id)
        if not zone_meta:
            print(f"[WARN] Zone #{zone_id} inactive or missing meta for Order #{order['id']}")
            not_assigned += 1
            continue

        if not is_within_zone(lat, lng, zone_meta['zone_data'], zone_meta['radius_km']):
            print(f"[WARN] Order #{order['id']} outside zone radius (zone_id={zone_id})")
            not_assigned += 1
            continue

        riders = get_available_riders(zone_id)
        if not riders:
            print(f"[WARN] No available riders for Order #{order['id']} (zone_id={zone_id})")
            not_assigned += 1
            continue

        # Build prep time (kept to your original behavior)
        if table_name == "tbl_normal_order":
            total_time, prep_details = get_preparation_time_summary(order.get('items'))
        else:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT ptitle FROM tbl_subscribe_order_product WHERE oid = %s", (order['id'],))
            products = cursor.fetchall()
            cursor.close()
            conn.close()
            item_names = [p['ptitle'] for p in products]
            total_time, prep_details = get_preparation_time_summary(','.join(item_names))

        best_score, nearest_rider = -1.0, None
        best_dist_txt, best_eta_txt, best_link = None, None, None
        rejected_riders = []

        for r in riders:
            r_lat, r_lng = float(r['current_lat']), float(r['current_lng'])
            dist_txt, eta_txt = get_distance_and_time((r_lat, r_lng), (lat, lng))

            if dist_txt and eta_txt:
                dist_km = _parse_distance_text_to_km(dist_txt)
                if dist_km is None:
                    rejected_riders.append((r['id'], "distance_parse_error"))
                    continue
                if not validate_eta(eta_txt, zone_meta):
                    rejected_riders.append((r['id'], "eta_invalid"))
                    continue
                score = calculate_rider_score(r, dist_km)
                if score > best_score:
                    nearest_rider = r
                    best_score = score
                    best_dist_txt = dist_txt
                    best_eta_txt = eta_txt
                    best_link = get_direction_link(r_lat, r_lng, lat, lng)
                else:
                    rejected_riders.append((r['id'], "low_score"))
            else:
                rejected_riders.append((r['id'], "distance_eta_unavailable"))

        if nearest_rider:
            assign_order(order['id'], nearest_rider['id'], table_name, best_score, zone_id)
            insert_rider_notifications(order['id'], [nearest_rider['id']], table_name)
            notify_user(order.get('uid', 0), order['id'], order.get('name', 'User'))

            folium.Marker(
                location=[lat, lng],
                popup=f"Order #{order['id']} → {nearest_rider['title']}\n{best_dist_txt}, {best_eta_txt}",
                icon=folium.Icon(color="green")
            ).add_to(order_map)

            order['assigned_rider_name'] = nearest_rider['title']
            order['zone'] = zone_title
            order['distance'] = best_dist_txt
            order['eta'] = best_eta_txt
            order['route_link'] = best_link
            assigned_orders.append(order)
            assigned += 1

            # Log rejections for all non-selected riders
            for rider_id, reason in rejected_riders:
                if rider_id != nearest_rider['id']:
                    log_rider_rejection(order['id'], rider_id, reason)
        else:
            not_assigned += 1
            print(f"[WARN] No rider selected for Order #{order['id']}")
            for rider_id, reason in rejected_riders:
                log_rider_rejection(order['id'], rider_id, reason)

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

        detailed_assignments = []
        for order in list1 + list2:
            detailed_assignments.append({
                "order_id": order["id"],
                "user_name": order.get("name", "Unknown"),
                "zone": order.get("zone"),
                "assigned_rider": order.get("assigned_rider_name"),
                "distance": order.get("distance"),
                "eta": order.get("eta"),
                "google_maps_link": order.get("route_link")
            })

        return jsonify({
            "assigned": a1 + a2,
            "not_assigned": n1 + n2,
            "message": "Order assignment completed. Map saved as order_assignment_map.html",
            "details": detailed_assignments
        })
    except Exception as e:
        print("[ERROR] in /assign_orders:", e)
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=True)


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
#     point = Point(lat, lng)
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
#     return zone

# def is_within_zone(lat, lng, zone_data, radius_km):
#     try:
#         center_lat = zone_data['center_lat']
#         center_lng = zone_data['center_lng']
#         distance = ((lat - center_lat)**2 + (lng - center_lng)**2)**0.5 * 111  # Approx km
#         return distance <= radius_km
#     except Exception as e:
#         print("[ERROR] Zone radius check failed:", e)
#         return False

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
