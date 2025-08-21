from flask import Flask, jsonify
from order_assign import process_order_table
import os

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "API is working!"})

@app.route('/assign_orders', methods=['GET'])
def assign_orders():
    try:
        # Process normal orders
        assigned_normal, not_assigned_normal, normal_orders = process_order_table("tbl_normal_order")

        # Process subscription orders
        assigned_subscribe, not_assigned_subscribe, subscribe_orders = process_order_table("tbl_subscribe_order")

        # Combine results
        detailed_assignments = []
        for order in normal_orders + subscribe_orders:
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
            "assigned": assigned_normal + assigned_subscribe,
            "not_assigned": not_assigned_normal + not_assigned_subscribe,
            "message": "Order assignment completed. Map saved as order_assignment_map.html",
            "details": detailed_assignments
        })

    except Exception as e:
        print("Error in /assign_orders:", e)
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=True)
