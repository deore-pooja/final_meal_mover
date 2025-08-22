import os
from flask import Flask, jsonify
from order_assign import process_order_table

app = Flask(__name__)

@app.route('/')
def home():
    """Simple API status check."""
    return jsonify({"message": "API is working!"})

@app.route('/assign_orders', methods=['GET'])
def assign_orders():
    """API endpoint to trigger order assignment for normal and subscribed orders."""
    try:
        print("Starting order assignment for 'tbl_normal_order'...")
        assigned_normal, not_assigned_normal, list_normal = process_order_table("tbl_normal_order")
        
        print("Starting order assignment for 'tbl_subscribe_order'...")
        assigned_subscribe, not_assigned_subscribe, list_subscribe = process_order_table("tbl_subscribe_order")

        all_assigned_orders = list_normal + list_subscribe
        
        detailed_assignments = []
        for order in all_assigned_orders:
            detailed_assignments.append({
                "order_id": order["id"],
                "user_name": order.get("name", "N/A"),
                "zone": order.get("zone", "N/A"),
                "assigned_rider": order.get("assigned_rider_name", "N/A"),
                "distance": order.get("distance", "N/A"),
                "eta": order.get("eta", "N/A"),
                "google_maps_link": order.get("route_link", "N/A")
            })

        total_assigned = assigned_normal + assigned_subscribe
        total_not_assigned = not_assigned_normal + not_assigned_subscribe

        return jsonify({
            "status": "success",
            "message": "Order assignment process completed. Map saved as order_assignment_map.html",
            "total_assigned": total_assigned,
            "total_not_assigned": total_not_assigned,
            "details": detailed_assignments
        })
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"status": "error", "message": f"An error occurred during order assignment: {str(e)}"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=True)




# import os
# from flask import Flask, jsonify
# from order_assign import process_order_table

# app = Flask(__name__)

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
#                 "user_name": order["name"],
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
#         return jsonify({"error": str(e)})


# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 10000))
#     app.run(host='0.0.0.0', port=port, debug=True)