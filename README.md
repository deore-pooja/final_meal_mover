# 🚀 Auto Order Assignment System

This project automates the rider assignment of delivery orders to nearby available riders using real-time geolocation, estimated travel time, and food preparation duration. It integrates Google Maps APIs, a MySQL database, and a Flask API server. The system also generates a visual map of assigned orders using Folium.

---

## 🧠 Key Features

- 📍 **Location-based rider assignment** using Google Maps Distance Matrix API
- ⏱️ **Food preparation time** calculation from Excel-based item list
- 🛵 **Rider selection** based on proximity and availability
- 🗺️ **Polygon-based delivery zone validation**
- 🌐 **Flask-based API** to trigger order assignment
- 📌 **Database-driven** operations with MySQL
- 🖼️ **Folium map** output for visual inspection of assignments

---

## 🧰 Tech Stack

- **Python 3.x**
- **Flask** – lightweight API server
- **Google Maps API** – geocoding & distance estimation
- **MySQL** – storage for orders, riders, zones, and notifications
- **Folium** – interactive map generation
- **Pandas** – Excel reading & manipulation
- **Shapely** – polygon boundary validation
- **dotenv** – secure environment configuration



