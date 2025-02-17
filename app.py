from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import logging

app = Flask(__name__)
CORS(app)

# Setup logging
logging.basicConfig(level=logging.DEBUG)

# Database connection function
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="manoj",
            database="college_predictor"
        )
        return conn
    except mysql.connector.Error as e:
        logging.error("❌ Database Connection Failed: %s", e)
        return None

@app.route("/predict", methods=["POST"])
def predict_colleges():
    data = request.json

    # Validate required fields
    required_fields = ["maths", "physics", "chemistry", "min_cutoff", "category", "branch", "district"]
    for field in required_fields:
        if field not in data:
            logging.warning(f"⚠️ Missing required field: {field}")
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        maths = float(data.get("maths"))
        physics = float(data.get("physics"))
        chemistry = float(data.get("chemistry"))
        min_cutoff = float(data.get("min_cutoff"))
        category = data.get("category").strip()
        branch = data.get("branch").strip()
        district = data.get("district").strip()

        # Validate input data
        if maths < 0 or physics < 0 or chemistry < 0 or min_cutoff < 0:
            logging.warning("⚠️ Invalid input values. Marks and cutoffs should be non-negative.")
            return jsonify({"error": "Marks and cutoffs should be non-negative."}), 400

        # Calculate Max Cutoff
        max_cutoff = maths + (physics / 2) + (chemistry / 2)

        # Connect to Database
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor(dictionary=True)

        # Dynamic Query Construction
        query = """
            SELECT college_name, branch, branch_code, district, community, average_cutoff
            FROM colleges
            WHERE average_cutoff >= %s AND average_cutoff <= %s
        """
        params = [min_cutoff, max_cutoff]

        # Apply optional filters
        if category:
            query += " AND community LIKE %s"
            params.append(f"%{category}%")
        if branch:
            query += " AND branch LIKE %s"
            params.append(f"%{branch}%")
        if district:
            query += " AND district LIKE %s"
            params.append(f"%{district}%")

        try:
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
        except mysql.connector.Error as err:
            logging.error("❌ SQL Query Failed: %s", err)
            return jsonify({"error": "Database query failed"}), 500

        conn.close()

        logging.debug(f"✅ Query Results: {results}")
        logging.debug(f"🔢 Cutoff Range: {min_cutoff} - {max_cutoff}")
        logging.debug(f"🛠 Query Params: Category={category}, Branch={branch}, District={district}")

        if results:
            return jsonify(results)
        else:
            return jsonify({"message": "No matching colleges found"}), 404

    except Exception as e:
        logging.error("❌ Error processing prediction request: %s", e, exc_info=True)
        return jsonify({"error": "An error occurred while processing the prediction"}), 500

@app.route("/filters", methods=["GET"])
def get_filters():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor(dictionary=True)

        # Fetch unique categories
        cursor.execute("SELECT DISTINCT community FROM colleges")
        categories = [{"value": row["community"], "label": row["community"]} for row in cursor.fetchall()]

        # Fetch unique branches
        cursor.execute("SELECT DISTINCT branch FROM colleges")
        branches = [{"value": row["branch"], "label": row["branch"]} for row in cursor.fetchall()]

        # Fetch unique districts
        cursor.execute("SELECT DISTINCT district FROM colleges")
        districts = [{"value": row["district"], "label": row["district"]} for row in cursor.fetchall()]

        conn.close()

        return jsonify({
            "categories": categories,
            "branches": branches,
            "districts": districts
        })

    except Exception as e:
        logging.error(f"❌ Error fetching filters: {str(e)}", exc_info=True)
        return jsonify({"error": "An error occurred while fetching filters"}), 500

@app.route("/register", methods=["POST"])
def register_user():
    data = request.json

    # Input validation
    required_fields = ["name", "age", "gender", "school", "dob", "mobile", "email"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400

    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        query = """
            INSERT INTO users (name, age, gender, school, dob, mobile, email)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (data["name"], data["age"], data["gender"], data["school"],
                               data["dob"], data["mobile"], data["email"]))
        conn.commit()
        conn.close()

        return jsonify({"message": "User registered successfully!"}), 201
    except Exception as e:
        logging.error("❌ Error processing registration request: %s", e, exc_info=True)
        return jsonify({"error": "An error occurred while processing the registration"}), 500

@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"message": "Server is running!"}), 200

if __name__ == "__main__":
    app.run(debug=True)
