from flask import Flask, request, jsonify
import mysql.connector
import logging
import os
from dotenv import load_dotenv


from flask_cors import CORS

app = Flask(__name__)
CORS(app) 

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_db_connection():
    return mysql.connector.connect(
        # host=os.getenv("MYSQL_HOST"),
        # user=os.getenv("MYSQL_USER"),
        # password=os.getenv("MYSQL_PASSWORD"),
        # database=os.getenv("MYSQL_DATABASE"),
        # port=int(os.getenv("MYSQL_PORT"))
         host="localhost",
        user="root",
        password="manoj",
        database="college_predictor"
    )




@app.route('/predict', methods=['POST'])
def predict_colleges():
    try:
        data = request.json
        logging.info(f"🔍 Received request data: {data}")  # Debugging log

        min_cutoff = data.get("min_cutoff")
        max_cutoff = data.get("max_cutoff")
        category = data.get("category")
        branch = data.get("branch")
        district = data.get("district")

        # Log extracted values
        logging.info(f"Parsed values: min_cutoff={min_cutoff}, max_cutoff={max_cutoff}, category={category}, branch={branch}, district={district}")

        # Validate required fields
        if min_cutoff is None or max_cutoff is None or not category:
            logging.error("❌ Missing required fields in request")
            return jsonify({"error": "Missing required fields: min_cutoff, max_cutoff, category"}), 400

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # ✅ Use `average_cutoff` instead of `cutoff`
        query = """
        SELECT DISTINCT * FROM colleges 
        WHERE average_cutoff BETWEEN %s AND %s 
        AND community = %s AND branchname = %s AND district = %s
        """
        cursor.execute(query, (min_cutoff, max_cutoff, category,branch,district))
        result = cursor.fetchall()

        logging.info(f"✅ Query result: {result}")  # Debugging log

        cursor.close()
        conn.close()

        return jsonify({"predicted_colleges": result})
    except Exception as e:
        logging.error(f"❌ Error in /predict: {str(e)}")
        return jsonify({"error": "An error occurred while predicting colleges."}), 500
@app.route('/categories', methods=['GET'])
def get_categories():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT DISTINCT community FROM colleges")
        result = cursor.fetchall()

        categories = [row['community'] for row in result]

        cursor.close()
        conn.close()

        return jsonify({"categories": categories})
    except Exception as e:
        logging.error(f"Error in /categories: {str(e)}")
        return jsonify({"error": str(e)}), 500
@app.route('/districts', methods=['GET'])
def get_districts():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # ✅ Fetch DISTINCT districts from the `colleges` table
        cursor.execute("SELECT DISTINCT district FROM colleges")
        result = cursor.fetchall()

        districts = [row['district'] for row in result]

        cursor.close()
        conn.close()

        return jsonify({"districts": districts})
    except Exception as e:
        logging.error(f"❌ Error in /districts: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/branches', methods=['GET'])
def get_branches():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Fetch DISTINCT branch names from the colleges table
        cursor.execute("SELECT DISTINCT branchname FROM colleges WHERE branchname IS NOT NULL")
        result = cursor.fetchall()

        branches = [row['branchname'] for row in result]

        cursor.close()
        conn.close()

        return jsonify({"branches": branches})
    except Exception as e:
        logging.error(f"Error in /branches: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/all-colleges', methods=['GET'])
def get_all_colleges():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM college_location")
        result = cursor.fetchall()
        logging.info(result)
        cursor.close()
        conn.close()
        
        return jsonify({"colleges": result})
    except Exception as e:
        logging.error(f"Error in /all-colleges: {str(e)}")
        return jsonify({"error": "An error occurred while fetching colleges."}), 500
@app.route('/filters', methods=['GET'])
def get_filters():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Fetch distinct districts
        cursor.execute("SELECT DISTINCT college_district FROM college_location")
        districts = [row['college_district'] for row in cursor.fetchall()] or []

        # Fetch distinct college codes
        cursor.execute("SELECT DISTINCT code FROM college_location")
        college_codes = [row['code'] for row in cursor.fetchall()] or []

        cursor.close()
        conn.close()

        return jsonify({"districts": districts, "college_codes": college_codes})
    except Exception as e:
        logging.error(f"Error in /filters: {str(e)}")
        return jsonify({"districts": [], "college_codes": [], "error": str(e)}), 500

@app.route('/register', methods=['POST'])
def register_user():
    try:
        data = request.json
        name = data.get('name')
        age = data.get('age')
        gender = data.get('gender')
        school = data.get('school')
        dob = data.get('dob')
        mobile = data.get('mobile')
        email = data.get('email')

        logging.info(f"Data Received: {data}")

        if not all([name, age, gender, school, dob, mobile, email]):
            return jsonify({'error': 'Missing required fields'}), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        query = "INSERT INTO users (name, age, gender, school, dob, mobile, email) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        values = (name, age, gender, school, dob, mobile, email)

        cursor.execute(query, values)
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({'message': 'User registered successfully'}), 201

    except Exception as e:
        logging.error(f"Error in /register: {str(e)}")
        return jsonify({'error': str(e)}), 500
@app.route("/")
def home():
    return "✅ Flask App is Running on Clever Cloud!", 200

if __name__ == '__main__':
    (app.run(debug=True))