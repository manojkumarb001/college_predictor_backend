import mysql.connector
import pandas as pd
from mysql.connector import Error

def get_db_connection():
    """Establish and return a database connection."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",  # Update with your MySQL username
            password="manoj",  # Update with your MySQL password
            database="college_predictor"
        )
        if connection.is_connected():
            print("✅ Successfully connected to the database.")
            return connection
    except Error as e:
        print(f"❌ Error while connecting to MySQL: {e}")
        return None

def populate_colleges():
    """Populate the 'colleges' and 'college_location' tables with data from Excel files."""
    conn = get_db_connection()
    if conn is None:
        return  # Stop if no connection could be established

    cursor = conn.cursor()

    # File paths for all Excel data
    file_paths = [
    r"C:\Users\manoj\Prodigy_Infotech_Internship\College predictor\clg predictor 3.0\college-predictor\backend\static\college_data\Round1.xlsx",
    r"C:\Users\manoj\Prodigy_Infotech_Internship\College predictor\clg predictor 3.0\college-predictor\backend\static\college_data\Round2.xlsx",
    r"C:\Users\manoj\Prodigy_Infotech_Internship\College predictor\clg predictor 3.0\college-predictor\backend\static\college_data\Round3.xlsx",
    r"C:\Users\manoj\Prodigy_Infotech_Internship\College predictor\clg predictor 3.0\college-predictor\backend\static\college_data\college_location.xlsx"  # New file path for college_location
]


    # Process each Excel file
    for file_path in file_paths:
        try:
            df = pd.read_excel(file_path)

            # Clean column names by removing spaces and newlines
            df.columns = df.columns.str.upper().str.replace(' ', '_').str.replace('\n', '')

            print(f"📂 Processing file: {file_path}")
            print("🔹 Cleaned Columns:", df.columns)  # Print cleaned column names

            # Check for data from college_location.xlsx file
            if 'CODE' in df.columns and 'COLLEGE_NAME' in df.columns and 'COLLEGE_DISTRICT' in df.columns:
                print(f"🔹 'college_location.xlsx' file found and processing")
                for _, row in df.iterrows():
                    college_code = row['CODE']
                    college_name = row['COLLEGE_NAME']
                    college_district = row['COLLEGE_DISTRICT']

                    # SQL Query to Insert Data into college_location table
                    sql = """INSERT INTO college_location (code, college_name, college_district)
                             VALUES (%s, %s, %s)"""
                    cursor.execute(sql, (college_code, college_name, college_district))

            else:
                # Check if 'AGGRMARK' exists in the file (for colleges data)
                if 'AGGRMARK' in df.columns:
                    print(f"🔹 'AGGRMARK' column found in file: {file_path}")
                    for _, row in df.iterrows():
                        # Validate cutoff value
                        average_cutoff_value = validate_cutoff(row['AGGRMARK'])

                        if average_cutoff_value is not None:
                            # Ensure COLLEGENAME is a valid string
                            college_name = row['COLLEGENAME']
                            if isinstance(college_name, str) and college_name.strip():  # Check if COLLEGENAME is valid
                                district_value = row['DISTRICT'] if isinstance(row['DISTRICT'], str) else "Unknown"
                            else:
                                district_value = "Unknown"  # Set to "Unknown" if COLLEGENAME is invalid

                            # SQL Query to Insert Data into colleges table
                            sql = """INSERT INTO colleges (college_name, branch, branch_code, community, average_cutoff, district, college_code)
                                     VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                            cursor.execute(sql, (college_name, row['BRANCHCODE'], row['COLLEGECODE'],
                                                 row['COMMUNITY'], average_cutoff_value, district_value, row['COLLEGECODE']))
                        else:
                            print(f"❌ Invalid AGGRMARK value for APPLNNO {row['APPLNNO']}")
                    print(f"✅ Data from {file_path} inserted successfully.")
                else:
                    print(f"❌ Column mismatch or invalid data in file: {file_path}")
                    continue

        except Exception as e:
            print(f"❌ Error processing file {file_path}: {e}")
            continue

    conn.commit()  # Commit all changes
    conn.close()   # Close database connection
    print("🎉 Database updated successfully!")

def validate_cutoff(value):
    """Validate and clean cutoff values."""
    try:
        return float(value) if pd.notnull(value) else None
    except ValueError:
        return None  # Return None if conversion fails

if __name__ == "__main__":
    populate_colleges()
