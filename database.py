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
            print("Successfully connected to the database.")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

def validate_cutoff(value):
    """Validate and clean cutoff values."""
    try:
        # Remove non-numeric characters or handle NaNs
        return float(value) if pd.notnull(value) else None
    except ValueError:
        return None  # Return None if the value cannot be converted

def populate_colleges():
    """Populate the 'colleges' table with data from Excel files."""
    conn = get_db_connection()
    if conn is None:
        return  # Stop if no connection could be established

    cursor = conn.cursor()

    # Absolute file paths
    file_paths = [
        r"C:\Users\manoj\Prodigy_Infotech_Internship\College predictor\clg predictor 3.0\college-predictor\backend\static\college_data\Round1.xlsx",
        r"C:\Users\manoj\Prodigy_Infotech_Internship\College predictor\clg predictor 3.0\college-predictor\backend\static\college_data\Round2.xlsx",
        r"C:\Users\manoj\Prodigy_Infotech_Internship\College predictor\clg predictor 3.0\college-predictor\backend\static\college_data\Round3.xlsx"
    ]

    # Insert data into the 'colleges' table
    for file_path in file_paths:
        try:
            df = pd.read_excel(file_path)

            # Clean column names: remove quotes, spaces, and special characters
            df.columns = df.columns.str.replace('"', '').str.strip().str.replace(' ', '_').str.replace('\n', '')

            print("Cleaned Columns in the file:", df.columns)  # Print cleaned column names

            for _, row in df.iterrows():
                # Calculate the average cutoff (assuming AGGRMARK represents the cutoff)
                average_cutoff_value = validate_cutoff(row['AGGRMARK'])

                if average_cutoff_value is not None:  # Only proceed if the value is valid
                    # Prepare and execute the SQL statement to insert data with average_cutoff
                    sql = """INSERT INTO colleges (college_name, branch, branch_code, community, average_cutoff)
                             VALUES (%s, %s, %s, %s, %s)"""
                    cursor.execute(sql, (row['COLLEGE_NAME'], row['BRANCHCODE'], row['COLLEGECODE'],
                                         row['COMMUNITY'], average_cutoff_value))

            print(f"Data from {file_path} inserted successfully.")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue

    conn.commit()  # Commit all changes to the database
    conn.close()   # Close the database connection
    print("Database populated successfully!")

if __name__ == "__main__":
    populate_colleges()
