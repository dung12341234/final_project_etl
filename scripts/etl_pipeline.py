import psycopg2
import pandas as pd
from datetime import datetime

def run_etl():
    conn = psycopg2.connect(
        host="postgres_container",
        port=5432,
        database="education_db",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    # Load CSV
    df = pd.read_csv("/opt/airflow/scripts/Students_adaptability_level_online_education.csv")

    # Step 1: Insert into raw_students (TEXT only)
    cur.execute("DELETE FROM raw_students;")
    for idx, row in df.iterrows():
        cur.execute("""
            INSERT INTO raw_students(
                student_id, gender, age, education_level, institution_type, it_student,
                location, load_shedding, financial_condition, internet_type,
                network_type, class_duration, self_lms, device, adaptivity_level
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            idx + 1,
            row['Gender'], row['Age'], row['Education Level'], row['Institution Type'],
            row['IT Student'],
            row['Location'], row['Load-shedding'], row['Financial Condition'],
            row['Internet Type'], row['Network Type'],
            row['Class Duration'],
            row['Self Lms'],
            row['Device'], row['Adaptivity Level']
        ))
    conn.commit()
    print("Step 1 complete: raw_students loaded")

    # Step 2: Insert into warehouse_students (convert + validate)
    cur.execute("DELETE FROM warehouse_students;")
    cur.execute("""
        INSERT INTO warehouse_students(
            student_id, gender, age, education_level, institution_type, it_student,
            location, load_shedding, financial_condition, internet_type,
            network_type, class_duration, self_lms, device, adaptivity_level, loaded_at
        )
        SELECT DISTINCT ON (student_id)
            student_id,
            gender,
            CASE 
                WHEN age IN ('16-20','21-25','26-30') THEN age
                ELSE NULL
            END,
            education_level,
            institution_type,
            CASE WHEN LOWER(it_student) = 'yes' THEN TRUE ELSE FALSE END,
            location,
            load_shedding,
            financial_condition,
            internet_type,
            network_type,
            CASE 
                WHEN class_duration ~ '^[0-9]{1,2}-[A-Za-z]{3}$' 
                THEN TO_DATE(class_duration, 'DD-Mon')
                ELSE NULL
            END,
            CASE WHEN LOWER(self_lms) = 'yes' THEN TRUE ELSE FALSE END,
            device,
            adaptivity_level,
            NOW()
        FROM raw_students
        WHERE gender IS NOT NULL;
    """)
    conn.commit()
    print("Step 2 complete: warehouse_students loaded")

    # Step 3: Log invalid rows into etl_import_error
    cur.execute("DELETE FROM etl_import_error;")
    cur.execute("""
        INSERT INTO etl_import_error(
            student_id, gender, age, education_level, institution_type, it_student,
            location, load_shedding, financial_condition, internet_type,
            network_type, class_duration, self_lms, device, adaptivity_level, error_reason
        )
        SELECT student_id, gender, age, education_level, institution_type, it_student,
               location, load_shedding, financial_condition, internet_type,
               network_type, class_duration, self_lms, device, adaptivity_level,
               'Invalid age or class_duration'
        FROM raw_students
        WHERE age NOT IN ('16-20','21-25','26-30')
           OR (class_duration !~ '^[0-9]{1,2}-[A-Za-z]{3}$' AND class_duration <> '');
    """)
    conn.commit()
    print("Step 3 complete: invalid rows logged in etl_import_error")

    # Step 4: Log ETL run
    cur.execute("""
        INSERT INTO etl_log(run_timestamp, rows_inserted, notes)
        VALUES (%s, %s, %s);
    """, (datetime.now(), len(df), 'ETL run completed successfully'))
    conn.commit()
    print("Step 4 complete: ETL run recorded in etl_log")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_etl()