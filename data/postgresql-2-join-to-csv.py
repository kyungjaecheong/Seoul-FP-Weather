import os
import sys
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv(verbose=True)

HOST = os.getenv('postgre_host')
PASSWORD = os.getenv('postgre_password')

DATABASE = 'postgredb'
USERNAME = 'kjcheong'
PORT = 5432

def main():
    # postgreSQL 연결
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USERNAME,
            password=PASSWORD)
        cur = conn.cursor()
        print('connection success to DB')
    except:
        print('connection failure to DB')
        sys.exit()
    
    # JOIN QUERY
    sql_query_join = """
    SELECT w."year" , w."month" , w.avgta , w.maxta , w.minta , w.sumrn , w.avgws , w.avgrhm , w.sumsshr , w.avgps , fs2.patient_count
    FROM weather w
    JOIN fp_seoul fs2
    ON w.month_id = fs2.month_id
    """
    
    # to csv
    sql_csv = f"""COPY ({sql_query_join}) TO STDOUT WITH CSV DELIMITER ',';"""
    with open("./data/csv/fp-weather.csv", "w") as cf:
        cur.copy_expert(sql_csv, cf)
    
    conn.close()
    
    print('"fp-weather.csv" file created')

if __name__ == "__main__":
    main()