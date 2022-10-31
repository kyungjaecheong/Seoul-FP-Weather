import os
import sys
import csv
import json
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv(verbose=True)

HOST = os.getenv('postgre_host')
PASSWORD = os.getenv('postgre_password')

DATABASE = 'postgredb'
USERNAME = 'kjcheong'
PORT = 5432

KEY = os.getenv('fp_api_key')
REGION = "서울"
API_URL = f'http://openapi.foodsafetykorea.go.kr/api/{KEY}/I2848/json/1/999/OCCRNC_AREA={REGION}'

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
    
    # 테이블 초기화
    cur.execute("""DROP TABLE IF EXISTS fp_seoul;""")
    cur.execute("""DROP TABLE IF EXISTS weather;""")
    cur.execute("""DROP TABLE IF EXISTS w_avg_month;""")
    cur.execute("""DROP TABLE IF EXISTS w_avg_season;""")
    print('4 table dropped')
    
    # 테이블 생성
    sql_create_table_asv = """CREATE TABLE IF NOT EXISTS w_avg_season (
        season VARCHAR(8) NOT NULL,
        avgTa FLOAT,
        maxTa FLOAT,
        minTa FLOAT,
        sumRn FLOAT,
        avgWs FLOAT,
        avgRhm FLOAT,
        sumSsHr FLOAT,
        avgPs FLOAT,
        CONSTRAINT w_avg_season_PK PRIMARY KEY (season)
        );"""
    sql_create_table_ayv = """CREATE TABLE IF NOT EXISTS w_avg_month (
        month VARCHAR(2) NOT NULL,
        season VARCHAR(8) NOT NULL,
        avgTa FLOAT,
        maxTa FLOAT,
        minTa FLOAT,
        sumRn FLOAT,
        avgWs FLOAT,
        avgRhm FLOAT,
        sumSsHr FLOAT,
        avgPs FLOAT,
        CONSTRAINT w_avg_month_PK PRIMARY KEY (month),
        CONSTRAINT w_avg_month_FK FOREIGN KEY (season) REFERENCES w_avg_season (season)
        );"""
    sql_create_table_w = """CREATE TABLE IF NOT EXISTS weather (
        month_id DATE NOT NULL,
        year VARCHAR(4) NOT NULL,
        month VARCHAR(2) NOT NULL,
        avgTa FLOAT,
        maxTa FLOAT,
        minTa FLOAT,
        sumRn FLOAT,
        avgWs FLOAT,
        avgRhm FLOAT,
        sumSsHr FLOAT,
        avgPs FLOAT,
        CONSTRAINT weather_PK PRIMARY KEY (month_id),
        CONSTRAINT weather_FK FOREIGN KEY (month) REFERENCES w_avg_month (month)        
        );"""
    sql_create_table_fp = """CREATE TABLE IF NOT EXISTS fp_seoul (
        month_id DATE NOT NULL,
        patient_count INTEGER,
        CONSTRAINT fp_seoul_PK PRIMARY KEY (month_id),
        CONSTRAINT fp_seoul_FK FOREIGN KEY (month_id) REFERENCES weather (month_id)
        );"""
    
    
    
    cur.execute(sql_create_table_asv)
    cur.execute(sql_create_table_ayv)
    cur.execute(sql_create_table_w)
    cur.execute(sql_create_table_fp)
    
    print('4 table created')
    
    # csv file로부터 데이터 추출
    with open('./data/csv/seoul-weather-month.csv','r') as cf:
        csv_reader = csv.reader(cf)
        next(csv_reader)
        data_w = list(csv_reader)
    
    # api data 수집
    req = requests.get(API_URL)
    raw_data = json.loads(req.text)    
    
    fp_raw = raw_data['I2848']['row']
    data_fp = []
    for row in fp_raw:
        if row["OCCRNC_YEAR"] == "2022":
            continue
        else:
            y_m = row["OCCRNC_YEAR"]+"-"+row["OCCRNC_MM"]
            p_c = row["PATNT_CNT"]
            data_fp.append([y_m,p_c])
    
    # 월별평년값 데이터 추출
    with open('./data/csv/seoul-average-month.csv','r') as cf:
        csv_reader = csv.reader(cf)
        next(csv_reader)
        data_ayv = list(csv_reader)
    # 계절평균값 데이터 추출
    with open('./data/csv/seoul-average-season.csv','r') as cf:
        csv_reader = csv.reader(cf)
        next(csv_reader)
        data_asv = list(csv_reader)
    
    #평년값 데이터 삽입 명령어
    sql_insert_asv = """INSERT INTO w_avg_season 
        (season, avgTa, maxTa, minTa, sumRn, avgWs, avgRhm, sumSsHr, avgPs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    sql_insert_ayv = """INSERT INTO w_avg_month 
        (month, season, avgTa, maxTa, minTa, sumRn, avgWs, avgRhm, sumSsHr, avgPs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    
    # 테이블에 데이터 삽입 명령어
    sql_insert_w = """INSERT INTO weather 
        (month_id, year, month, avgTa, maxTa, minTa, sumRn, avgWs, avgRhm, sumSsHr, avgPs)
        VALUES (TO_DATE(%s,'YYYY.MM'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    sql_insert_fp = """INSERT INTO fp_seoul(month_id, patient_count) VALUES (TO_DATE(%s,'YYYY.MM'), %s);"""
    
    
    cur.executemany(sql_insert_asv, data_asv)
    cur.executemany(sql_insert_ayv, data_ayv)
    cur.executemany(sql_insert_w, data_w)
    cur.executemany(sql_insert_fp, data_fp)
           
    conn.commit()
    conn.close()
    
    print('data inserted to DB')
    
if __name__ == "__main__":
    main()