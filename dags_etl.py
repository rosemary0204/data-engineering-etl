!pip install sqlalchemy==1.3.2

%load_ext sql

%sql postgresql://jeony325:Jeony325!1@learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/prod
      
%%sql

DROP TABLE IF EXISTS jeony325.name_gender;
CREATE TABLE jeony325.name_gender (
   name varchar(32),
   gender varchar(8)
);


import psycopg2

# Redshift connection 함수
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "jeony325"
    redshift_pass = "Jeony325!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()
  
  
import requests

def extract(url):
    f = requests.get(link)
    return (f.text)
  
def transform(text):
    lines = text.split("\n")[1:]
    return lines
  
def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('jeony325', 'MALE');....;END;
    cur = get_Redshift_connection()
    cur.execute("BEGIN;DELETE FROM jeony325.name_gender;")
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO jeony325.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
            print(sql)
            cur.execute(sql)
    cur.execute("end;")
    
    
link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"

data = extract(link)


lines = transform(data)

load(lines)
