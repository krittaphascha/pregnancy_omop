import os
import pyodbc
from dotenv import load_dotenv
import sqlalchemy as sal
import urllib

load_dotenv()

def get_connection():

    server_name = os.getenv('SERVER_NAME')
    database_name = os.getenv('DATABASE_NAME')
    username = os.getenv('PANDA_USERNAME')
    password = os.getenv('PANDA_PASSWORD')
    password = urllib.parse.quote_plus(password)

    engine = sal.create_engine(f'mssql+pyodbc://{username}:{password}@{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server')
    conn = engine.connect()

    return conn