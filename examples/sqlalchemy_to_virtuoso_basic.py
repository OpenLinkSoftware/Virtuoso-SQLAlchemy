# This example presumes the Northwind Database has been installed via demo_dav.vad package

import sqlalchemy as db
import pandas as pd

engine = db.create_engine('virtuoso+pyodbc://demo:demo@VOS')

connection = engine.connect()
df = pd.read_sql_query("SELECT TOP 10 * FROM Demo.demo.Customers ", connection)
print(df)
