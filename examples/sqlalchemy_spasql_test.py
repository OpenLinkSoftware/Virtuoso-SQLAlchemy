# This SPASQL example leverages Virtuoso's combined support of both SQL and SPARQL which enables SQL queries against any Knowledge Graph 
# accessible via SPARQL. This example uses the popular SPARQL endpoint for the DBpedia Knowledge Graph 

import sqlalchemy as db
import pandas as pd

engine = db.create_engine('virtuoso+pyodbc://demo:demo@New Demo Server')

connection = engine.connect()
df = pd.read_sql_query("SELECT TOP 10 movie FROM (SPARQL PREFIX dbr: <http://dbpedia.org/resource/> PREFIX dbo: <http://dbpedia.org/ontology/> SELECT ?movie WHERE {SERVICE <http://dbpedia.org/sparql> {?movie rdf:type dbo:Film ; dbo:director dbr:Spike_Lee . }}) AS movies ", connection)
print(df)
