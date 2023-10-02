# List Spike Lee Movies from the DBpedia Knowledge Graph via its SPARQL Query Services endpoint.

import os
os.environ["OPENAI_API_KEY"] = "{YOUR-OPENAI-API-KEY}"

from langchain.chat_models import ChatOpenAI
from langchain.chains import GraphSparqlQAChain
from langchain.graphs import RdfGraph

graph = RdfGraph(query_endpoint="https://dbpedia.org/sparql")


chain = GraphSparqlQAChain.from_llm(
    ChatOpenAI(model="gpt-3.5-turbo", temperature=0, verbose=True), graph=graph, verbose=True
)

query = """
Relevant DBpedia Knowledge Graph relationship types (relations):
  ?movie rdf:type dbo:Film .
  ?movie dbo:director ?name .

Associated namespaces:
 dbr:  <http://dbpedia.org/resource/>
 dbo:  <http://dbpedia.org/ontology/>
 rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

List movies by Spike Lee
"""

res = chain.run(query)
print(res)
