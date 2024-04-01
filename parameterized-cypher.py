from dataclasses import dataclass, field
import csv
import json
import os
import pathlib
import sys
import typing

from icecream import ic
from tqdm import tqdm
import dotenv
import matplotlib.pyplot as plt
import neo4j
import pandas as pd
import seaborn as sns

dotenv.load_dotenv(dotenv.find_dotenv())

bolt_uri: str = os.environ.get("NEO4J_BOLT")
username: str = os.environ.get("NEO4J_USER")
password: str = os.environ.get("NEO4J_PASS")

print(f"Connecting to {bolt_uri} as {username}")

driver: neo4j.BoltDriver = neo4j.GraphDatabase.driver(
    bolt_uri,
    auth = ( username, password, ),
)

records, summary, keys = driver.execute_query("RETURN true as ready")

print(f"Neo4j is ready: {records[0].data()['ready']}")

example_data = [
    {
        "data_source": "A",
        "record_id": "1",
        "name": "Alice",
        "age": 30,
    },
    {
        "data_source": "A",
        "record_id": "2",
        "name": "Bob",
        "age": 25,
    },
    {
        "data_source": "B",
        "record_id": "3",
        "name": "Charlie",
        "age": 35,
    },
    {
        "data_source": "B",
        "record_id": "4",
        "name": "David",
        "age": 40,
    },
]

with driver.session() as session:
    for record in example_data:
      session.run("""
        WITH toUpper($input.data_source) + "." + toString($input.record_id) as uid
        MERGE (rec:Record { uid: uid })
          ON CREATE SET rec += $input  
        RETURN rec.data_source, rec.record_id                
        """, 
        input = record 
      )