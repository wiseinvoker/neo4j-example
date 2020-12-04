from neo4j import GraphDatabase
import csv

user = "neo4j"
password = "secret"

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(user, password))

# To see nodes and relationships
# MATCH (n) RETURN n LIMIT 25
# To remove all nodes and relationship
# MATCH (n) DETACH DELETE n

# Create Count
def create_count(tx, id, year, mon, india, us, au, france, london, grand_total):
    tx.run("MERGE (y:Year {year: $year}) MERGE (c:Count {_id: $id, name: $mon, _mon: $mon, _India: $india, _US: $us, _Australia: $au, _France: $france, _London: $london, _GrandTotal: $grand_total}) MERGE (c)-[:CREATED_AT]->(y)", id=id, mon=mon, india=india, us=us, au=au, france=france, london=london, grand_total=grand_total, year=year)

#
# USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///count.csv" AS csvLine
MERGE (y:Year {year: csvLine.YEAR})
MERGE (c:Count {_id: csvLine.id, _mon: csvLine.MONTH, _India: csvLine.INDIA, _US: csvLine.US, _Australia: csvLine.AUSTRALIA, _France: csvLine.FRANCE, _London: csvLine.LONDON, _GrandTotal: toInteger(csvLine.INDIA)+toInteger(csvLine.US)+toInteger(csvLine.AUSTRALIA)+toInteger(csvLine.FRANCE)+toInteger(csvLine.LONDON)})
MERGE (c)-[:CREATED_AT]->(y)

# 
# 
#  #


# Create Stage
def create_stage(tx, id, year, mon, india, us, au, france, london, grand_total):
    tx.run("MERGE (y:Year {year: $year}) MERGE (c:Stage {_id: $id, name: $mon, _mon: $mon, _India: $india, _US: $us, _Australia: $au, _France: $france, _London: $london, _GrandTotal: $grand_total}) MERGE (c)-[:CREATED_AT]->(y)", id=id, mon=mon, india=india, us=us, au=au, france=france, london=london, grand_total=grand_total, year=year)


# To import and create Counts
with open('count.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        grand_total = int(row["INDIA"]) + int(row["US"]) + int(row["AUSTRALIA"]) + int(row["FRANCE"]) + int(row["LONDON"])
        with driver.session() as session:
            session.write_transaction(create_count, row['id'], row['YEAR'], row['MONTH'], row["INDIA"], row["US"], row["AUSTRALIA"], row["FRANCE"], row["LONDON"], grand_total)


# To import and create Stages
with open('stage.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        grand_total = int(row["INDIA"]) + int(row["US"]) + int(row["AUSTRALIA"]) + int(row["FRANCE"]) + int(row["LONDON"])
        with driver.session() as session:
            session.write_transaction(create_stage, row['id'], row['YEAR'], row['MONTH'], row["INDIA"], row["US"], row["AUSTRALIA"], row["FRANCE"], row["LONDON"], grand_total)


driver.close()