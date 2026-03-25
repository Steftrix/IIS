In folderu neo4j/import sa aveti csv urile la produs si order line
In neo4j browser:
1.

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row RETURN row",
  "
    MERGE (p:Product {id: row.id})
    SET p.name = row.name
  ",
  {batchSize: 10000, parallel: false}
);

2. 

CREATE CONSTRAINT product_id IF NOT EXISTS
FOR (p:Product) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT order_id IF NOT EXISTS
FOR (o:Order) REQUIRE o.id IS UNIQUE;

3. 

CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///order_items.csv' AS row RETURN row",
  "
    MERGE (o:Order {id: row.order_id})
    WITH o, row
    MATCH (p:Product {id: row.product_id})
    MERGE (o)-[:CONTAINS]->(p)
  ",
  {batchSize: 10000, parallel: true}
);

4. 

CALL apoc.periodic.iterate(
  "
    MATCH (o:Order)
    RETURN o
  ",
  "
    MATCH (o)-[:CONTAINS]->(p:Product)
    WITH o, collect(p) AS products

    UNWIND range(0, size(products)-1) AS i
    UNWIND range(i+1, size(products)-1) AS j

    WITH products[i] AS p1, products[j] AS p2
    MERGE (p1)-[r:BOUGHT_WITH]->(p2)
    ON CREATE SET r.co_purchase_count = 1
    ON MATCH SET r.co_purchase_count = r.co_purchase_count + 1
  ",
  {batchSize: 500, parallel: false}
);
