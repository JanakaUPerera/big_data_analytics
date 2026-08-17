// Verify Neo4j can read the generated CSV
LOAD CSV WITH HEADERS
FROM 'file:///patent_citations_5000.csv' AS row
RETURN row.source_patent AS source_patent,
       row.cited_patent AS cited_patent
LIMIT 5;

// Create a uniqueness constraint for Patent IDs
CREATE CONSTRAINT patent_id_unique IF NOT EXISTS
FOR (p:Patent)
REQUIRE p.id IS UNIQUE;

// Import the Patent nodes
LOAD CSV WITH HEADERS
FROM 'file:///patent_citations_5000.csv' AS row

UNWIND [row.source_patent, row.cited_patent] AS patent_id

WITH DISTINCT patent_id

MERGE (:Patent {id: patent_id});

// Verify how many Patent nodes were created
MATCH (p:Patent)
RETURN count(p) AS total_patents;

// Create the citation relationships
LOAD CSV WITH HEADERS
FROM 'file:///patent_citations_5000.csv' AS row

MATCH (source:Patent {id: row.source_patent})
MATCH (cited:Patent {id: row.cited_patent})

MERGE (source)-[:CITES]->(cited);

// Verify the number of CITES relationships
MATCH ()-[r:CITES]->()
RETURN count(r) AS total_citations;

// Visually inspect part of the graph
MATCH (source:Patent)-[r:CITES]->(cited:Patent)
RETURN source, r, cited
LIMIT 100;

// Find the patents with the highest out-degree
MATCH (p:Patent)-[:CITES]->(cited:Patent)
RETURN p.id AS patent_id,
       count(cited) AS out_degree
ORDER BY out_degree DESC
LIMIT 10;

// Find the patents with the highest in-degree
MATCH (source:Patent)-[:CITES]->(p:Patent)
RETURN p.id AS patent_id,
       count(source) AS in_degree
ORDER BY in_degree DESC
LIMIT 10;

// Find patents that both cite and are cited
MATCH (p:Patent)
OPTIONAL MATCH (p)-[:CITES]->(out:Patent)
WITH p, count(DISTINCT out) AS out_degree
OPTIONAL MATCH (in:Patent)-[:CITES]->(p)
WITH p, out_degree, count(DISTINCT in) AS in_degree
WHERE out_degree > 0 AND in_degree > 0
RETURN p.id AS patent_id,
       in_degree,
       out_degree
ORDER BY (in_degree + out_degree) DESC
LIMIT 10;

// Verify whether there is any overlap at all
MATCH (p:Patent)
WHERE EXISTS {
    MATCH (p)-[:CITES]->()
}
AND EXISTS {
    MATCH ()-[:CITES]->(p)
}
RETURN count(p) AS patents_with_both_directions;

// Count source-only and cited-only patents
MATCH (p:Patent)
WITH p,
     EXISTS { MATCH (p)-[:CITES]->() } AS has_outgoing,
     EXISTS { MATCH ()-[:CITES]->(p) } AS has_incoming
RETURN
    sum(CASE WHEN has_outgoing AND NOT has_incoming THEN 1 ELSE 0 END) AS source_only_patents,
    sum(CASE WHEN has_incoming AND NOT has_outgoing THEN 1 ELSE 0 END) AS cited_only_patents;

// Count isolated patents
MATCH (p:Patent)
WHERE NOT (p)-[:CITES]-()
RETURN count(p) AS isolated_patents;

// Find the overall most connected patents
MATCH (p:Patent)
OPTIONAL MATCH (p)-[:CITES]->(out:Patent)
WITH p, count(DISTINCT out) AS out_degree
OPTIONAL MATCH (in:Patent)-[:CITES]->(p)
WITH p, out_degree, count(DISTINCT in) AS in_degree
RETURN p.id AS patent_id,
       in_degree,
       out_degree,
       (in_degree + out_degree) AS total_degree
ORDER BY total_degree DESC
LIMIT 10;

// Find a sample citation path of length 2
MATCH (a:Patent)-[:CITES]->(b:Patent)-[:CITES]->(c:Patent)
RETURN a.id AS source_patent,
       b.id AS middle_patent,
       c.id AS destination_patent
LIMIT 10;

// Calculate basic graph statistics
MATCH (p:Patent)
WITH count(p) AS node_count
MATCH ()-[r:CITES]->()
WITH node_count, count(r) AS relationship_count
RETURN
    node_count,
    relationship_count,
    toFloat(relationship_count) / node_count AS avg_relationships_per_node;

// Calculate graph density
MATCH (p:Patent)
WITH count(p) AS n
MATCH ()-[r:CITES]->()
WITH n, count(r) AS m
RETURN
    n AS node_count,
    m AS relationship_count,
    toFloat(m) / (n * (n - 1)) AS graph_density;

// Calculate average in-degree and out-degree
MATCH (p:Patent)
OPTIONAL MATCH (p)-[:CITES]->(out:Patent)
WITH p, count(out) AS out_degree
OPTIONAL MATCH (in:Patent)-[:CITES]->(p)
WITH p, out_degree, count(in) AS in_degree
RETURN
    avg(toFloat(out_degree)) AS average_out_degree,
    avg(toFloat(in_degree)) AS average_in_degree;

// Analyse the out-degree distribution
MATCH (p:Patent)
OPTIONAL MATCH (p)-[:CITES]->(cited:Patent)
WITH p, count(cited) AS out_degree
RETURN
    out_degree,
    count(p) AS patent_count
ORDER BY out_degree ASC;

// Verify the highest out-degree distribution values
MATCH (p:Patent)
OPTIONAL MATCH (p)-[:CITES]->(cited:Patent)
WITH p, count(cited) AS out_degree
WITH out_degree, count(p) AS patent_count
RETURN out_degree, patent_count
ORDER BY out_degree DESC
LIMIT 10;

// Analyse the in-degree distribution
MATCH (p:Patent)
OPTIONAL MATCH (source:Patent)-[:CITES]->(p)
WITH p, count(source) AS in_degree
WITH in_degree, count(p) AS patent_count
RETURN in_degree, patent_count
ORDER BY in_degree DESC;