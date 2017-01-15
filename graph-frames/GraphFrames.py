# Tested on graphframes-0.1.0 and pyspark-1.6
# Rajdeep : Tested on graphframes-0.3.0 and pyspark-2.0
# https://graphframes.github.io/api/python/graphframes.html#
# https://graphframes.github.io/user-guide.html
# pyspark --packages graphframes:graphframes:0.1.0-spark1.6
# pyspark --packages graphframes:graphframes:0.3.0-spark2.0-s_2.11

# Import graphFrames 
from graphframes import *

# Create a Vertex DataFrame with "id" "name" "age"
users = sqlContext.createDataFrame([("a", "Alice", 34),("b", "Bob", 36),("c", "Charlie", 30),("d", "David", 29),("e", "Esther", 32),("f", "Fanny", 36),("g", "Gabby", 60)], ["id", "name", "age"])

# Create an Edge DataFrame with "src" "dst" "relationship" columns
followers = sqlContext.createDataFrame([("a", "b", "friend"),("b", "c", "follow"),("c", "b", "follow"),("f", "c", "follow"), ("e", "f", "follow"),("e", "d", "friend"),("d", "a", "friend"),("a", "e", "friend")], ["src", "dst", "relationship"])

# Create a GraphFrame
socialGraph = GraphFrame(users, followers)

# Graph Properties
socialGraph.edges

socialGraph.vertices

socialGraph.degrees

socialGraph.inDegrees.show()

#
# +---+--------+                                                                  
# | id|inDegree|
# +---+--------+
# |  f|       1|
# |  e|       1|
# |  d|       1|
# |  c|       2|
# |  b|       2|
# |  a|       1|
# +---+--------+
#

socialGraph.outDegrees.show()

# +---+---------+
# | id|outDegree|
# +---+---------+
# |  f|        1|
# |  e|        2|
# |  d|        1|
# |  c|        1|
# |  b|        1|
# |  a|        2|
# +---+---------+


# Find the youngest user's age in the graph.
# This queries the vertex DataFrame.
socialGraph.vertices.groupBy().max("age").show()

# +--------+                                                                      
# |max(age)|
# +--------+
# |      60|
# +--------+


# Count the number of "follows" in the graph.
# This queries the edge DataFrame.
socialGraph.edges.filter("relationship = 'follow'").count()
# 4

# Motif finding
motifs = socialGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

# +--------------+------------+--------------+------------+
# |             a|           e|             b|          e2|
# +--------------+------------+--------------+------------+
# |[c,Charlie,30]|[c,b,follow]|    [b,Bob,36]|[b,c,follow]|
# |    [b,Bob,36]|[b,c,follow]|[c,Charlie,30]|[c,b,follow]|
# +--------------+------------+--------------+------------+

# Joins
# Inner join
users.filter(users.age > 35).join(followers, users.id == followers.src).show()

# Subgraph
v2 = socialGraph.vertices.filter("age > 30")
e2 = socialGraph.edges.filter("relationship = 'follow'")
# Build the subgraph
g2 = GraphFrame(v2, e2)

# Aggregates
# Find the age of the oldest user
users.agg({"age": "max"}).collect() # 60
# Find the youngest user
users.agg({"age": "min"}).collect() # 29
# Find the average age of the user in social network
users.agg({"age": "avg"}).collect() # 36.71

# Breadth First Search
# Search from "Esther" for users of age < 34
paths = socialGraph.bfs("name = 'Esther'", "age < 34")
paths.show()

# Specify edge filters or max path lengths.
socialGraph.bfs("name = 'Esther'", "age < 32",edgeFilter="relationship != 'follow'", maxPathLength=3).show()

# Label Propogation algorithm
result = socialGraph.labelPropagation(maxIter=4)
result.select("id", "label").show()

# Finding the shortest path for given vertex id
shortPath = socialGraph.shortestPaths(landmarks=[2, 4])
shortPath.show()

# Run PageRank algorithm, and show results.
pageRank1 = socialGraph.pageRank(resetProbability=0.10, maxIter=5)
pageRank1.vertices.show()
pageRank1.edges.show()

# vertices
# +---+-------+---+-------------------+
# | id|   name|age|           pagerank|
# +---+-------+---+-------------------+
# |  a|  Alice| 34|0.27250862500000006|
# |  g|  Gabby| 60|                0.1|
# |  e| Esther| 32|0.21930737500000003|
# |  c|Charlie| 30| 0.9836470000000002|
# |  b|    Bob| 36| 0.9451573749999999|
# |  f|  Fanny| 36|0.19536681250000004|
# |  d|  David| 29|0.19536681250000004|
# +---+-------+---+-------------------+

# edges
# +---+---+------------+------+                                                   
# |src|dst|relationship|weight|
# +---+---+------------+------+
# |  f|  c|      follow|   1.0|
# |  c|  b|      follow|   1.0|
# |  a|  b|      friend|   0.5|
# |  b|  c|      follow|   1.0|
# |  a|  e|      friend|   0.5|
# |  e|  f|      follow|   0.5|
# |  d|  a|      friend|   1.0|
# |  e|  d|      friend|   0.5|
# +---+---+------------+------+



# Run PageRank until convergence to tolerance "tol".
pageRank2 = socialGraph.pageRank(resetProbability=0.10, tol=0.01)
pageRank2.vertices.show()
pageRank2.edges.show()
