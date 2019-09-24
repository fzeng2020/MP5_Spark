from pyspark import *
from pyspark.sql import SparkSession
from graphframes import *

sc = SparkContext()
spark = SparkSession.builder.appName('fun').getOrCreate()

#https://docs.databricks.com/_static/notebooks/graphframes-user-guide-py.html
def get_shortest_distances(graphframe, dst_id):
    # TODO
    # Find shortest distances in the given graphframe to the vertex which has id `dst_id`
    # The result is a dictionary where key is a vertex id and the corresponding value is
    # the distance of this node to vertex `dst_id`.
    diction = {}
    frame = graphframe.shortestPaths(landmarks=[dst_id])
    for row in frame.collect():
        if len(row.distances) == 0:
            diction[row.id] = -1
        else:
            diction[row.id] = row.distances[dst_id]
    return diction
# https://developmentality.wordpress.com/2012/03/30/three-ways-of-creating-dictionaries-in-python/
if __name__ == "__main__":
    vertex_list = []
    edge_list = []
    with open('dataset/graph.data') as f:
        for line in f:
            data = line.split()
            # TODO: Parse line to get vertex id
            src = data[0]
            # TODO: Parse line to get ids of vertices that src is connected to
            dst_list = data[1:]
            vertex_list.append([src])
            edge_list += [(src, dst) for dst in dst_list]

    vertices = spark.createDataFrame(vertex_list, ['id'])  # TODO: Create vertices dataframe
    edges = spark.createDataFrame(edge_list, ['src', 'dst']) # TODO: Create dataframe for edges

    g = GraphFrame(vertices, edges)
    sc.setCheckpointDir("/tmp/shortest-paths")

    # We want the shortest distance from every vertex to vertex 1
    for k, v in get_shortest_distances(g, '1').items():
        print(k, v)
