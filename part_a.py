
from pyspark import *
from pyspark.sql import SparkSession
from graphframes import *

sc = SparkContext()
spark = SparkSession.builder.appName('fun').getOrCreate()


def get_connected_components(graphframe):

    result = graphframe.connectedComponents()
    result1 = result.select('component').distinct().collect()
    components =[(row.component) for row in result1]
    output = []
    for i in components:
            output += [[(row.id) for row in result.filter(result['component'] == i).collect()]]
        
    return output 


if __name__ == '__main__':
    vertex_list = []
    edge_list = []
    with open('dataset/graph.data') as f:  # Do not modify
        for line in f:
            data = line.split()
            src = data[0]  # TODO: Parse src from line
            dst_list = data[1:]  # TODO: Parse dst_list from line
            vertex_list.append((src,))
            edge_list += [(src, dst) for dst in dst_list]

    vertices = spark.createDataFrame(vertex_list, ['id'])  # TODO: Create vertices dataframe
    edges = spark.createDataFrame(edge_list, ['src', 'dst'])  # TODO: Create edges dataframe
    g = GraphFrame(vertices, edges)
    sc.setCheckpointDir('/tmp/connected-components')

    result = get_connected_components(g)


 
    for line in result:
        print (' '.join(line))	
