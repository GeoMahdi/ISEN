# importing required py2neo objects
from py2neo import Graph, NodeMatcher
graph = Graph(user='neo4j', password='12345',port = 11006, scheme='bolt', host='localhost')
# defining the node matcher object
matcher = NodeMatcher(graph)
# getting user nodes using node matcher
placeNodes = matcher.match("Place")
deletePPPCypherQuery = 'MATCH (p1:Place)-[s:PPP]-(p2:Place)' \
                              'DELETE s'
graph.run(deletePPPCypherQuery)
for place in placeNodes:
    dimensionQuery0 = 'MATCH (p1:Place)' \
                      'SET p1.pppdim = 0'
    graph.run(dimensionQuery0)
for place in placeNodes:

    dimensionQuery1 = 'MATCH (p1:Place)-[r:ACCESS]-(p2:Place)' \
                       'WHERE p1.name = "{}"' \
                       'RETURN COUNT( DISTINCT(p2))'.format(dict(place)["name"])
    print(dimensionQuery1)
    dimension = graph.evaluate(dimensionQuery1)
    dimensionQuery2='MATCH (p1:Place)' \
                     'WHERE p1.name = "{}"' \
                     'SET p1.pppdim = {}'.format(dict(place)["name"],dimension)
    print(dimensionQuery2)
    graph.run(dimensionQuery2)
