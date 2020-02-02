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
maxqnearness = -1
for place1 in placeNodes:
    for place2 in placeNodes:
        if place1 != place2:
            qNearnessCypherQuery = 'MATCH (place1:Place)-[r:ACCESS]-(p:Place)-[s:ACCESS]-(place2:Place) ' \
                                   'Where place1.name = "{}" AND place2.name = "{}" ' \
                                   'return COUNT (DISTINCT p)'.format(dict(place1)["name"], dict(place2)["name"])
            qnearness = graph.run(qNearnessCypherQuery).data()[0]['COUNT (DISTINCT p)']-1
            if qnearness > maxqnearness:
                maxqnearness = qnearness
            print(qnearness)
            createQNearGraphCypherQuery = 'MATCH (place1:Place), (place2:Place) ' \
                                          'Where place1.name = "{}" AND place2.name = "{}" ' \
                                          'MERGE (place1)-[r:PPP]->(place2)' \
                                          'SET r.qnear={}, r.maxqnear={}'.format(dict(place1)["name"], dict(place2)["name"], qnearness, maxqnearness)
            print(createQNearGraphCypherQuery)
            graph.run(createQNearGraphCypherQuery)

placeNodes_ = matcher.match("Place")
for place_1 in placeNodes_:
    place_1_dimension = graph.evaluate('MATCH (p:Place)'
                                      'WHERE p.name = "{}"'
                                      'return p.pppdim'.format(dict(place_1)["name"]))
    for place_2 in placeNodes_:
        place_2_dimension = graph.evaluate('MATCH (p:Place)'
                                          'WHERE p.name = "{}"'
                                          'return p.pppdim'.format(dict(place_2)["name"]))
        if place_1 != place_2:
            print(dict(place_1)["name"], dict(place_2)["name"])
            qmax = maxqnearness
            while qmax > -1:
                print('qmax = {}'.format(qmax))
                qConnectCypherQuery = 'MATCH (place1:Place), (place2:Place)' \
                                      'Where place1.name = "{}" AND place2.name = "{}"' \
                                      'MATCH path = ShortestPath((place1)-[*]-(place2))' \
                                      'WHERE ALL (r in relationships(path) WHERE type(r)="PPP" AND r.qnear >= {}) ' \
                                      'return path'.format(dict(place_1)["name"], dict(place_2)["name"], qmax)
                print(qConnectCypherQuery)
                res = graph.evaluate(qConnectCypherQuery)
                if res != None:
                    connectiveLength = res.__len__()
                    rels = res.relationships
                    qconnectvalue = rels[0].get('qnear')
                    for rel in rels:
                        if rel.get('qnear') < qconnectvalue:
                            qconnectvalue = rel.get('qnear')
                    if (place_1_dimension == 0 and place_2_dimension==0):
                        easierAccessibility = 0
                    else:
                        easierAccessibility = (qconnectvalue+1)/(place_1_dimension+place_2_dimension)
                    createQConnectGraphCypherQuery = 'MATCH (place1:Place), (place2:Place) ' \
                                                     'Where place1.name = "{}" AND place2.name = "{}" ' \
                                                     'MERGE (place1)-[s:PPP]->(place2) ' \
                                                     'SET s.qconnect={} ' \
                                                     'SET s.length = {} ' \
                                                     'SET s.easieraccessibility={} '.format(dict(place_1)["name"], dict(place_2)["name"], qconnectvalue, connectiveLength, easierAccessibility)

                    print(createQConnectGraphCypherQuery)
                    print('res = {}'.format(res))
                    graph.run(createQConnectGraphCypherQuery)
                    break
                else:
                    qmax = qmax-1
                    print(qmax)
