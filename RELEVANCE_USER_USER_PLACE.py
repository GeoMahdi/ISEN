# importing required py2neo objects
from py2neo import Graph, NodeMatcher
graph = Graph(user='neo4j', password='12345',port = 11006, scheme='bolt', host='localhost')
# defining the node matcher object
matcher = NodeMatcher(graph)
# getting user nodes using node matcher
placeNodes = matcher.match("Place")
userNodes = matcher.match("Person")
deleteUUPQ = 'MATCH (p1:Person)-[s:UUPQ]-(p2:Place) ' \
             'DELETE s'
deleteUUP = 'MATCH (p1:Person)-[s:UUP]-(p2:Place) ' \
             'DELETE s'
graph.run(deleteUUP)
for user in userNodes:
    dimensionQuery0 = 'MATCH (p1:Person)' \
                      'SET p1.uupdim = 0'
    graph.run(dimensionQuery0)
for user in userNodes:
    dimensionQuery1 = 'MATCH (p1:Person)-[r:KNOWS]-(person:Person)' \
                       'WHERE p1.name = "{}"' \
                       'RETURN COUNT( DISTINCT(person))'.format(dict(user)["name"])
    print(dimensionQuery1)
    dimension = graph.evaluate(dimensionQuery1)
    dimensionQuery2='MATCH (p1:Person)' \
                     'WHERE p1.name = "{}"' \
                     'SET p1.uupdim = {}'.format(dict(user)["name"],dimension)
    graph.run(dimensionQuery2)

for place in placeNodes:
    dimensionQuery_0 = 'MATCH (p1:Place)' \
                      'SET p1.puudim = 0'
    graph.run(dimensionQuery_0)

for place in placeNodes:

    dimensionQuery_1 = 'MATCH (p1:Place)-[r:CHECK_IN]-(person:Person)' \
                       'WHERE p1.name = "{}"' \
                       'RETURN COUNT( DISTINCT(person))'.format(dict(place)["name"])
    print(dimensionQuery_1)
    dimension = graph.evaluate(dimensionQuery_1)
    dimensionQuery_2='MATCH (p1:Place)' \
                     'WHERE p1.name = "{}"' \
                     'SET p1.puudim = {}'.format(dict(place)["name"],dimension)
    graph.run(dimensionQuery_2)

maxqnearness = -1
for user in userNodes:
    for place in placeNodes:
            qNearnessCypherQuery = 'MATCH (person1:Person)-[r:KNOWS]-(person:Person)-[s:CHECK_IN]-(place:Place) ' \
                                   'Where person1.name = "{}" AND place.name = "{}" ' \
                                   'return COUNT (DISTINCT person)'.format(dict(user)["name"], dict(place)["name"])
            qnearness = graph.run(qNearnessCypherQuery).data()[0]['COUNT (DISTINCT person)'] - 1
            if qnearness > maxqnearness:
                maxqnearness = qnearness
            print(qnearness)
            createQNearGraphCypherQuery = 'MATCH (person1:Person), (place:Place) ' \
                                          'Where person1.name = "{}" AND place.name = "{}" ' \
                                          'MERGE (person1)-[r:UUP]->(place)' \
                                          'SET r.qnear={}, r.maxqnear={}'.format(dict(user)["name"], dict(place)["name"], qnearness, maxqnearness)
            print(createQNearGraphCypherQuery)
            graph.run(createQNearGraphCypherQuery)

for user in userNodes:
     user_dimension = graph.evaluate('MATCH (p:Person)'
                                     'WHERE p.name = "{}"'
                                     'return p.uupdim'.format(dict(user)["name"]))
     for place in placeNodes:
         place_dimension = graph.evaluate('MATCH (p:Place)'
                                          'WHERE p.name = "{}"'
                                           'return p.puudim'.format(dict(place)["name"]))
         print(dict(user)["name"], dict(place)["name"])
         qmax = maxqnearness
         while qmax > -1:
             print('qmax = {}'.format(qmax))
             qConnectCypherQuery = 'MATCH (person1:Person), (place:Place)' \
                                   'Where person1.name = "{}" AND place.name = "{}"' \
                                   'MATCH path = ShortestPath((person1)-[*]-(place))' \
                                   'WHERE ALL (r in relationships(path) WHERE type(r)="UUP" AND r.qnear >= {}) ' \
                                   'return path'.format(dict(user)["name"], dict(place)["name"], qmax)
             print(qConnectCypherQuery)
             res = graph.evaluate(qConnectCypherQuery)
             if res != None:
                 connectiveLength = res.__len__()
                 rels = res.relationships
                 qconnectvalue = rels[0].get('qnear')
                 for rel in rels:
                     if rel.get('qnear') < qconnectvalue:
                         qconnectvalue = rel.get('qnear')
                 if (user_dimension == 0 and place_dimension == 0):
                     similarity = 0
                 else:
                     similarity = (qconnectvalue + 1) / (user_dimension + place_dimension)
                     createQConnectGraphCypherQuery = 'MATCH (person1:Person), (place:Place) ' \
                                                      'Where person1.name = "{}" AND place.name = "{}" ' \
                                                      'MERGE (person1)-[s:UUP]->(place) ' \
                                                      'SET s.qconnect={} ' \
                                                      'SET s.length = {} ' \
                                                      'SET s.relevance={} '.format(dict(user)["name"],
                                                                                    dict(place)["name"], qconnectvalue,
                                                                                    connectiveLength, similarity)

                     print(createQConnectGraphCypherQuery)
                     print('res = {}'.format(res))
                     graph.run(createQConnectGraphCypherQuery)
                     break
             else:
                 qmax = qmax - 1
                 print(qmax)