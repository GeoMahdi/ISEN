# importing required py2neo objects
from py2neo import Graph, NodeMatcher
graph = Graph(user='neo4j', password='12345',port = 11006, scheme='bolt', host='localhost')
# defining the node matcher object
matcher = NodeMatcher(graph)
# getting user nodes using node matcher
userNodes = matcher.match("Person")
deleteQNEARPersonEdgesCypherQuery = 'MATCH (p1:Person)-[s:QNEAR]-(p2:Person)' \
                              'DELETE s'
deleteQCONNECTPersonEdgesCypherQuery = 'MATCH (p1:Person)-[s:QCONNECT]-(p2:Person)' \
                              'DELETE s'
deleteQPersonEdgesCypherQuery = 'MATCH (p1:Person)-[s:Q]-(p2:Person)' \
                              'DELETE s'
deleteUUQPersonEdgesCypherQuery = 'MATCH (p1:Person)-[s:UUQ]-(p2:Person)' \
                              'DELETE s'
deleteUPUQPersonEdgesCypherQuery = 'MATCH (p1:Person)-[s:UPUQ]-(p2:Person)' \
                              'DELETE s'
deleteUPUPersonEdgesCypherQuery = 'MATCH (p1:Person)-[s:UPU]-(p2:Person)' \
                              'DELETE s'
graph.run(deleteQNEARPersonEdgesCypherQuery)
graph.run(deleteQCONNECTPersonEdgesCypherQuery)
graph.run(deleteQPersonEdgesCypherQuery)
graph.run(deleteUUQPersonEdgesCypherQuery)
graph.run(deleteUPUQPersonEdgesCypherQuery)
graph.run(deleteUPUPersonEdgesCypherQuery)
for user in userNodes:
    dimensionQuery0 = 'MATCH (p1:Person)' \
                      'SET p1.upudim = 0'
    graph.run(dimensionQuery0)
for user in userNodes:
    dimensionQuery1 = 'MATCH (p1:Person)-[r:CHECK_IN]-(place:Place)' \
                       'WHERE p1.name = "{}"' \
                       'RETURN COUNT( DISTINCT(place))'.format(dict(user)["name"])
    print(dimensionQuery1)
    dimension = graph.evaluate(dimensionQuery1)
    dimensionQuery2='MATCH (p1:Person)' \
                     'WHERE p1.name = "{}"' \
                     'SET p1.upudim = {}'.format(dict(user)["name"],dimension)
    graph.run(dimensionQuery2)
maxqnearness = -1
for user1 in userNodes:
    for user2 in userNodes:
        if user1 != user2:
            qNearnessCypherQuery = 'MATCH (person1:Person)-[r:CHECK_IN]-(place:Place)-[s:CHECK_IN]-(person2:Person) ' \
                                   'Where person1.name = "{}" AND person2.name = "{}" ' \
                                   'return COUNT (DISTINCT place)'.format(dict(user1)["name"], dict(user2)["name"])
            qnearness = graph.run(qNearnessCypherQuery).data()[0]['COUNT (DISTINCT place)']-1
            if qnearness > maxqnearness:
                maxqnearness = qnearness
            print(qnearness)
            createQNearGraphCypherQuery = 'MATCH (person1:Person), (person2:Person) ' \
                                          'Where person1.name = "{}" AND person2.name = "{}" ' \
                                          'MERGE (person1)-[r:UPU]->(person2)' \
                                          'SET r.qnear={}, r.maxqnear={}'.format(dict(user1)["name"], dict(user2)["name"], qnearness, maxqnearness)
            print(createQNearGraphCypherQuery)
            graph.run(createQNearGraphCypherQuery)
userNodes_ = matcher.match("Person")
for user_1 in userNodes_:
    user_1_dimension = graph.evaluate('MATCH (p:Person)'
                                      'WHERE p.name = "{}"'
                                      'return p.upudim'.format(dict(user_1)["name"]))
    for user_2 in userNodes_:
        user_2_dimension = graph.evaluate('MATCH (p:Person)'
                                          'WHERE p.name = "{}"'
                                          'return p.upudim'.format(dict(user_2)["name"]))
        if user_1 != user_2:
            print(dict(user_1)["name"], dict(user_2)["name"])
            qmax = maxqnearness
            while qmax > -1:
                print('qmax = {}'.format(qmax))
                qConnectCypherQuery = 'MATCH (person1:Person), (person2:Person)' \
                                      'Where person1.name = "{}" AND person2.name = "{}"' \
                                      'MATCH path = ShortestPath((person1)-[*]-(person2))' \
                                      'WHERE ALL (r in relationships(path) WHERE type(r)="UPU" AND r.qnear >= {}) ' \
                                      'return path'.format(dict(user_1)["name"], dict(user_2)["name"], qmax)
                print(qConnectCypherQuery)
                res = graph.evaluate(qConnectCypherQuery)
                if res != None:
                    connectiveLength = res.__len__()
                    rels = res.relationships
                    qconnectvalue = rels[0].get('qnear')
                    for rel in rels:
                        if rel.get('qnear') < qconnectvalue:
                            qconnectvalue = rel.get('qnear')
                    if(user_1_dimension == 0 and user_2_dimension == 0):
                        similarity = 0
                    else:
                        similarity = (qconnectvalue+1)/(user_1_dimension+user_2_dimension)
                    createQConnectGraphCypherQuery = 'MATCH (person1:Person), (person2:Person) ' \
                                                     'Where person1.name = "{}" AND person2.name = "{}" ' \
                                                     'MERGE (person1)-[s:UPU]->(person2) ' \
                                                     'SET s.qconnect={} ' \
                                                     'SET s.length = {} ' \
                                                     'SET s.similarity={} '.format(dict(user_1)["name"], dict(user_2)["name"], qconnectvalue, connectiveLength, similarity)

                    print(createQConnectGraphCypherQuery)
                    print('res = {}'.format(res))
                    graph.run(createQConnectGraphCypherQuery)
                    break
                else:
                    qmax = qmax-1
                    print(qmax)
