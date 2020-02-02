# importing required py2neo objects
from py2neo import Graph, NodeMatcher
graph = Graph(user='neo4j', password='12345',port = 11006, scheme='bolt', host='localhost')
# defining the node matcher object
matcher = NodeMatcher(graph)
# getting user nodes using node matcher
personNodes = matcher.match("Person")
deleteUUUCypherQuery = 'MATCH (p1:Person)-[s:UUU]-(p2:Person)' \
                              'DELETE s'
graph.run(deleteUUUCypherQuery)
for person in personNodes:
    dimensionQuery0 = 'MATCH (p1:Person)' \
                      'SET p1.uuudim = 0'
    graph.run(dimensionQuery0)
for person in personNodes:

    dimensionQuery1 = 'MATCH (p1:Person)-[r:KNOWS]-(p2:Person)' \
                       'WHERE p1.name = "{}"' \
                       'RETURN COUNT( DISTINCT(p2))'.format(dict(person)["name"])
    print(dimensionQuery1)
    dimension = graph.evaluate(dimensionQuery1)
    dimensionQuery2='MATCH (p1:Person)' \
                     'WHERE p1.name = "{}"' \
                     'SET p1.uuudim = {}'.format(dict(person)["name"],dimension)
    print(dimensionQuery2)
    graph.run(dimensionQuery2)
maxqnearness = -1
for person1 in personNodes:
    for person2 in personNodes:
        if person1 != person2:
            qNearnessCypherQuery = 'MATCH (person1:Person)-[r:KNOWS]-(p:Person)-[s:KNOWS]-(person2:Person) ' \
                                   'Where person1.name = "{}" AND person2.name = "{}" ' \
                                   'return COUNT (DISTINCT p)'.format(dict(person1)["name"], dict(person2)["name"])
            qnearness = graph.run(qNearnessCypherQuery).data()[0]['COUNT (DISTINCT p)']-1
            if qnearness > maxqnearness:
                maxqnearness = qnearness
            print(qnearness)
            createQNearGraphCypherQuery = 'MATCH (person1:Person), (person2:Person) ' \
                                          'Where person1.name = "{}" AND person2.name = "{}" ' \
                                          'MERGE (person1)-[r:UUU]->(person2)' \
                                          'SET r.qnear={}, r.maxqnear={}'.format(dict(person1)["name"], dict(person2)["name"], qnearness, maxqnearness)
            print(createQNearGraphCypherQuery)
            graph.run(createQNearGraphCypherQuery)

personNodes_ = matcher.match("Person")
for person_1 in personNodes_:
    person_1_dimension = graph.evaluate('MATCH (p:Person)'
                                      'WHERE p.name = "{}"'
                                      'return p.uuudim'.format(dict(person_1)["name"]))
    for person_2 in personNodes_:
        person_2_dimension = graph.evaluate('MATCH (p:Person)'
                                          'WHERE p.name = "{}"'
                                          'return p.uuudim'.format(dict(person_2)["name"]))
        if person_1 != person_2:
            print(dict(person_1)["name"], dict(person_2)["name"])
            qmax = maxqnearness
            while qmax > -1:
                print('qmax = {}'.format(qmax))
                qConnectCypherQuery = 'MATCH (person1:Person), (person2:Person)' \
                                      'Where person1.name = "{}" AND person2.name = "{}"' \
                                      'MATCH path = ShortestPath((person1)-[*]-(person2))' \
                                      'WHERE ALL (r in relationships(path) WHERE type(r)="UUU" AND r.qnear >= {}) ' \
                                      'return path'.format(dict(person_1)["name"], dict(person_2)["name"], qmax)
                print(qConnectCypherQuery)
                res = graph.evaluate(qConnectCypherQuery)
                if res != None:
                    connectiveLength = res.__len__()
                    rels = res.relationships
                    qconnectvalue = rels[0].get('qnear')
                    for rel in rels:
                        if rel.get('qnear') < qconnectvalue:
                            qconnectvalue = rel.get('qnear')
                    if (person_1_dimension == 0 and person_2_dimension==0):
                        similarity = 0
                    else:
                        similarity = (qconnectvalue+1)/(person_1_dimension+person_2_dimension)
                    createQConnectGraphCypherQuery = 'MATCH (person1:Person), (person2:Person) ' \
                                                     'Where person1.name = "{}" AND person2.name = "{}" ' \
                                                     'MERGE (person1)-[s:UUU]->(person2) ' \
                                                     'SET s.qconnect={} ' \
                                                     'SET s.length = {} ' \
                                                     'SET s.similarity={} '.format(dict(person_1)["name"], dict(person_2)["name"], qconnectvalue, connectiveLength, similarity)

                    print(createQConnectGraphCypherQuery)
                    print('res = {}'.format(res))
                    graph.run(createQConnectGraphCypherQuery)
                    break
                else:
                    qmax = qmax-1
                    print(qmax)
