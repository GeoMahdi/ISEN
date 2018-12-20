from py2neo import Graph, NodeSelector
graph = Graph(user='neo4j', password='12345')
selector = NodeSelector(graph)
userNodes = selector.select("Person")
deleteQNEARPersonEdgesCypherQuery = 'MATCH (p1:Person)-[s:QNEAR]-(p2:Person)' \
                              'DELETE s'
graph.run(deleteQNEARPersonEdgesCypherQuery)
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
                                          'MERGE (person1)-[r:QNEAR]->(person2)' \
                                          'SET r.qnearness={}, r.maxqnearness={}'.format(dict(user1)["name"], dict(user2)["name"], qnearness, maxqnearness)
            print(createQNearGraphCypherQuery)

qmax = maxqnearness
for user_1 in userNodes:
    for user_2 in userNodes:
        if user_1 != user_2:
            while qmax > -1:
                qConnectCypherQuery = 'MATCH (person1:Person), (person2:Person) ' \
                                          'Where person1.name = "{}" AND person2.name = "{}" ' \
                                      'path = ShortestPath((person1)-[*]-(person2))' \
                                      ' WHERE ALL (r in relationships(p) WHERE type(r)="QNEAR" AND r.qnearness >= {}) ' \
                                      'return path'.format(dict(user_1)["name"], dict(user_2)["name"], qmax)
                print(qConnectCypherQuery)
                res = graph.evaluate(qConnectCypherQuery)
                if res != None:
                    connectiveLength = res.__len__()
                    rels = res.relationships()
                    qconnectvalue = rels[0].get('qnearness')
                    for rel in rels:
                        if rel.get('qnearness') < qconnectvalue:
                            qconnectvalue = rel.get('qnearness')

                    createQConnectGraphCypherQuery = 'MATCH (person1:Person), (place2:Person2) ' \
                                                      'Where person1.name = "{}" AND person2.name = "{}" ' \
                                                      'MERGE (person1)-[s:QCONNECT]->(person2) ' \
                                                      'SET s.qconnect={} SET s.length = {}'.format(dict(user_1)["name"], dict(user_2)["name"], qconnectvalue, connectiveLength)

                    print(createQConnectGraphCypherQuery)
                    graph.run(createQConnectGraphCypherQuery)
                    break
                else:
                    qmax = qmax-1


#
#

#
#
# deleteQNEARPlaceEdgesCypherQuery = 'MATCH (pl1:Place)-[t:QNEAR]-(pl2:Place)' \
#                               'DELETE t'
# graph.run(deleteQNEARPlaceEdgesCypherQuery)
# maxqnearness_ = -1
# placeNodes = selector.select("Place")
#
# for place1 in placeNodes:
#     for place2 in placeNodes:
#         if place1 != place2:
#             qNearnessCypherQuery_ = 'MATCH (place1:Place)-[r:CHECK_IN]-(person:Person)-[s:CHECK_IN]-(place2:Place) ' \
#                                     'Where place1.name = "{}" AND place2.name = "{}" ' \
#                                     'return COUNT (DISTINCT person)'.format(dict(place1)["name"], dict(place2)["name"])
#             qnearness_ = graph.run(qNearnessCypherQuery_).data()[0]['COUNT (DISTINCT person)']-1
#             if qnearness_ > maxqnearness_:
#                 maxqnearness_ = qnearness_
#             print(qnearness_)
#             createQNearGraphCypherQuery_ = 'MATCH (place1:Place), (place2:Place) ' \
#                                            'Where place1.name = "{}" AND place2.name = "{}" ' \
#                                            'MERGE (place1)-[r:QNEAR]->(place2) SET r.qnearness={}, r.maxqnearness={}'.format(dict(place1)["name"], dict(place2)["name"], qnearness_, maxqnearness_)
#             print(createQNearGraphCypherQuery_)
#             graph.run(createQNearGraphCypherQuery_)

            # if qnearness_ > -1:
            #     createQConnectGraphCypherQuery_ = 'MATCH (place1:Place), (place2:Place) ' \
            #                                       'Where place1.name = "{}" AND place2.name = "{}" ' \
            #                                       'MERGE (place1)-[s:QCONNECT]->(place2) SET s.qconnectedness={} SET s.length = 1'.format(dict(place1)["name"], dict(place2)["name"], qnearness_)
            #     print(createQConnectGraphCypherQuery_)
            #     graph.run(createQConnectGraphCypherQuery_)








#Test
# print(userNodes)
# print(len((list(userNodes))))
# text= 'Match (person1:Person)-[r:CHECK_IN]-(place:Place)-[s:CHECK_IN]-(person2:Person) Where person1.name = "Ben" AND person2.name = "Iman" return COUNT (DISTINCT place)'
# print(type(graph.run(text).data()))
# data=graph.run(text).data()
# print(type((data[0])))
# print((data[0])['COUNT (DISTINCT place)'])
            # print(placeNodes)
            # print(len((list(placeNodes))))
            # text= 'MATCH (place1:Place)-[r:CHECK_IN]-(person:Person)-[s:CHECK_IN]-(place2:Place) Where place1.name = "SALLE TP CHIMIE" AND place2.name = "SALLE TP 230" return COUNT (DISTINCT person)'
            # data=graph.run(text).data()
            # print(type((data[0])))
            # print((data[0])['COUNT (DISTINCT person)'])

# text = "Match (person1:Person)-[r:CHECK_IN]-(place:Place)-[s:CHECK_IN]-(person2:Person) Where person1.name = \"Thierry\" AND person2.name = \"Mahdi\" return COUNT (DISTINCT place)"
# result = graph.run(text).data()
# print(type(result))
# print(type(result[0]))
# print(result[0]["COUNT (DISTINCT place)"])
# user1="Mahdi"
# user2="Rahimi"
# text = "Match (person1:Person)-[r:CHECK_IN]-(place:Place)-[s:CHECK_IN]-(person2:Person) Where person1.name = {} AND person2.name = {} return COUNT (DISTINCT place)".format(user1,user2)
# print(text)