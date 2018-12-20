from py2neo import Graph, NodeSelector
graph = Graph(user='neo4j', password='12345')
# selector = NodeSelector(graph)
# userNodes = selector.select("Person")
qConnectCypherQuery = 'match (p1:Person{name:"Mahdi"}),(p2:Person{name:"Thierry"}),p = ShortestPath((p1)-[*]-(p2))' \
                                      ' WHERE ALL (r in relationships(p) WHERE type(r)="QNEAR" AND r.qnearness >= 2)' \
                                       'return p'
res = graph.evaluate(qConnectCypherQuery)
p=graph.evaluate(qConnectCypherQuery).relationships()
q=p[0]
q.get('maxqnearness')
print(p[0])