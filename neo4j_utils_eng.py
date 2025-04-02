import time

import neo4j
from neo4j import GraphDatabase


driver = GraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "password"))

def load_all(services, edges, db_node_mapping, node_info):
    query = "MATCH (n) DETACH DELETE n"
    results = driver.session().run(query)
    print(results)

    #query = "LOAD CSV WITH HEADERS FROM 'file:///" + SERVICE_NODE_FILE_NAME + "' AS line FIELDTERMINATOR ';' CALL{ WITH line CREATE (i:Service {node_id: toInteger(line.node_id), name: line.service_name})} IN TRANSACTIONS OF 100 ROWS"
    query = "UNWIND $services AS service CREATE (:Service {node_id: service.serviceId, name: service.serviceId})"
    results = driver.session().run(query, services=services)
    print(results)

    # query = "LOAD CSV WITH HEADERS FROM 'file:///" + SIZING_FILE_NAME + "' AS line FIELDTERMINATOR ';' CALL{ WITH line MERGE (s:Service {name: line.service_name}) set s.cpu_request=toFloat(line.cpu_request), s.mem_request=toFloat(line.mem_request)} IN TRANSACTIONS OF 100 ROWS"
    query = ("UNWIND $services AS service MERGE (s:Service {name: service.serviceId}) SET s.cpu_request = toFloat(service.constraints.cpu), s.mem_request = toFloat(service.constraints.ram)")
    results = driver.session().run(query, services=services)
    print(results)

    query = 'MATCH (s:Service) WHERE s.name ENDS WITH "db" WITH s CREATE (i:Database {node_id:s.node_id, name:s.name, cpu_request:s.cpu_request, mem_request:s.mem_request}) DELETE s'
    results = driver.session().run(query)
    print(results)


    query = "CREATE CONSTRAINT IF NOT EXISTS FOR (node:Database) REQUIRE node.node_id IS UNIQUE"
    results = driver.session().run(query)
    print(results)

    query = "CREATE CONSTRAINT IF NOT EXISTS FOR (node:Service) REQUIRE node.node_id IS UNIQUE"
    results = driver.session().run(query)
    print(results)

    # query = """LOAD CSV WITH HEADERS FROM 'file:///""" + SERVICE_EDGE_FILE_NAME + """' AS line FIELDTERMINATOR ';'
    #             CALL{ WITH line
    #                 MATCH (src {node_id: toInteger(line.source)})
    #                 WITH src, line MATCH (dst {node_id: toInteger(line.dest)}) CREATE (src)-[r:CALL {total_frequency: toFloat(line.total_frequency)}]->(dst)
    #                 } IN TRANSACTIONS OF 100 ROWS"""
    query = """
                UNWIND $edges AS edge
                MATCH (src {node_id: edge.source})
                MATCH (dst {node_id: edge.dest})
                CREATE (src)-[r:CALL {total_frequency: toFloat(edge.total_frequency)}]->(dst)
            """
    results = driver.session().run(query, edges=edges)
    print(results)

    query = "MATCH (x) SET x.community=-1"
    results = driver.session().run(query)
    print(results)

    # query = "LOAD CSV WITH HEADERS FROM 'file:///" + COMMUNITIES_FILE_NAME + "' AS line FIELDTERMINATOR ';' CALL{ WITH line MATCH (d:Database {name: line.name}) WITH line, d SET d.community=toInteger(line.community)} IN TRANSACTIONS OF 100 ROWS"
    # results = driver.session().run(query)
    with driver.session() as session:
        for db_name, node_id in db_node_mapping.items():
            query = """
            MATCH (d:Database {name: $db_name})
            SET d.community = $node_id
            """
            session.run(query, db_name=db_name, node_id=node_id)
    print(results)

    # query = "MATCH (d:Database) WHERE d.community<>-1 WITH d MATCH (x) WITH d, max(x.node_id) as m  MERGE (a:Anchor {community: d.community, node_id: m +1})"
    query = "MATCH (d:Database) WHERE d.community<>-1 MERGE (a:Anchor {community: d.community})"
    results = driver.session().run(query)
    print(results)

    # query = f"MATCH (a:Anchor) SET a.cpu_request=toFloat(0), a.mem_request=toFloat(0), a.cpu_limit={MACHINE_CPU_LIMIT}, a.mem_limit={MACHINE_MEM_LIMIT}"
    with driver.session() as session:
        for node in node_info:
            query = f"MATCH (a:Anchor) SET a.cpu_request = toFloat(0), a.mem_request = toFloat(0), a.cpu_limit = $cpu, a.mem_limit = $ram"
            results = session.run(query, cpu=node["cpu"], ram=node["ram"])
            print(results)

    query = "MATCH (s:Service)-[c:CALL]->(d:Database) WITH s, c, d MATCH (a:Anchor {community: d.community}) WITH s, c, d, a MERGE (s)-[x:CALL{total_frequency: c.total_frequency}]->(a) "
    results = driver.session().run(query)
    print(results)

    query = "MATCH (d:Database) WHERE d.community<>-1 WITH d MATCH (a:Anchor {community: d.community}) WITH d, a MERGE  (d)-[x:ANCHORED]->(a)"
    results = driver.session().run(query)
    print(results)

    query = "MATCH (d:Database)-[x:ANCHORED]->(a:Anchor) WHERE d.community<>-1 SET a.cpu_request=a.cpu_request+d.cpu_request, a.mem_request=a.mem_request+d.mem_request"
    results = driver.session().run(query)
    print(results)

    query = "MATCH (d:Database)-[x:ANCHORED]->(a:Anchor) WHERE d.community<>-1 SET a.db_cpu_request=a.cpu_request, a.db_mem_request=a.mem_request"
    results = driver.session().run(query)
    print(results)

    query = "MATCH ()-[c:CALL]->() set c.distance = 1/c.total_frequency"
    results = driver.session().run(query)
    print(results)

    query = 'MATCH (a:Anchor) set a.name = "anchor-"+a.community'
    results = driver.session().run(query)
    print(results)


def set_community_of_random():
    query = """LOAD CSV WITH HEADERS FROM 'file:///deployment_variables_random.csv' AS line FIELDTERMINATOR ';' CALL{ WITH line MATCH (i:Service {name: line.name}) SET i.community=toInteger(line.cluster_id)} IN TRANSACTIONS OF 100 ROWS"""
    results = driver.session().run(query)
    print(results)


def set_community_of_random_DBPS():
    query = """LOAD CSV WITH HEADERS FROM 'file:///deployment_variables_randomDBPS.csv' AS line FIELDTERMINATOR ';' CALL{ WITH line MATCH (i:Service {name: line.name}) SET i.community=toInteger(line.cluster_id)} IN TRANSACTIONS OF 100 ROWS"""
    results = driver.session().run(query)
    print(results)

def calculate_modularity():
    project_query = """MATCH (m)-[r]->(n) WHERE NOT (m:Database) AND NOT (n:Database) 
        WITH gds.graph.project('graphGreedy', m, n, {
          sourceNodeProperties: m { .community },
          targetNodeProperties: n { .community  },
          relationshipProperties: r { .total_frequency } 
          }) AS g
        RETURN
          g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels"""

    results_query = driver.session().run(project_query)
    time.sleep(5)

    calculate_modularity = '''
        CALL gds.modularity.stream('graphGreedy', {
             communityProperty: 'community',
             relationshipWeightProperty: 'total_frequency'
        })
        YIELD communityId, modularity
        RETURN communityId, modularity
        '''
    #         ORDER BY communityId ASC
    results = driver.execute_query(calculate_modularity, result_transformer_=neo4j.Result.to_df)

    delete_projection = '''CALL gds.graph.drop('graphGreedy')'''
    results_del = driver.session().run(delete_projection)
    return results

def calculate_conductance():
    project_query = """MATCH (m)-[r]->(n) WHERE NOT (m:Database) AND NOT (n:Database) 
        WITH gds.graph.project('graphGreedy', m, n, {
          sourceNodeProperties: m { .community },
          targetNodeProperties: n { .community  },
          relationshipProperties: r { .total_frequency } 
          }) AS g
        RETURN
          g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels"""

    results_query = driver.session().run(project_query)
    time.sleep(2)

    calculate_conductance = '''
        CALL gds.conductance.stream('graphGreedy', {
             communityProperty: 'community',
             relationshipWeightProperty: 'total_frequency'
        })
        YIELD community, conductance
        RETURN community, conductance
        '''
    #         ORDER BY communityId ASC
    results = driver.execute_query(calculate_conductance, result_transformer_=neo4j.Result.to_df)


    delete_projection = '''CALL gds.graph.drop('graphGreedy')'''
    results_del = driver.session().run(delete_projection)
    return results

if __name__ == '__main__':
    services = [
    {"serviceId": "ts-system-proxy", "constraints": {
                    "carbonIntensity": 0.53,
                    "cpu": 2,
                    "ram": 8589934592,
                    "storage": 0,
                    "gpu": 0
                    }
    },
    {"serviceId": "ts-contacts-service", "constraints": {
                    "carbonIntensity": 0.53,
                    "cpu": 2,
                    "ram": 8589934592,
                    "storage": 0,
                    "gpu": 0
                    }
     },
    {"serviceId": "Data-Lake-DB", "constraints": {
                    "carbonIntensity": 0.53,
                    "cpu": 2,
                    "ram": 8589934592,
                    "storage": 0,
                    "gpu": 0
                    }
     }
    ]

    edges = [
        {"source": "ts-system-proxy", "dest": "Data-Lake-DB", "total_frequency": 10},
        {"source": "ts-system-proxy", "dest": "ts-contacts-service", "total_frequency": 5},
        {"source": "ts-contacts-service", "dest": "Data-Lake-DB", "total_frequency": 7}
    ]

    db_node_mapping = {'Data-Lake-DB': 'node-2'}

    node_info = [{'cpu': 4, 'nodeId': 'node-1', 'ram': 17179869184}, {'cpu': 4, 'nodeId': 'node-2', 'ram': 85899345920}]

    load_all(services, edges, db_node_mapping, node_info)

    # calculate_modularity()






