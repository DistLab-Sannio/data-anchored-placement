import numpy as np
import time


class GreedyCommunityConstrained:
    def __init__(self, communities, driver):
        self.communities = communities
        self.driver = driver
        self.communities_limits = dict()
        self.communities_request = dict()

    def init_communities(self):
        query = "MATCH(s:Service) SET s.community = -1"
        results = self.driver.session().run(query)
        print(results)
        for community in self.communities:
            query = f"MATCH(a:Anchor) WHERE a.community = '{community}' RETURN a.cpu_limit as l, a.db_cpu_request as c"
            results = self.driver.session().run(query)
            o = results.data()
            if not o:
                l=0
                c=0
            else:
                x = o[0]
                l = x["l"]
                c = x['c']
            self.communities_limits[community] = l
            self.communities_request[community] = c

        #pass

    def get_not_in_communities_count(self):
        query = f"MATCH (m)-[]->(n) WHERE m.community = -1 AND NOT (m:Database) RETURN count(m)"
        results = self.driver.session().run(query)
        x = results.data()[0]['count(m)']
        return x

    def get_leaf_services_not_in_communities_count(self):
        query = f"MATCH (m) WHERE NOT (m)-->() AND ()-->(m) AND m.community = -1 AND NOT (m:Database) RETURN count(m)"
        results = self.driver.session().run(query)
        x = results.data()[0]['count(m)']
        return x


    def get_dangling_node_count(self):
        query = f"MATCH (m) WHERE NOT (m)-->() AND NOT ()-->(m) AND m.community = -1 AND NOT (m:Database) RETURN count(m)"
        results = self.driver.session().run(query)
        x = results.data()[0]['count(m)']
        return x

    def mark_dangling_nodes(self):
        query = f"MATCH (m) WHERE NOT (m)-->() AND NOT ()-->(m) AND m.community = -1 AND NOT (m:Database) RETURN *"
        results = self.driver.session().run(query)
        nodes = list(results.graph()._nodes.values())

        for node in nodes:
            x = node._properties

            min_community_request = np.inf
            min_community = -1
            for c_key in self.communities_request.keys():
                if self.communities_request[c_key] < min_community_request:
                    min_community_request = self.communities_request[c_key]
                    min_community = c_key

            self.communities_request[min_community] = self.communities_request[min_community] + node._properties["cpu_request"]
            selected_node_id = node._properties["name"]
            print(f"Assigning node_id {selected_node_id} \t\t to community min {min_community}")
            query = f"MATCH (m) WHERE m.name = '{selected_node_id}' SET m.community = '{min_community}'"
            results = self.driver.session().run(query)
            print(results)


    def mark_node(self, leaf=False):
        candidates = dict()
        for community in self.communities:
            max_gravity = -np.inf
            max_rel = None
            if not leaf:
                query = f"MATCH (m)-[r]->(n) WHERE m.community = -1 AND n.community = '{community}' AND NOT (n:Database) RETURN *"
            else:
                query = f"MATCH (m)-[r]->(n) WHERE NOT (n)-->() AND n.community = -1 AND m.community = '{community}' AND NOT (n:Database) RETURN *"

            results = self.driver.session().run(query)
            rels = list(results.graph()._relationships.values())

            for rel in rels:
                gravity = rel._properties["total_frequency"]
                if gravity > max_gravity:
                    max_gravity = gravity
                    max_rel = rel

            if max_rel:
                if not leaf:
                    n = max_rel.start_node
                else:
                    n = max_rel.end_node
                candidates[community] = dict()
                candidates[community]["gravity"] = max_gravity
                candidates[community]["rel"] = max_rel
                candidates[community]["node"] = n

        assigned = False
        first_candidate = None
        while not assigned and len(candidates) > 0:
            max_gravity = -np.inf
            max_community = -1
            for community in candidates.keys():
                if candidates[community]["gravity"] > max_gravity:
                    max_gravity = candidates[community]["gravity"]
                    max_community = community

            max_candidate = candidates[max_community]
            node_cpu_request = max_candidate["node"]._properties["cpu_request"]
            # print(max_community)
            new_request = self.communities_request[max_community] + node_cpu_request
            if new_request <= self.communities_limits[max_community]:
                # print("assegna")
                selected_node_id = max_candidate["node"]._properties["node_id"]
                print(f"Assigning node_id {selected_node_id}\t to community {max_community}")
                query = f"MATCH (m:Service) WHERE m.node_id = '{selected_node_id}' SET m.community = '{max_community}'"
                results = self.driver.session().run(query)
                print(results)
                self.communities_request[max_community] = new_request
                assigned = True
            else:
                p = candidates.pop(max_community)
                if first_candidate is None:
                    first_candidate = p
                # print(f"pop {p['node'].id}")

        if len(candidates) == 0 and not assigned:
            print(f'no candidates for {first_candidate["node"]._properties["node_id"]}')
            min_community_request = np.inf
            min_community = -1
            for c_key in self.communities_request.keys():
                if self.communities_request[c_key] < min_community_request:
                    min_community_request = self.communities_request[c_key]
                    min_community = c_key

            self.communities_request[min_community] = self.communities_request[min_community]+ first_candidate["node"]._properties["cpu_request"]

            selected_node_id = first_candidate["node"]._properties["node_id"]
            print(f"Assigning node_id {selected_node_id} \t\t to community min {min_community}")
            query = f"MATCH (m:Service) WHERE m.node_id = '{selected_node_id}' SET m.community = '{min_community}'"
            results = self.driver.session().run(query)
            print(results)



    def mark_communities(self):

        i = 0
        # nx_utils.save_graph(f'plots2/{i}')
        i += 1
        self.init_communities()
        while self.get_not_in_communities_count() > 0:
            self.mark_node(leaf=False)
            # nx_utils.save_graph(f'plots2/{i}')
            i += 1

        print("#############################  leaf  #############################")
        while self.get_leaf_services_not_in_communities_count() > 0:
            self.mark_node(leaf=True)
            # nx_utils.save_graph(f'plots2/{i}')
            i += 1

        print("#############################  dangling  #############################")
        self.mark_dangling_nodes()

        print(f'community_limits \t {self.communities_limits}')
        print(f'community_requests \t {self.communities_request}')

    def prepare_placements(self):
        query = """
            MATCH (n) WHERE NOT (n:Anchor) RETURN *
            """

        results = self.driver.session().run(query)
        nodes = list(results.graph()._nodes.values())
        placements = []
        for node in nodes:
            name = node._properties['name']
            # if name.endswith("-DB"):
            # name = name.replace("-service-DB", "-mongo")
            community = node._properties['community']
            placements.append({
                "serviceId": name,
                "nodeId": f"{community}"
            })

        return placements

    def calculate_deployments(self, greedy_manager, services, edges, db_node_mapping, node_info):
        self.load_all(services, edges, db_node_mapping, node_info)
        print("***************** CONSTRAINED GREEDY *********************")
        start = time.time()
        greedy_manager.mark_communities()
        end = time.time()
        print(f"Community marking tooks: {end - start}")

        placements = self.prepare_placements()

        return placements

    def load_all(self, services, edges, db_node_mapping, node_info):
        query = "MATCH (n) DETACH DELETE n"
        results = self.driver.session().run(query)
        print(results)

        #query = "LOAD CSV WITH HEADERS FROM 'file:///" + SERVICE_NODE_FILE_NAME + "' AS line FIELDTERMINATOR ';' CALL{ WITH line CREATE (i:Service {node_id: toInteger(line.node_id), name: line.service_name})} IN TRANSACTIONS OF 100 ROWS"
        query = "UNWIND $services AS service CREATE (:Service {node_id: service.serviceId, name: service.serviceId})"
        results = self.driver.session().run(query, services=services)
        print(results)

        # query = "LOAD CSV WITH HEADERS FROM 'file:///" + SIZING_FILE_NAME + "' AS line FIELDTERMINATOR ';' CALL{ WITH line MERGE (s:Service {name: line.service_name}) set s.cpu_request=toFloat(line.cpu_request), s.mem_request=toFloat(line.mem_request)} IN TRANSACTIONS OF 100 ROWS"
        query = ("UNWIND $services AS service MERGE (s:Service {name: service.serviceId}) SET s.cpu_request = toFloat(service.constraints.cpu), s.mem_request = toFloat(service.constraints.ram)")
        results = self.driver.session().run(query, services=services)
        print(results)

        query = 'MATCH (s:Service) WHERE s.name ENDS WITH "db" WITH s CREATE (i:Database {node_id:s.node_id, name:s.name, cpu_request:s.cpu_request, mem_request:s.mem_request}) DELETE s'
        results = self.driver.session().run(query)
        print(results)


        query = "CREATE CONSTRAINT IF NOT EXISTS FOR (node:Database) REQUIRE node.node_id IS UNIQUE"
        results = self.driver.session().run(query)
        print(results)

        query = "CREATE CONSTRAINT IF NOT EXISTS FOR (node:Service) REQUIRE node.node_id IS UNIQUE"
        results = self.driver.session().run(query)
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
        results = self.driver.session().run(query, edges=edges)
        print(results)

        query = "MATCH (x) SET x.community=-1"
        results = self.driver.session().run(query)
        print(results)

        # query = "LOAD CSV WITH HEADERS FROM 'file:///" + COMMUNITIES_FILE_NAME + "' AS line FIELDTERMINATOR ';' CALL{ WITH line MATCH (d:Database {name: line.name}) WITH line, d SET d.community=toInteger(line.community)} IN TRANSACTIONS OF 100 ROWS"
        # results = driver.session().run(query)
        with self.driver.session() as session:
            for db_name, node_id in db_node_mapping.items():
                query = """
                MATCH (d:Database {name: $db_name})
                SET d.community = $node_id
                """
                session.run(query, db_name=db_name, node_id=node_id)
        print(results)

        # query = "MATCH (d:Database) WHERE d.community<>-1 WITH d MATCH (x) WITH d, max(x.node_id) as m  MERGE (a:Anchor {community: d.community, node_id: m +1})"
        query = "MATCH (d:Database) WHERE d.community<>-1 MERGE (a:Anchor {community: d.community})"
        results = self.driver.session().run(query)
        print(results)

        # query = f"MATCH (a:Anchor) SET a.cpu_request=toFloat(0), a.mem_request=toFloat(0), a.cpu_limit={MACHINE_CPU_LIMIT}, a.mem_limit={MACHINE_MEM_LIMIT}"
        with self.driver.session() as session:
            for node in node_info:
                query = f"MATCH (a:Anchor) SET a.cpu_request = toFloat(0), a.mem_request = toFloat(0), a.cpu_limit = $cpu, a.mem_limit = $ram"
                results = session.run(query, cpu=node["cpu"], ram=node["ram"])
                print(results)

        query = "MATCH (s:Service)-[c:CALL]->(d:Database) WITH s, c, d MATCH (a:Anchor {community: d.community}) WITH s, c, d, a MERGE (s)-[x:CALL{total_frequency: c.total_frequency}]->(a) "
        results = self.driver.session().run(query)
        print(results)

        query = "MATCH (d:Database) WHERE d.community<>-1 WITH d MATCH (a:Anchor {community: d.community}) WITH d, a MERGE  (d)-[x:ANCHORED]->(a)"
        results = self.driver.session().run(query)
        print(results)

        query = "MATCH (d:Database)-[x:ANCHORED]->(a:Anchor) WHERE d.community<>-1 SET a.cpu_request=a.cpu_request+d.cpu_request, a.mem_request=a.mem_request+d.mem_request"
        results = self.driver.session().run(query)
        print(results)

        query = "MATCH (d:Database)-[x:ANCHORED]->(a:Anchor) WHERE d.community<>-1 SET a.db_cpu_request=a.cpu_request, a.db_mem_request=a.mem_request"
        results = self.driver.session().run(query)
        print(results)

        query = "MATCH ()-[c:CALL]->() set c.distance = 1/c.total_frequency"
        results = self.driver.session().run(query)
        print(results)

        query = 'MATCH (a:Anchor) set a.name = "anchor-"+a.community'
        results = self.driver.session().run(query)
        print(results)


    def set_community_of_random(self):
        query = """LOAD CSV WITH HEADERS FROM 'file:///deployment_variables_random.csv' AS line FIELDTERMINATOR ';' CALL{ WITH line MATCH (i:Service {name: line.name}) SET i.community=toInteger(line.cluster_id)} IN TRANSACTIONS OF 100 ROWS"""
        results = self.driver.session().run(query)
        print(results)


    def set_community_of_random_DBPS(self):
        query = """LOAD CSV WITH HEADERS FROM 'file:///deployment_variables_randomDBPS.csv' AS line FIELDTERMINATOR ';' CALL{ WITH line MATCH (i:Service {name: line.name}) SET i.community=toInteger(line.cluster_id)} IN TRANSACTIONS OF 100 ROWS"""
        results = self.driver.session().run(query)
        print(results)

# if __name__ == '__main__':
#     mark_communities()