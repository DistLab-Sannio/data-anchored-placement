from neo4j import GraphDatabase
import numpy as np
import time
import neo4j_utils_eng


# driver = GraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "password"))
# COMMUNITIES = ['node-1', 'node-2', 'node-3', 'node-4']
# communities_limits = dict()
# communities_request = dict()

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
        neo4j_utils_eng.load_all(services, edges, db_node_mapping, node_info)
        print("***************** CONSTRAINED GREEDY *********************")
        start = time.time()
        greedy_manager.mark_communities()
        end = time.time()
        print(f"Community marking tooks: {end - start}")

        placements = self.prepare_placements()

        return placements


# if __name__ == '__main__':
#     mark_communities()