from neo4j import GraphDatabase
import numpy as np


driver = GraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "password"))
COMMUNITIES = ['node-1', 'node-2', 'node-3', 'node-4']
communities_limits = dict()
communities_request = dict()

def init_communities():
    query = "MATCH(s:Service) SET s.community = -1"
    results = driver.session().run(query)
    print(results)
    for community in COMMUNITIES:
        query = f"MATCH(a:Anchor) WHERE a.community = '{community}' RETURN a.cpu_limit as l, a.db_cpu_request as c"
        results = driver.session().run(query)
        o = results.data()
        if not o:
            l=0
            c=0
        else:
            x = o[0]
            l = x["l"]
            c = x['c']
        communities_limits[community] = l
        communities_request[community] = c

    pass

def get_not_in_communities_count():
    query = f"MATCH (m)-[]->(n) WHERE m.community = -1 AND NOT (m:Database) RETURN count(m)"
    results = driver.session().run(query)
    x = results.data()[0]['count(m)']
    return x

def get_leaf_services_not_in_communities_count():
    query = f"MATCH (m) WHERE NOT (m)-->() AND ()-->(m) AND m.community = -1 AND NOT (m:Database) RETURN count(m)"
    results = driver.session().run(query)
    x = results.data()[0]['count(m)']
    return x


def get_dangling_node_count():
    query = f"MATCH (m) WHERE NOT (m)-->() AND NOT ()-->(m) AND m.community = -1 AND NOT (m:Database) RETURN count(m)"
    results = driver.session().run(query)
    x = results.data()[0]['count(m)']
    return x

def mark_dangling_nodes():
    query = f"MATCH (m) WHERE NOT (m)-->() AND NOT ()-->(m) AND m.community = -1 AND NOT (m:Database) RETURN *"
    results = driver.session().run(query)
    nodes = list(results.graph()._nodes.values())

    for node in nodes:
        x = node._properties

        min_community_request = np.inf
        min_community = -1
        for c_key in communities_request.keys():
            if communities_request[c_key] < min_community_request:
                min_community_request = communities_request[c_key]
                min_community = c_key

        communities_request[min_community] = communities_request[min_community] + node._properties["cpu_request"]
        selected_node_id = node._properties["name"]
        print(f"Assigning node_id {selected_node_id} \t\t to community min {min_community}")
        query = f"MATCH (m) WHERE m.name = '{selected_node_id}' SET m.community = '{min_community}'"
        results = driver.session().run(query)
        print(results)


def mark_node(leaf=False):
    candidates = dict()
    for community in COMMUNITIES:
        max_gravity = -np.inf
        max_rel = None
        if not leaf:
            query = f"MATCH (m)-[r]->(n) WHERE m.community = -1 AND n.community = '{community}' AND NOT (n:Database) RETURN *"
        else:
            query = f"MATCH (m)-[r]->(n) WHERE NOT (n)-->() AND n.community = -1 AND m.community = '{community}' AND NOT (n:Database) RETURN *"

        results = driver.session().run(query)
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
        new_request = communities_request[max_community] + node_cpu_request
        if new_request <= communities_limits[max_community]:
            # print("assegna")
            selected_node_id = max_candidate["node"]._properties["node_id"]
            print(f"Assigning node_id {selected_node_id}\t to community {max_community}")
            query = f"MATCH (m:Service) WHERE m.node_id = '{selected_node_id}' SET m.community = '{max_community}'"
            results = driver.session().run(query)
            print(results)
            communities_request[max_community] = new_request
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
        for c_key in communities_request.keys():
            if communities_request[c_key] < min_community_request:
                min_community_request = communities_request[c_key]
                min_community = c_key

        communities_request[min_community] = communities_request[min_community]+ first_candidate["node"]._properties["cpu_request"]

        selected_node_id = first_candidate["node"]._properties["node_id"]
        print(f"Assigning node_id {selected_node_id} \t\t to community min {min_community}")
        query = f"MATCH (m:Service) WHERE m.node_id = '{selected_node_id}' SET m.community = '{min_community}'"
        results = driver.session().run(query)
        print(results)



def mark_communities():

    i = 0
    # nx_utils.save_graph(f'plots2/{i}')
    i += 1
    init_communities()
    while get_not_in_communities_count() > 0:
        mark_node(leaf=False)
        # nx_utils.save_graph(f'plots2/{i}')
        i += 1

    print("#############################  leaf  #############################")
    while get_leaf_services_not_in_communities_count() > 0:
        mark_node(leaf=True)
        # nx_utils.save_graph(f'plots2/{i}')
        i += 1

    print("#############################  dangling  #############################")
    mark_dangling_nodes()

    print(f'community_limits \t {communities_limits}')
    print(f'community_requests \t {communities_request}')

if __name__ == '__main__':
    mark_communities()