import time

from neo4j import GraphDatabase
import networkx as nx
import pandas as pd

#import GreedyCommunityConstrained as GreedyCommunityConstrained
import neo4j_utils_eng


# # @TODO togliere da qui il driver
# driver = GraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "password"))






if __name__ == '__main__':

    # services = [
    #     {"serviceId": "ts-system-proxy", "constraints": {
    #         "carbonIntensity": 0.53,
    #         "cpu": 2,
    #         "ram": 8589934592,
    #         "storage": 0,
    #         "gpu": 0
    #     }
    #      },
    #     {"serviceId": "ts-contacts-service", "constraints": {
    #         "carbonIntensity": 0.53,
    #         "cpu": 2,
    #         "ram": 8589934592,
    #         "storage": 0,
    #         "gpu": 0
    #     }
    #      },
    #     {"serviceId": "Data-Lake-DB", "constraints": {
    #         "carbonIntensity": 0.53,
    #         "cpu": 2,
    #         "ram": 8589934592,
    #         "storage": 0,
    #         "gpu": 0
    #     }
    #      }
    # ]
    #
    # edges = [
    #     {"source": "ts-system-proxy", "dest": "Data-Lake-DB", "total_frequency": 10},
    #     {"source": "ts-system-proxy", "dest": "ts-contacts-service", "total_frequency": 5},
    #     {"source": "ts-contacts-service", "dest": "Data-Lake-DB", "total_frequency": 7}
    # ]
    #
    # db_node_mapping = {'Data-Lake-DB': 'node-2'}
    #
    # node_info = [{'cpu': 4, 'nodeId': 'node-1', 'ram': 17179869184}, {'cpu': 44, 'nodeId': 'node-2', 'ram': 8589934592000000}]
    #
    # placements = calculate_deployments(services, edges, db_node_mapping, node_info)
    #
    # print(placements)
    pass

