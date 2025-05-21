import os
from flask import Flask, request, jsonify
from prometheus_api_client import PrometheusConnect

from GreedyCommunityConstrained import GreedyCommunityConstrained
from neo4j import GraphDatabase

app = Flask(__name__)

PROMETHEUS_URL = os.environ.get('PROMETHEUS_URL') if os.environ.get('PROMETHEUS_URL') else "http://prometheus:9090"
NEO4J_URL = os.environ.get('NEO4J_URL') if os.environ.get('NEO4J_URL') else 'neo4j://localhost:7687'
NEO4J_USER = os.environ.get('NEO4J_USER') if os.environ.get('NEO4J_USER') else 'neo4j'
NEO4J_PASSWORD= os.environ.get('NEO4J_PASSWORD') if os.environ.get('NEO4J_PASSWORD') else 'password'

print(f'PROMETHEUS_URL: {PROMETHEUS_URL}')
print(f'NEO4J_URL: {NEO4J_URL}')
print(f'NEO4J_USER: {NEO4J_USER}')
print(f'NEO4J_PASSWORD: {NEO4J_PASSWORD}')

prometheus = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)
driver = GraphDatabase.driver(uri=NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))

@app.route('/health', methods=['GET'])
def health():
    return "OK"


@app.route('/placement', methods=['POST'])
def placement():
    data = request.get_json()

    services = data.get("application", {}).get("services", [])

    query = 'increase(traces_service_graph_request_total[30m])'

    metrics = prometheus.custom_query(query=query)

    edges = [
        {
            'source': item['metric']['client'].split('.')[0],
            'dest': item['metric']['server'].split('.')[0],
            'total_frequency': item['value'][1]
        }
        for item in metrics
    ]

    db_services = {}
    for service in data["application"].get("services", []):

        # TODO ancorare tutto ciò che ha una region specificata e non solo ciò che finisce con db
        # TODO un servizio con più region o una region con più nodi
        # TODO se un nodo non ha nessuno ancorato, non viene assegnato nessuno a quel nodo
        if service["serviceId"].endswith("db"):
            positions = service["constraints"].get("positions", [])
            db_services[service["serviceId"]] = [
                pos["region"] if pos["region"] else pos["country"] for pos in positions
            ]

    db_node_mapping = {}
    for service, regions in db_services.items():
        for node in data["infrastructure"].get("nodes", []):
            node_region = node["properties"]["position"]["region"]
            node_country = node["properties"]["position"]["country"]

            if node_region in regions or node_country in regions:
                db_node_mapping[service] = node["nodeId"]
                break
            else:
                db_node_mapping[service] = -1

    infrastructure_data = data["infrastructure"]["nodes"]
    node_info = [
        {"nodeId": node["nodeId"], "cpu": node["properties"]["cpu"], "ram": node["properties"]["ram"]}
        for node in infrastructure_data
    ]

    greedy_manager = GreedyCommunityConstrained([node["nodeId"] for node in node_info], driver)

    placements = greedy_manager.calculate_deployments(greedy_manager, services, edges, db_node_mapping, node_info)

    response = {
        "appId": data.get("application", {}).get("appId", "unknown"),
        "placements": placements
    }

    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)