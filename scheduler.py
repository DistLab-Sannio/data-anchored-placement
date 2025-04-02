
from flask import Flask, request, jsonify
from prometheus_api_client import PrometheusConnect

import placement as pl


app = Flask(__name__)


PROMETHEUS_URL = "http://172.31.1.212:31300"
prometheus = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)



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

    placements = pl.calculate_deployments(services, edges, db_node_mapping, node_info)

    response = {
        "appId": data.get("application", {}).get("appId", "unknown"),
        "placements": placements
    }

    return jsonify(response)


if __name__ == '__main__':
    app.run(port=5001, debug=True)