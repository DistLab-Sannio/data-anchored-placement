# Install istio

Referring to official guide [here](https://istio.io/latest/docs/setup/install/istioctl/)
Using a machine with `kubectl` enabled to access the cluster. 


```shell
curl -L https://istio.io/downloadIstio | sh -

export PATH="$PATH:<your-path-to-istioctl>/istio-1.26.0/bin"  


cat <<EOF | istioctl install -y -f -
apiVersion: install.istio.io/v1alpha1
kind: IstioOperator
spec:
  meshConfig:
    enableTracing: true
    extensionProviders:
    - name: otel-tracing
      opentelemetry:
        port: 4317
        service: opentelemetry-collector.<NAMESPACE>.svc.cluster.local
        resource_detectors:
          environment: {}
EOF

kubectl apply -f - <<EOF
apiVersion: telemetry.istio.io/v1
kind: Telemetry
metadata:
  name: otel-demo
spec:
  tracing:
  - providers:
    - name: otel-tracing
    randomSamplingPercentage: 100
    customTags:
      "my-attribute":
        literal:
          value: "default-value"
EOF

kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.26/samples/addons/prometheus.yaml
kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.26/samples/addons/kiali.yaml

```