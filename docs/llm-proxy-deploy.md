# Deploying the Shoal LLM proxy

The LLM proxy has been renamed to `shoal-llm-gateway`; the chart values now use
`llmGateway`. See [the deployment guide](llm-gateway-deploy.md) for configuration
and migration instructions.

## Listener transport

The listener remains plaintext HTTP and **requires a TLS-terminating hop**
(ingress, sidecar or mTLS mesh) outside loopback development. That hop owns
certificate rotation. ClusterIP does not encrypt pod-network traffic.

The chart refuses a non-ClusterIP `llmGateway.service.type` unless
`llmGateway.service.allowPlaintext: true` explicitly acknowledges the exposure.
This boolean defaults to `false`; setting it does not configure TLS or verify
the terminating hop. Configure that protection separately before exposure.
`admission.allowPlaintext` does not acknowledge the listener, and neither setting
permits a remote plaintext upstream provider.

See [Listener transport](llm-gateway-deploy.md#listener-transport-require-a-terminating-hop)
for the complete transport contract.
