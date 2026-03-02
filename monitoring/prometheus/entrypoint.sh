#!/bin/sh
set -e

# Prometheus doesn't support environment variable substitution in its config natively.
# So we treat prometheus.yml as a template (mounted at /etc/prometheus/prometheus.yml.template)
# and use sed to substitute ${VAR} placeholders with their actual runtime values from the
# environment (injected by Docker Compose via env_file). The result is written to /tmp/prometheus.yml.
sed \
  -e "s|\${PUSHGATEWAY_HOST}|${PUSHGATEWAY_HOST}|g" \
  -e "s|\${PUSHGATEWAY_PORT}|${PUSHGATEWAY_PORT}|g" \
  -e "s|\${PROMETHEUS_PORT}|${PROMETHEUS_PORT}|g" \
  /etc/prometheus/prometheus.yml.template > /tmp/prometheus.yml

# Hand off to the real Prometheus binary, forwarding all CMD args from docker-compose
# (e.g. --config.file, --storage.tsdb.path, --web.external-url, etc.)
exec /bin/prometheus "$@"
