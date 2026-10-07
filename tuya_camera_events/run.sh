#!/usr/bin/with-contenv bashio

set -e

echo "Starting Tuya Camera Events..."

export TUYA_ACCESS_ID="$(bashio::config 'access_id')"
export TUYA_ACCESS_SECRET="$(bashio::config 'access_secret')"
export TUYA_ENVIRONMENT="$(bashio::config 'environment')"

if [ -z "${TUYA_ACCESS_ID}" ]; then
    bashio::log.fatal "Tuya Access ID is not configured."
    exit 1
fi

if [ -z "${TUYA_ACCESS_SECRET}" ]; then
    bashio::log.fatal "Tuya Access Secret is not configured."
    exit 1
fi

bashio::log.info "Tuya environment: ${TUYA_ENVIRONMENT}"
bashio::log.info "Starting Tuya Pulsar consumer..."

exec java -jar /app/target/tuya-camera-events-0.1.0.jar