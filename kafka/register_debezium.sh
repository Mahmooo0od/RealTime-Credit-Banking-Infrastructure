#!/bin/bash
echo "Registering Debezium PostgreSQL Connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '{
  "name": "postgres-debezium-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres_db",
    "database.port": "5432",
    "database.user": "iscore_user",
    "database.password": "iscore_password",
    "database.dbname": "home_credit_db",
    "topic.prefix": "banking"
  }
}'
