#!/usr/bin/env bash
# Initialisation de la plateforme après le premier "docker compose up -d"
set -e
echo "[1/3] Topic Kafka security-logs"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic security-logs --partitions 3 --replication-factor 1

echo "[2/3] Arborescence HDFS"
docker exec namenode hdfs dfs -mkdir -p /security-logs/raw /security-logs/processed /security-logs/alerts /security-logs/checkpoints

echo "[3/3] Index Elasticsearch"
curl -s -X PUT localhost:9200/security-events -H 'Content-Type: application/json' -d '{"mappings":{"properties":{
 "timestamp":{"type":"date"},"host":{"type":"keyword"},"source":{"type":"keyword"},"systeme":{"type":"keyword"},
 "type_evenement":{"type":"keyword"},"utilisateur":{"type":"keyword"},"adresse_ip":{"type":"keyword"},"message":{"type":"text"},
 "severite":{"type":"keyword"},"niveau_severite":{"type":"integer"},"categorie":{"type":"keyword"},"resultat":{"type":"keyword"},
 "event_id":{"type":"keyword"},"http_status":{"type":"integer"},"http_url":{"type":"keyword"},"log_type":{"type":"keyword"},
 "id_evenement":{"type":"keyword"}}}}'; echo
curl -s -X PUT localhost:9200/security-alerts -H 'Content-Type: application/json' -d '{"mappings":{"properties":{
 "timestamp":{"type":"date"},"fin_fenetre":{"type":"date"},"regle":{"type":"keyword"},"description":{"type":"text"},
 "cle":{"type":"keyword"},"type_cle":{"type":"keyword"},"nb_evenements":{"type":"integer"},"severite":{"type":"keyword"},
 "host":{"type":"keyword"},"id_alerte":{"type":"keyword"}}}}'; echo
echo "Initialisation terminée. Redémarrez Spark : docker compose restart spark"
