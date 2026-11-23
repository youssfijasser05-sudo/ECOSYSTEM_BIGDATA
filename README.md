# Collecte, centralisation et analyse Big Data des logs de sécurité

Projet ISET Nabeul — L3 RSI — Écosystème Big Data avancé (2026–2027) — Jasser Youssfi

Chaîne complète : **Filebeat / Winlogbeat → Logstash → Kafka → Spark Structured Streaming → HDFS/Parquet + Elasticsearch → Kibana**, avec 4 règles de détection.

## Architecture

| Machine | IP | Rôle |
|---|---|---|
| web01 (Ubuntu 24.04) | 192.168.100.10 | Nginx, SSH, Filebeat |
| dc01 (Windows Server 2022) | 192.168.100.20 | Active Directory, DNS, Winlogbeat |
| bigdata01 (Ubuntu 24.04 + Docker) | 192.168.100.30 | Plateforme Docker Compose |

## Services Docker

| Service | Image | Port |
|---|---|---|
| kafka | apache/kafka:4.3.1 (KRaft) | interne 9092 |
| logstash | logstash:8.19.21 | 5044 (Filebeat), 5045 (Winlogbeat) |
| namenode / datanode | apache/hadoop:3.4.1 | 9870 (interface HDFS) |
| elasticsearch | elasticsearch:8.19.21 | 127.0.0.1:9200 |
| kibana | kibana:8.19.21 | 5601 |
| spark | apache/spark:3.5.7 | 4040 (interface Spark) |

## Arborescence

```
docker-compose.yml          # les 7 services
hadoop.env                  # configuration HDFS
logstash/pipeline/          # normalisation (schéma commun) + sortie Kafka
spark/app/detection.py      # streaming : nettoyage, stockage, 4 règles
spark/app/analyse_hdfs.py   # relecture Parquet et agrégations
scripts/init.sh             # topic Kafka, dossiers HDFS, index Elasticsearch
sources/web01/filebeat.yml  # agent Linux
sources/dc01/winlogbeat.yml # agent Windows
```

## Démarrage

Prérequis : Docker + Docker Compose, 6 Go de RAM, `sudo sysctl -w vm.max_map_count=262144`.

```bash
docker compose up -d
sleep 90
./scripts/init.sh
docker compose restart spark
docker compose ps
```

Arrêt : `docker compose stop` (ne pas utiliser `down` : les données HDFS sont dans les conteneurs).

## Règles de détection (Spark)

| Règle | Condition | Fenêtre | Seuil |
|---|---|---|---|
| R1 brute force AD | échecs 4771 / 4625 pour un même compte | 5 min | ≥ 5 |
| R2 brute force SSH | ssh_failed_password depuis une même IP | 2 min | ≥ 5 |
| R3 pic d'erreurs HTTP | erreurs 4xx/5xx depuis une même IP | 1 min | ≥ 10 |
| R4 compte verrouillé | événement 4740 | immédiat | 1 |

## Commandes utiles

```bash
docker logs spark 2>&1 | grep "\[ALERTE\]"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic security-logs --from-beginning --max-messages 5
docker exec namenode hdfs dfs -ls -R /security-logs
docker run --rm --network bigdata -v "$PWD/spark/app:/opt/spark-app:ro" apache/spark:3.5.7 /opt/spark/bin/spark-submit --master local[2] /opt/spark-app/analyse_hdfs.py
```

Kibana : http://192.168.100.30:5601

## Limites

Maquette de laboratoire : un seul nœud par service, pas de TLS, sécurité Elasticsearch désactivée, comptes de test fictifs. Le contrôleur de domaine Windows (Active Directory) ne peut pas être conteneurisé sous Linux : les deux sources restent des VMs, leurs configurations sont dans `sources/`.
