# Relecture des fichiers Parquet stockés dans HDFS et agrégations analytiques (T7)
from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.appName("analyse-hdfs")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
H = "hdfs://namenode:8020/security-logs"

ev = spark.read.parquet(H + "/processed")
print("\n===== Schéma des événements stockés =====");          ev.printSchema()
print("===== Nombre total d'événements :", ev.count(), "=====")
print("===== Événements par source =====");                    ev.groupBy("source").count().orderBy(F.desc("count")).show()
print("===== Événements par machine et système =====");        ev.groupBy("host", "systeme").count().show()
print("===== Échecs par type d'événement =====")
ev.filter(F.col("resultat") == "echec").groupBy("type_evenement").count().orderBy(F.desc("count")).show(truncate=False)
print("===== Top adresses IP à l'origine d'échecs =====")
ev.filter((F.col("resultat") == "echec") & F.col("adresse_ip").isNotNull()) \
  .groupBy("adresse_ip").count().orderBy(F.desc("count")).show(10)
print("===== Volume par heure =====")
ev.groupBy(F.date_format("ts", "yyyy-MM-dd HH:00").alias("heure")).count().orderBy("heure").show(48)
print("===== Lecture ciblée grâce au partitionnement (source=ssh uniquement) =====")
ev.filter(F.col("source") == "ssh").select("ts", "utilisateur", "adresse_ip", "type_evenement").orderBy("ts").show(10, truncate=False)

al = spark.read.parquet(H + "/alerts")
print("===== Alertes par règle =====")
al.groupBy("regle").agg(F.count("*").alias("lignes"), F.max("nb_evenements").alias("max_evenements")).show(truncate=False)
al.select("regle", "cle", "debut", "nb_evenements", "severite").orderBy("debut").show(truncate=False)

print("===== Données brutes (raw) :", spark.read.parquet(H + "/raw").count(), "messages Kafka archivés =====")
