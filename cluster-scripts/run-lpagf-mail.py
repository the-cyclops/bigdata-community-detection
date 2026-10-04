import os
import time
from pyspark.sql import SparkSession
from graphframes import GraphFrame

def save_driver_communities(communities, output_path):
  flat_comms = [
      (node, comm_id)
      for comm_id, comm in enumerate(communities)
      for node in comm
  ]

  sc.parallelize(flat_comms).sortByKey().map(
      lambda x: f"{x[0]},{x[1]}"
  ).saveAsTextFile(output_path)

def save_df_communities(df_result, output_path):
  df_result.select("id", "label").rdd.map(
      lambda row: (row["id"], row["label"])
  ).sortByKey().map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(output_path)

def log_benchmark(algo_name, elapsed_time):
    with open(f"{RUN_DIR}/gf_benchmark_times.txt", "a") as f:
        f.write(f"{algo_name},{elapsed_time:.4f}\n")

if __name__ == "__main__":

    APP_TAG ="LPA_GF"
    spark = SparkSession.builder \
    .appName(f"CD-{APP_TAG}") \
    .getOrCreate()

    sc = spark.sparkContext

    # Checkpoint LPA 
    checkpoint_path = f"./spark_checkpoints_{APP_TAG}"
    os.makedirs(checkpoint_path, exist_ok=True)
    sc.setCheckpointDir(checkpoint_path)
    sc.setLogLevel("WARN")

    master_val = sc.master  # Restituisce es. "local[6]"
    mem_val = sc.getConf().get("spark.driver.memory", "default")  # Restituisce es. "8g"
    clean_master = master_val.replace("[", "").replace("]", "").replace("*", "all")
    RUN_DIR = f"{APP_TAG}_{clean_master}_{mem_val}" 
    os.makedirs(RUN_DIR, exist_ok=True)

    # 1. Carica e calcola pesi (puro RDD)
    textfile = "email-Eu-core.txt"
    
    raw_edges = sc.textFile(textfile)
    data = raw_edges.map(lambda x: x.split())

    # Pulisci, converti a int e aggrega
    # ((u,v),w)
    edges_weighted = (
        data.filter(lambda x: x[0] != x[1])
        .map(lambda x: ((min(int(x[0]), int(x[1])), max(int(x[0]), int(x[1]))), 1))
        .reduceByKey(lambda a, b: a + b)
    )

    # 2. Genera mapping deterministico 0...N-1 (puro RDD)
    unique_nodes = (
        edges_weighted.flatMap(lambda x: [x[0][0], x[0][1]])
        .distinct()
        .sortBy(lambda x: x)
    )

    # RDD di tuple: (u, new_u)
    node_map = unique_nodes.zipWithIndex().cache()

    # 3. Rimappa gli archi con due join (stile PageRank del lab)
    # (u, (v, weight))
    edges_by_u = edges_weighted.map(lambda x: (x[0][0], (x[0][1], x[1])))

    # Join su u -> (u, ((v, weight), new_u))
    #  map -> (v, (new_u, weight))
    edges_mapped_u = edges_by_u.join(node_map).map(
        lambda x: (x[1][0][0], (x[1][1], x[1][0][1]))
    )

    # Join su v -> (v, ((new_u, weight),new_v))
    # map -> (new_u, new_v, weight)
    edges_mapped = edges_mapped_u.join(node_map).map(
        lambda x: (x[1][0][0], x[1][1], x[1][0][1])
    )

    # 4. Creazione dei DataFrame per GraphFrames, friend is needed for compatibility with paper code
    # Prepariamo gli archi bidirezionali come tuple native: (src, dst, "friend", weight)
    edges_rdd = edges_mapped.flatMap(
        lambda x: [(x[0], x[1], "friend", x[2]), (x[1], x[0], "friend", x[2])]
    )
    edges_df = edges_rdd.toDF(["src", "dst", "relationship", "weight"]).cache()

    # Prepariamo i vertici come tuple native: (str(id), id, [id])
    vertices_rdd = node_map.map(lambda x: (str(x[1]), x[1], [x[1]]))
    vertices_df = vertices_rdd.toDF(["name", "id", "community"]).cache()

    # trigger lazy eval
    vertices_df.count()
    edges_df.count()

    g = GraphFrame(vertices_df, edges_df)

    # 1. Carica Ground Truth: (id_originale, dept)
    gt_raw = sc.textFile("email-Eu-core-department-labels.txt").map(
        lambda line: (int(line.split()[0]), int(line.split()[1]))
    )

    # 2. Join con node_map: (id_originale, (dept, id_rimappato)) -> (id_rimappato, dept)
    gt_mapped = (
        gt_raw.join(node_map)
        .map(lambda x: (x[1][1], x[1][0]))
        .sortByKey()  # Ordina da 0 a N-1
    )

    # 3. Salva la GT rimappata in locale/storage
    gt_mapped.map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(f"{RUN_DIR}/gt_mapped_gf")

    # lpa 
    t1 = time.time()
    result_lpa = g.labelPropagation(maxIter=10).cache()
    result_lpa.count()  # trigger the lazy evaluation
    t_lpa_gf = time.time() - t1

    print("LPA Execution Time:",t_lpa_gf)

    save_df_communities(result_lpa, f"{RUN_DIR}/results_lpa_gf")
    log_benchmark("LPA_GraphFrames", t_lpa_gf)

    spark.stop()