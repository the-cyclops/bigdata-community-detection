import os
import time
from pyspark.sql import SparkSession


def lpa_step_robust(edges, labels, iteration):
    # 1. Join edges con etichette correnti -> (src, ((dst, weight), src_label))
    joined = edges.join(labels)

    # 2. Voti pesati in entrata -> ((dst, src_label), weight)
    messages = joined.map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1]))

    # 3. Somma dei pesi per (dst, label)
    aggregated = messages.reduceByKey(lambda a, b: a + b)

    # 4. Raggruppa voti per nodo e unisci stato corrente
    node_data = (
        aggregated.map(lambda x: (x[0][0], (x[0][1], x[1])))
        .groupByKey()
        .join(labels)
    )

    def update_label(item):
        node, (votes, current_label) = item

        # Alternanza deterministica basata su id nodo e iterazione
        if (node % 2) != (iteration % 2):
            return (node, (current_label, 0))

        votes_list = list(votes)
        max_weight = max(v[1] for v in votes_list)

        top_labels = [
            label for label, weight in votes_list if weight == max_weight
        ]

        if current_label in top_labels:
            return (node, (current_label, 0))

        new_label = sorted(top_labels)[0]
        return (node, (new_label, 1))

    return node_data.map(update_label)

def run_robust_lpa(edges, labels, max_iterations=15):
    current_labels = labels

    for iteration in range(max_iterations):
        step_result = lpa_step_robust(edges, current_labels, iteration)
        
        # In-place checkpointing to break RDD lineage DAG
        step_result.checkpoint()
        
        # Force materialization with sum action to trigger checkpointing
        changed_count = step_result.map(lambda x: x[1][1]).sum()

        # Map the new labels RDD
        new_labels = step_result.map(lambda x: (x[0], x[1][0])).cache()
        new_labels.checkpoint()

        print(f"Iteration {iteration + 1}: {changed_count} nodes changed labels.")

        # Free memory of previous iteration
        if iteration > 0:
            current_labels.unpersist()

        current_labels = new_labels
        if changed_count == 0:
            print("Converged successfully.")
            break
   
    communities = (
        current_labels.map(lambda x: (x[1], x[0]))
        .groupByKey()
        .map(lambda x: set(x[1]))
    )

    current_labels.unpersist()


    return communities.collect()

def save_driver_communities(communities, output_path):
  # Appiattisce: genera coppie (nodo, id_comunita)
  flat_comms = [
      (node, comm_id)
      for comm_id, comm in enumerate(communities)
      for node in comm
  ]

  # Crea RDD, ordina e salva nello stesso formato di gt_mapped
  sc.parallelize(flat_comms).sortByKey().map(
      lambda x: f"{x[0]},{x[1]}"
  ).saveAsTextFile(output_path)

def log_benchmark(algo_name, elapsed_time):
  with open(f"{RUN_DIR}/benchmark_times.txt", "a") as f:
    f.write(f"{algo_name},{elapsed_time:.4f}\n")

if __name__ == "__main__":

    APP_TAG ="LPA_OURS"
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

    # 3. Rimappa gli archi con due join 
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

    # Create bidirectional RDD edges: (src, (dst, weight))
    edges = edges_mapped.flatMap(lambda x: [
        (x[0], (x[1], x[2])),
        (x[1], (x[0], x[2]))
    ]).cache()
    
    # Create initial labels: (node, node) using node_map index
    initial_labels = node_map.map(lambda x: (x[1], x[1])).cache()

    # Forza la materializzazione prima di avviare il cronometro
    edges.count()
    initial_labels.count()

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
    gt_mapped.map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(f"{RUN_DIR}/gt_mapped")

    t1 = time.time()
    detected_communities = run_robust_lpa(edges, initial_labels, max_iterations=10)
    t_lpa = time.time() - t1
    print("LPA Execution Time:", t_lpa)

    save_driver_communities(detected_communities, f"{RUN_DIR}/results_lpa")
    log_benchmark("LPA", t_lpa)

    spark.stop()