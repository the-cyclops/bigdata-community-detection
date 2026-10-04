# Distributed Community Detection with Spark and GraphFrames

Lo studio valuta la scalabilità e l'efficacia di algoritmi distribuiti di rilevamento delle comunità su grafi complessi (*email-Eu-core* e *Amazon product co-purchasing*) utilizzando **Apache Spark** e **GraphFrames**, confrontando un'implementazione personalizzata LPA basata su RDD con la versione nativa GraphFrames (GraphX) e le implementazioni di Louvain, FastGreedy e $k$-Clique Percolation tratte dal paper [*Large-Scale Graphs Community Detection using Spark GraphFrames*](papers-pdf/Large-Scale%20Graphs%20Community%20Detection%20using%20Spark%20GraphFrames.pdf)


## Struttura della Repository

```text
├── cluster-scripts/                        # Script eseguiti sul cluster Spark
│   ├── run-louvian-amazon.py               # Louvain su dataset Amazon (framework Apostol et al.)
│   ├── run-lpa-amazon.py                   # LPA custom su RDD (Amazon)
│   ├── run-lpa-mail.py                     # LPA custom su RDD (Email)
│   ├── run-lpagf-amazon.py                 # LPA nativo GraphFrames/GraphX (Amazon)
│   ├── run-lpagf-mail.py                   # LPA nativo GraphFrames/GraphX (Email)
│   ├── run-papergf-amazon.py               # FastGreedy e k-clique su GraphFrames (Amazon)
│   └── run-papergf-mail.py                 # Louvain, FastGreedy e k-clique su GraphFrames (Email)
│
├── papers-pdf/                             # Articoli scientifici di riferimento
│   ├── Amazon - Defining and Evaluating Network Communities based on Ground-truth.pdf
│   ├── Email - Local Higher-Order Graph Clustering.pdf
│   ├── Large-Scale Graphs Community Detection using Spark GraphFrames.pdf
│   └── LPA-Near linear time algorithm to detect community structures in large-scale networks.pdf
│
├── results-pdf/                            # Grafici generati in formato PDF
│   ├── amazon_distribuition_results.pdf    # Distribuzione dimensioni comunità (Amazon)
│   ├── amazon_time_results.pdf             # Tempi di esecuzione e speedup (Amazon)
│   ├── email_distribuition_results.pdf     # Distribuzione dimensioni comunità (Email)
│   └── email_time_results.pdf              # Tempi di esecuzione e speedup (Email)
│
├── analyze_amazon.ipynb                    # Analisi risultati, calcolo metriche e grafici (Amazon)
├── analyze_email.ipynb                     # Analisi risultati, calcolo metriche e grafici (Email)
├── GraphFrames_Community_Detection_EU_EMAIL.ipynb # Notebook esplorativo preliminare (Email)
├── Distributed Community Detection with Spark and GraphFrames # Relazione finale di progetto (PDF)
└── README.md
```
