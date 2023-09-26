
# Benchmarking Process

The benchmarking process implemented by Benchi consists of three main parts:

The main routine, the preprocess routine and the domain-specific language (DSL). Both the main and the preprocess routine are written in Python, while the DSL is specified in YAML. Together, the three parts form a six-staged process each benchmark execution will follow: First, the necessary resources for the benchmark are set up. Then, Benchi starts a preprocess phase for preparing the datasets, followed by, depending on the system, an ingestion phase for loading the datasets into the systems, which is succeeded by the execution phase where the zonal statistics are computed. Finally, the system is cleaned up. When all benchmark executions have been completed, the results produced by the queries are evaluated.
