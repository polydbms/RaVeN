# Architecture


## File structure

… It therefore requires medium to strong guarantees with regards to the structure of the directories it uses.

### Controller directory structure

As the controller is controlled completely by the user, it requires fewer guarantees than the host. The `benchi.py` script though still needs to be able to access the following files and directories:

| file / directory | User-created? | 
|-----|-----|
|  host configuration | yes |
|  workload definition | yes|
|  results folder | no|
|  results database |no|
|  SSH config | yes|
|  SSH key | yes |

### Host directory structure

The controller then creates a strong file structure on the host system using ssh, scp, and rsync. Internally, the structure looks as follows:

```
benchiroot/
├── config/
│   ├── system_1/
│   ├── system_n/
│   └── teardown.sh
├── data
│   ├── vector_dataset_1/
│   ├── vector-dataset_n/
│   ├── raster-dataset_1/
│   └── raster-dataset_n/
└── measurements
    ├── run_1/
    │   ├── system_config_1/
    │   │   ├── execution.csv
    │   │   ├── ingestion.csv
    │   │   └── preprocess.csv
    │   └── system_config_n/
    │       ├── execution.csv
    │       ├── ingestion.csv
    │       └── preprocess.csv
    └── run_n/
        ├── system_config_1/
        │   ├── execution.csv
        │   ├── ingestion.csv
        │   └── preprocess.csv
        └── system_config_n/
            ├── execution.csv
            ├── ingestion.csv
            └── preprocess.csv
  
```


### Logs
Benchi records a bunch of logs during runtime. All logs created on the host and the docker containers it is running are forwarded to the controller. To save space and increase legibility, Benchi truncates repeated output lines with the amount of repetitions. The logs of Benchi are not saved automatically and therefore need to be exported manually, if they shall be examined later on.


