[![Python application](https://github.com/imperial-genomics-facility/igf-airflow-hpc/actions/workflows/python-app.yml/badge.svg)](https://github.com/imperial-genomics-facility/igf-airflow-hpc/actions/workflows/python-app.yml)

# Apache Airflow pipelines for IGF
This repository contains Apache Airflow DAG files, created and maintained by the **NIHR Imperial BRC Genomics Facility**, for the processing of genomics sequencing datasets.


**How to setup Airflow on HPC?**
To set up Apache Airflow on an HPC cluster, refer to this [blog post](https://avik-datta-15.medium.com/how-to-setup-apache-airflow-on-hpc-cluster-ea2575764b43)

## Requirements:
  * Docker
  * Python3.9 (for HPC workers)

## How to install?

### Setup Docker environment
  * **Clone the repository**
  ```bash
  git clone https://github.com/imperial-genomics-facility/igf-airflow-hpc.git
  ```

  * **Build custom docker image**
  ```bash
  cd igf-airflow-hpc/docker/airflow
  docker build -t airflow:v2.6.2 .
  ```

  * **Build PGBouncer image**
  ```bash
  cd igf-airflow-hpc/docker/pgbouncer
  docker build -t pgbouncer:v1.19.0 .
  ```

  * **Setup environment files**
    
    Copy following template files:

    * _igf-airflow-hpc/docker/env_template/airflow_db_env_template_
    * _igf-airflow-hpc/docker/env_template/airflow_env_template_

  * **Setup PGBouncer connections and start Airflow**

    * Follow these instructions: [README](docker/README)

  * **Get python core library**
  ```bash
  git clone https://github.com/imperial-genomics-facility/data-management-python.git
  ```

  ### Setup HPC environment

  * Clone repository and install Python environment
  ```bash
  git clone https://github.com/imperial-genomics-facility/igf-airflow-hpc.git
  git clone https://github.com/imperial-genomics-facility/data-management-python.git
  pip install -r requirements_2.6.2.txt  # For compatibility with Apache Airflow v2.6.2
  export PYTHONPATH=/PATH/data-management-python
  ```

## License
This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.
