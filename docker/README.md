* start only database containers
```bash
AIRFLOW_UID="$(id -u)" GID="$(id -g)" docker-compose -f docker-compose.yml -p igf_airflow_v3 up -d airflow_pgbouncer_v3
```
* connect to db
```bash
docker exec -it airflow_db_v3 psql -h  airflow_db_v3 -d airflow -U airflow -W
```
* get Posegres user

```sql
SELECT rolname,rolpassword FROM pg_authid where rolname='DB_USER_NAME';
```

* stop db containers
```bash
AIRFLOW_UID="$(id -u)" GID="$(id -g)" docker-compose -f docker-compose.yml -p igf_airflow_v3 down
```

* create support files
```bash
cp pgbouncer/airflow_pgbouncer.ini /home/igf/airflow_v3/pgbouncer/pgbouncer.ini
touch /home/igf/airflow_v3/pgbouncer/userlist.txt
```

* add user ids
```
"USER" "SCRAM-SHA"
```

* init

```bash
AIRFLOW_UID="$(id -u)" GID="$(id -g)" docker-compose -f docker-compose.yml -p igf_airflow_v3 up airflow_init
## wait
AIRFLOW_UID="$(id -u)" GID="$(id -g)" docker-compose -f docker-compose.yml -p igf_airflow_v3 down
```

* start airflow
```bash
AIRFLOW_UID="$(id -u)" GID="$(id -g)" docker-compose -f docker-compose.yml -p igf_airflow_v3 up -d
```
