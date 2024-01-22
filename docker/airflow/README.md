## Set up

### Secret key generation
```bash
    python -c 'import secrets; print(secrets.token_hex(16));'
```

### Fernet key generation
[Airflow doc - Fernet](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/secrets/fernet.html)

```bash
    python -c "from cryptography.fernet import Fernet; fernet_key = Fernet.generate_key(); print(fernet_key.decode())"
```

* init
AIRFLOW_UID="$(id -u)" GID="$(id -g)" docker-compose -f docker-compose.yml -p igf_airflow_v
3 up airflow_init