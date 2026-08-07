# med-reconciliation-service

med-reconciliation-service — domain: ehr

- **Port:** 8304
- **Language:** Python 3.11 + Flask
- **Database:** `ehr` (Postgres, table `med_reconciliation`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/med_reconciliation/`          |
| POST      | `/api/med_reconciliation/`          |
| GET       | `/api/med_reconciliation/<id>`      |
| PUT/PATCH | `/api/med_reconciliation/<id>`      |
| DELETE    | `/api/med_reconciliation/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** (none)
**Subscribes:** encounter.started, prescription.issued

## HTTP peer dependencies

- `prescriptions-service`
- `ehr-service`
- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
