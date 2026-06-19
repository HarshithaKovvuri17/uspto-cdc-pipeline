# USPTO CDC Pipeline — Testing Guide

All commands below map directly to the evaluation requirements. Run them from your host terminal in the **project root directory** (`d:\GPP\uspto-cdc-pipeline`) after the stack is up. 

*Note: All commands are formatted on a single line so they run flawlessly in PowerShell, Command Prompt (CMD), or Git Bash without line continuation errors.*

---

## 0. Start the Full Stack

```bash
docker-compose up --build -d
```

Wait ~2-3 minutes for all services to become healthy before running any tests.

---

## Requirement 1 — All Services Running & Healthy

**Check that all 6 services are up and healthy:**

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

**Expected:** All services show `Up` or `healthy` state.

---

## Requirement 2 — WAL Level Set to `logical` on postgres-source

```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "SHOW wal_level;"
```

**Expected output:**
```
 wal_level 
-----------
 logical
```

---

## Requirement 3 — Source `patent` Table Populated with CSV Data

```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "SELECT COUNT(*) FROM public.patent;"
```

**Expected:** Count matches the number of rows in `init-db/patents.csv` (5 rows).

To inspect all 5 initial source rows:
```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "SELECT * FROM public.patent;"
```

---

## Requirement 4 — Debezium Connector Registered & Running

```bash
docker exec -i kafka-connect curl -s http://localhost:8083/connectors/uspto-patent-connector/status
```

**Expected:** JSON response where `connector.state = "RUNNING"` and the task has `state = "RUNNING"`.

To list all registered connectors:
```bash
docker exec -i kafka-connect curl -s http://localhost:8083/connectors
```

---

## Requirement 5 — Target Tables Schema Check

```bash
# Check patent_current_state schema
docker exec -i postgres-target psql -U postgres -d postgres_target -c "\d public.patent_current_state"

# Check patent_history schema
docker exec -i postgres-target psql -U postgres -d postgres_target -c "\d public.patent_history"
```

---

## Requirement 6 — Initial Snapshot: `patent_current_state` Mirrors Source

Compare source count to target current state count:
```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "SELECT COUNT(*) FROM public.patent;"
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT COUNT(*) FROM public.patent_current_state;"
```

Verify table content equality using hashes:
```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "SELECT md5(string_agg(id || title || num_claims::text, '' ORDER BY id)) FROM public.patent;"
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT md5(string_agg(id || title || num_claims::text, '' ORDER BY id)) FROM public.patent_current_state;"
```

---

## Requirement 7 — Initial Snapshot: `patent_history` Has One Active Record Per Patent

Active records (where `valid_to IS NULL`) must equal the source count (5):
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT COUNT(*) FROM public.patent_history WHERE valid_to IS NULL;"
```

No records should have a closed validity timestamp yet (count must be 0):
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT COUNT(*) FROM public.patent_history WHERE valid_to IS NOT NULL;"
```

---

## Requirement 8 — INSERT Test

Insert a new patent into the source table:
```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "INSERT INTO public.patent (id, title, num_claims) VALUES ('10000005', 'Method and apparatus for quantum error correction', 10);"
```

Give the pipeline a few seconds to process, then verify the target current state contains the row:
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT * FROM public.patent_current_state WHERE id = '10000005';"
```

Verify that an active history record was created (`valid_to IS NULL`):
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT * FROM public.patent_history WHERE id = '10000005';"
```

---

## Requirement 9 — UPDATE Test (SCD Type 2)

Update the title on the source database:
```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "UPDATE public.patent SET title = 'Method and system for blockchain based smart contracts v2' WHERE id = '10000000';"
```

Verify that the target current state contains the updated title:
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT id, title FROM public.patent_current_state WHERE id = '10000000';"
```

Verify that the history contains exactly 2 rows for this ID (1 closed, 1 active):
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT history_id, title, valid_from, valid_to FROM public.patent_history WHERE id = '10000000' ORDER BY valid_from;"
```

---

## Requirement 10 — DELETE Test

Delete a patent from the source database:
```bash
docker exec -i postgres-source psql -U postgres -d postgres_source -c "DELETE FROM public.patent WHERE id = '10000001';"
```

Verify that the record has been deleted from the current state (returns 0 rows):
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT * FROM public.patent_current_state WHERE id = '10000001';"
```

Verify that the active history record was successfully closed (`valid_to IS NOT NULL`):
```bash
docker exec -i postgres-target psql -U postgres -d postgres_target -c "SELECT history_id, id, valid_from, valid_to FROM public.patent_history WHERE id = '10000001';"
```

---

## Requirement 11 — Run the Automated Verification Script

**On Windows PowerShell (recommended):**
```powershell
powershell -ExecutionPolicy Bypass -File verify.ps1
```

**Expected:** All assertions pass and the script outputs `=== All CDC Pipeline Assertions Passed Successfully! ===`.

---

## Requirement 12 — `.env.example` Check

Verify `.env.example` exists and contains configuration placeholders:
```powershell
Get-Content .env.example

---

## Teardown

```bash
docker-compose down -v
```
