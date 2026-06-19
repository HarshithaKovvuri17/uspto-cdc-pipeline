#!/usr/bin/env bash
set -e

# Helper functions to query DBs
query_source() {
  docker exec -i postgres-source psql -U postgres -d postgres_source -t -A -c "$1"
}

query_target() {
  docker exec -i postgres-target psql -U postgres -d postgres_target -t -A -c "$1"
}

echo "=== Starting Verification Script ==="

# Reset source and target databases to initial state (idempotency support)
echo "Resetting source and target databases to clean initial state..."
query_source "DELETE FROM public.patent WHERE id = '10000005';" >/dev/null 2>&1 || true
query_source "INSERT INTO public.patent (id, title, num_claims) VALUES ('10000001', 'System and method for real-time data streaming and processing', 8) ON CONFLICT (id) DO NOTHING;" >/dev/null 2>&1 || true
query_source "UPDATE public.patent SET title = 'Method and system for blockchain based smart contracts' WHERE id = '10000000';" >/dev/null 2>&1 || true

query_target "DELETE FROM public.patent_current_state WHERE id = '10000005';" >/dev/null 2>&1 || true
query_target "DELETE FROM public.patent_history WHERE id = '10000005';" >/dev/null 2>&1 || true
query_target "INSERT INTO public.patent_current_state (id, title, num_claims) VALUES ('10000001', 'System and method for real-time data streaming and processing', 8) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, num_claims = EXCLUDED.num_claims;" >/dev/null 2>&1 || true
query_target "DELETE FROM public.patent_history WHERE id = '10000001';" >/dev/null 2>&1 || true
query_target "INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to) VALUES ('10000001', 'System and method for real-time data streaming and processing', 8, NOW(), NULL);" >/dev/null 2>&1 || true
query_target "UPDATE public.patent_current_state SET title = 'Method and system for blockchain based smart contracts' WHERE id = '10000000';" >/dev/null 2>&1 || true
query_target "DELETE FROM public.patent_history WHERE id = '10000000';" >/dev/null 2>&1 || true
query_target "INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to) VALUES ('10000000', 'Method and system for blockchain based smart contracts', 15, NOW(), NULL);" >/dev/null 2>&1 || true

echo "Waiting 3 seconds for CDC pipeline to stabilize..."
sleep 3

# 1. Wait for target tables to be populated with initial snapshot (5 records)
echo "Checking initial snapshot state..."
MAX_RETRIES=20
RETRY_COUNT=0
while true; do
  SOURCE_COUNT=$(query_source "SELECT COUNT(*) FROM public.patent;")
  TARGET_CURRENT_COUNT=$(query_target "SELECT COUNT(*) FROM public.patent_current_state;")
  TARGET_HISTORY_COUNT=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE valid_to IS NULL;")
  
  echo "Current counts -> Source: $SOURCE_COUNT, Target Current: $TARGET_CURRENT_COUNT, Target Active History: $TARGET_HISTORY_COUNT"
  
  if [ "$SOURCE_COUNT" = "$TARGET_CURRENT_COUNT" ] && [ "$SOURCE_COUNT" = "$TARGET_HISTORY_COUNT" ] && [ "$SOURCE_COUNT" -gt 0 ]; then
    echo "Initial snapshot successfully synchronized!"
    break
  fi
  
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "Timed out waiting for initial snapshot synchronization."
    exit 1
  fi
  sleep 3
done

# Verify all history records have valid_to as NULL
NULL_VAL_COUNT=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE valid_to IS NOT NULL;")
if [ "$NULL_VAL_COUNT" != "0" ]; then
  echo "Assertion Failed: Some snapshot history records have valid_to set to non-null value!"
  exit 1
fi
echo "Initial snapshot assertions passed."

# 2. Test INSERT operation
echo "Testing INSERT operation..."
query_source "INSERT INTO public.patent (id, title, num_claims) VALUES ('10000005', 'Method and apparatus for quantum error correction', 10);"

# Wait for replication
MAX_RETRIES=15
RETRY_COUNT=0
while true; do
  INSERTED_CURRENT=$(query_target "SELECT COUNT(*) FROM public.patent_current_state WHERE id = '10000005';")
  INSERTED_HISTORY=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000005' AND valid_to IS NULL;")
  
  if [ "$INSERTED_CURRENT" = "1" ] && [ "$INSERTED_HISTORY" = "1" ]; then
    echo "INSERT verification passed."
    break
  fi
  
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "Assertion Failed: Inserted row '10000005' not found in target tables after timeout!"
    exit 1
  fi
  sleep 1
done

# 3. Test UPDATE operation (SCD Type 2)
echo "Testing UPDATE operation (SCD Type 2)..."
query_source "UPDATE public.patent SET title = 'Method and system for blockchain based smart contracts v2' WHERE id = '10000000';"

# Wait for replication
MAX_RETRIES=15
RETRY_COUNT=0
while true; do
  UPDATED_TITLE=$(query_target "SELECT title FROM public.patent_current_state WHERE id = '10000000';")
  HIST_TOTAL_COUNT=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000000';")
  HIST_CLOSED_COUNT=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000000' AND valid_to IS NOT NULL;")
  HIST_ACTIVE_COUNT=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000000' AND valid_to IS NULL;")
  
  if [ "$UPDATED_TITLE" = "Method and system for blockchain based smart contracts v2" ] && \
     [ "$HIST_TOTAL_COUNT" = "2" ] && \
     [ "$HIST_CLOSED_COUNT" = "1" ] && \
     [ "$HIST_ACTIVE_COUNT" = "1" ]; then
    
    HIST_ACTIVE_TITLE=$(query_target "SELECT title FROM public.patent_history WHERE id = '10000000' AND valid_to IS NULL;")
    if [ "$HIST_ACTIVE_TITLE" = "Method and system for blockchain based smart contracts v2" ]; then
      echo "UPDATE verification passed."
      break
    fi
  fi
  
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "Assertion Failed: Update test timed out or assertions failed!"
    echo "Found title: $UPDATED_TITLE, total hist: $HIST_TOTAL_COUNT, closed: $HIST_CLOSED_COUNT, active: $HIST_ACTIVE_COUNT"
    exit 1
  fi
  sleep 1
done

# 4. Test DELETE operation
echo "Testing DELETE operation..."
query_source "DELETE FROM public.patent WHERE id = '10000001';"

# Wait for replication
MAX_RETRIES=15
RETRY_COUNT=0
while true; do
  DELETED_CURRENT=$(query_target "SELECT COUNT(*) FROM public.patent_current_state WHERE id = '10000001';")
  DELETED_ACTIVE_HIST=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000001' AND valid_to IS NULL;")
  DELETED_CLOSED_HIST=$(query_target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000001' AND valid_to IS NOT NULL;")
  
  if [ "$DELETED_CURRENT" = "0" ] && [ "$DELETED_ACTIVE_HIST" = "0" ] && [ "$DELETED_CLOSED_HIST" = "1" ]; then
    echo "DELETE verification passed."
    break
  fi
  
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "Assertion Failed: Delete test timed out or assertions failed!"
    echo "Current count: $DELETED_CURRENT, active hist: $DELETED_ACTIVE_HIST, closed hist: $DELETED_CLOSED_HIST"
    exit 1
  fi
  sleep 1
done

echo "=== All CDC Pipeline Assertions Passed Successfully! ==="
exit 0

