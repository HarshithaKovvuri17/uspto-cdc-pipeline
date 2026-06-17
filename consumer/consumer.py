import os
import json
import time
import logging
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("cdc-consumer")

# Read environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.public.patent")
POSTGRES_HOST = os.getenv("POSTGRES_TARGET_HOST", "postgres-target")
POSTGRES_PORT = os.getenv("POSTGRES_TARGET_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_TARGET_DB", "postgres_target")
POSTGRES_USER = os.getenv("POSTGRES_TARGET_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_TARGET_PASSWORD", "postgres")

def get_db_connection():
    """Establish and return target database connection."""
    while True:
        try:
            logger.info(f"Connecting to target database at {POSTGRES_HOST}:{POSTGRES_PORT}...")
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Target database connection established successfully.")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to target database: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def get_kafka_consumer():
    """Establish and return Kafka consumer."""
    while True:
        try:
            from kafka import KafkaConsumer
            logger.info(f"Connecting to Kafka brokers at {KAFKA_BOOTSTRAP_SERVERS}...")
            # Initialize consumer. Note: we disable auto_commit to commit manually after DB updates.
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='patent-cdc-consumer-group'
            )
            logger.info(f"Subscribed to topic '{KAFKA_TOPIC}' successfully.")
            return consumer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka or subscribe: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def process_message(conn, payload):
    """Processes a single Debezium event and updates the target database in a single transaction."""
    op = payload.get("op")
    if not op:
        logger.warning("No 'op' field in payload, skipping message.")
        return

    logger.info(f"Processing operation: '{op}'")
    cur = conn.cursor()
    try:
        if op in ('c', 'r'): # Create or Read (snapshot)
            after = payload.get("after")
            if not after:
                logger.warning("Operation is create/read but 'after' is missing/null.")
                return

            id_val = after.get("id")
            title_val = after.get("title")
            claims_val = after.get("num_claims")

            # 1. Update patent_current_state
            cur.execute("""
                INSERT INTO public.patent_current_state (id, title, num_claims)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title, num_claims = EXCLUDED.num_claims;
            """, (id_val, title_val, claims_val))

            # 2. Update patent_history (idempotent logic)
            cur.execute("""
                SELECT history_id, title, num_claims FROM public.patent_history
                WHERE id = %s AND valid_to IS NULL;
            """, (id_val,))
            active = cur.fetchone()

            if not active:
                cur.execute("""
                    INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to)
                    VALUES (%s, %s, %s, NOW(), NULL);
                """, (id_val, title_val, claims_val))
                logger.info(f"Inserted initial history record for ID {id_val}")
            else:
                hist_id, hist_title, hist_claims = active
                if hist_title != title_val or hist_claims != claims_val:
                    cur.execute("""
                        UPDATE public.patent_history
                        SET valid_to = NOW()
                        WHERE history_id = %s;
                    """, (hist_id,))
                    cur.execute("""
                        INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to)
                        VALUES (%s, %s, %s, NOW(), NULL);
                    """, (id_val, title_val, claims_val))
                    logger.info(f"Updated history record for ID {id_val} due to value difference in snapshot/create.")

        elif op == 'u': # Update
            after = payload.get("after")
            if not after:
                logger.warning("Operation is update but 'after' is missing/null.")
                return

            id_val = after.get("id")
            title_val = after.get("title")
            claims_val = after.get("num_claims")

            # 1. Update patent_current_state
            cur.execute("""
                INSERT INTO public.patent_current_state (id, title, num_claims)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title, num_claims = EXCLUDED.num_claims;
            """, (id_val, title_val, claims_val))

            # 2. Update patent_history
            cur.execute("""
                SELECT history_id, title, num_claims FROM public.patent_history
                WHERE id = %s AND valid_to IS NULL;
            """, (id_val,))
            active = cur.fetchone()

            if not active:
                # If no active record, insert new
                cur.execute("""
                    INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to)
                    VALUES (%s, %s, %s, NOW(), NULL);
                """, (id_val, title_val, claims_val))
                logger.info(f"No active history row found for ID {id_val} on update. Inserted new history row.")
            else:
                hist_id, hist_title, hist_claims = active
                if hist_title != title_val or hist_claims != claims_val:
                    # Values changed: close current record and insert new one
                    cur.execute("""
                        UPDATE public.patent_history
                        SET valid_to = NOW()
                        WHERE history_id = %s;
                    """, (hist_id,))
                    cur.execute("""
                        INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to)
                        VALUES (%s, %s, %s, NOW(), NULL);
                    """, (id_val, title_val, claims_val))
                    logger.info(f"SCD2: Closed active row (ID {id_val}, hist_id {hist_id}) and created new active row.")
                else:
                    logger.info(f"Duplicate update event received for ID {id_val} with no change in values. Ignored for history.")

        elif op == 'd': # Delete
            before = payload.get("before")
            if not before:
                logger.warning("Operation is delete but 'before' is missing/null.")
                return

            id_val = before.get("id")

            # 1. Delete from patent_current_state
            cur.execute("DELETE FROM public.patent_current_state WHERE id = %s;", (id_val,))
            logger.info(f"Deleted ID {id_val} from current state.")

            # 2. Close active history row in patent_history
            cur.execute("""
                SELECT history_id FROM public.patent_history
                WHERE id = %s AND valid_to IS NULL;
            """, (id_val,))
            active = cur.fetchone()

            if active:
                hist_id = active[0]
                cur.execute("""
                    UPDATE public.patent_history
                    SET valid_to = NOW()
                    WHERE history_id = %s;
                """, (hist_id,))
                logger.info(f"SCD2: Closed active history row (ID {id_val}, hist_id {hist_id}) on delete.")
            else:
                logger.warning(f"Delete event received for ID {id_val} but no active history row was found.")

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Transaction failed, changes rolled back: {e}")
        raise e
    finally:
        cur.close()

def main():
    conn = get_db_connection()
    consumer = get_kafka_consumer()

    logger.info("Starting consuming events...")
    try:
        for msg in consumer:
            if msg.value is None:
                # Handle tombstone messages
                logger.info(f"Received tombstone message for key: {msg.key}. Skipping.")
                consumer.commit()
                continue

            logger.info(f"Received message: Partition={msg.partition}, Offset={msg.offset}")
            
            # Debezium messages may have schema and payload envelopes
            # If schemas are enabled in connector config, value contains 'schema' and 'payload'
            # If schemas are disabled, value is the payload itself.
            val = msg.value
            if isinstance(val, dict) and "payload" in val:
                payload = val["payload"]
            else:
                payload = val

            if payload is None:
                logger.warning("Parsed message payload is None. Skipping.")
                consumer.commit()
                continue

            try:
                process_message(conn, payload)
                # Successfully processed, commit offsets
                consumer.commit()
            except psycopg2.InterfaceError:
                # If database connection is dead, try reconnecting
                logger.error("Database connection lost! Reconnecting...")
                conn = get_db_connection()
                # Do not commit offsets, so this message will be retried
            except Exception as e:
                # Other errors - we will log and try again (message will be retried if offset is not committed)
                logger.error(f"Error processing event, will retry message: {e}")
                time.sleep(2)
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
