CREATE TABLE IF NOT EXISTS public.patent_current_state (
    id VARCHAR(20) PRIMARY KEY,
    title TEXT,
    num_claims INTEGER
);

CREATE TABLE IF NOT EXISTS public.patent_history (
    history_id SERIAL PRIMARY KEY,
    id VARCHAR(20) NOT NULL,
    title TEXT,
    num_claims INTEGER,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ
);
