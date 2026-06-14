CREATE TABLE IF NOT EXISTS public.patent (
    id VARCHAR(20) PRIMARY KEY,
    title TEXT,
    num_claims INTEGER
);

-- Copy data from patents.csv
COPY public.patent(id, title, num_claims)
FROM '/docker-entrypoint-initdb.d/patents.csv'
DELIMITER ','
CSV HEADER;
