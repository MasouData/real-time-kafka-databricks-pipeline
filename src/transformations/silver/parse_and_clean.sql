-- CREATE OR REPLACE TEMP VIEW bronze_view
-- AS
-- SELECT
--   key, topic, partition, offset, 
--   timestamp,
--   from_json(
--     cast(value as string),
--     'id INT, message STRING'
--   ) AS parsed_value
-- FROM STREAM(bronze_table);

-- CREATE OR REPLACE STREAMING TABLE silver_table (
--     CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
--     CONSTRAINT valid_message EXPECT (message IS NOT NULL) ON VIOLATION DROP ROW
-- )
-- comment "raw table change to silver and clean table"
-- as select cast(parsed_value.id as int) as id, cast(parsed_value.message as string) as message, time from stream(bronze_view);

CREATE OR REPLACE STREAMING TABLE silver_table (
    CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_message EXPECT (message IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Parsed and cleaned Kafka messages"
AS
SELECT
  CAST(from_json(CAST(value AS STRING), 'id INT, message STRING').id AS INT) AS id,
  CAST(from_json(CAST(value AS STRING), 'id INT, message STRING').message AS STRING) AS message,
  topic,
  partition,
  offset,
  timestamp
FROM STREAM(bronze_table);