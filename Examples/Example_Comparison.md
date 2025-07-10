CREATE TABLE Calgary_Lines AS
    SELECT * FROM 'simplifiedCSV/2024-2025/team_lines.parquet/*.parquet' WHERE Season = '2024/2025' AND TeamId = 19;

CREATE TABLE Calgary_Players AS
    SELECT * FROM 'simplifiedCSV/2024-2025/players.parquet/*.parquet' WHERE Season = '2024/2025' AND TeamId = 19;

COPY Calgary_Lines TO 'Calgary_Lines.csv' (HEADER, DELIMITER ',');
COPY Calgary_Players TO 'Calgary_Players.csv' (HEADER, DELIMITER ',');