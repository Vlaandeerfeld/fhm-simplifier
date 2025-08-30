CREATE TABLE Calgary_Lines AS
    SELECT * FROM 'simplifiedFiles/parquet/team_lines.parquet/*.parquet' WHERE Dates > '2024-10-05' AND TeamId = 19 LIMIT 1;

CREATE TABLE Calgary_Players AS
    SELECT * FROM 'simplifiedFiles/parquet/players.parquet/*.parquet' WHERE Season = '2024/2025' AND TeamId = 19;

COPY Calgary_Lines TO 'TeamLines/Calgary_Lines.csv' (HEADER, DELIMITER ',');
COPY Calgary_Players TO 'TeamLines/Calgary_Players.csv' (HEADER, DELIMITER ',');