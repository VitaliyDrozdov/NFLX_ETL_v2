
-- 4)
DO $$
DECLARE
    d DATE := '2018-01-01';
    start_time TIMESTAMP := NOW();
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN

    WHILE d <= '2018-01-31' LOOP
        CALL "DM".fill_account_balance_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
    PERFORM pg_sleep(5);
    end_time := NOW();
    duration := end_time - start_time;
    INSERT INTO "LOG".etl_log (table_name, start_time, end_time, duration)
    VALUES ('fill_account_balance_f', start_time, end_time, duration);

END $$;


-- 5)
DO $$
DECLARE
    d DATE := '2018-01-01';
    start_time TIMESTAMP := NOW();
    end_time TIMESTAMP;
    duration INTERVAL;

BEGIN

    WHILE d <= '2018-01-31' LOOP
        CALL "DM".fill_account_turnover_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
    PERFORM pg_sleep(5);

    end_time := NOW();
    duration := end_time - start_time;
    INSERT INTO "LOG".etl_log (table_name, start_time, end_time, duration)
    VALUES ('fill_account_turnover_f', start_time, end_time, duration);

END $$;


-- 6)
DO $$
DECLARE
    d DATE := '2018-02-01';
BEGIN
    CALL "DM".fill_f101_round_f(d);
END $$;
