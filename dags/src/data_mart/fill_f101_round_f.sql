
CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_date DATE;
    v_to_date DATE;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
BEGIN
    -- Определяем первый и последний день отчетного периода
    v_from_date := (i_OnDate - INTERVAL '1 month')::date;
    v_to_date := (i_OnDate - INTERVAL '1 day')::date;

    v_start_time := NOW();
    RAISE NOTICE 'Процедура fill_f101_round_f начата в %', v_start_time;

    RAISE NOTICE 'Удаление данных для периода % - % из f101_round_f', v_from_date, v_to_date;
    DELETE FROM "DM".f101_round_f
    WHERE from_date = v_from_date AND to_date = v_to_date;

    RAISE NOTICE 'Вставка данных в f101_round_f для периода % - %', v_from_date, v_to_date;
    INSERT INTO "DM".f101_round_f (
        from_date,
        to_date, chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    )
    SELECT
        v_from_date,
        v_to_date,
        l.chapter,
        LEFT(a.account_number, 5) AS ledger_account,

        a.char_type::CHAR(1) AS characteristic,
        -- BALANCE_IN_RUB
        CASE WHEN a.currency_code IN ('810', '643') THEN COALESCE(prev_balance.balance_out_rub, 0) ELSE 0 END AS balance_in_rub,
        -- BALANCE_IN_VAL
        CASE WHEN a.currency_code NOT IN ('810', '643') THEN COALESCE(prev_balance.balance_out_rub, 0) ELSE 0 END AS balance_in_val,
        -- BALANCE_IN_TOTAL
        COALESCE(prev_balance.balance_out_rub, 0) AS balance_in_total,

        -- TURN_DEB_RUB
        CASE WHEN a.currency_code IN ('810', '643') THEN COALESCE(t.turn_deb_rub, 0) ELSE 0 END AS turn_deb_rub,
        -- TURN_DEB_VAL
        CASE WHEN a.currency_code NOT IN ('810', '643') THEN COALESCE(t.turn_deb_rub, 0) ELSE 0 END AS turn_deb_val,
        -- TURN_DEB_TOTAL
        COALESCE(t.turn_deb_rub, 0) AS turn_deb_total,

        -- TURN_CRE_RUB
        CASE WHEN a.currency_code IN ('810', '643') THEN COALESCE(t.turn_cre_rub, 0) ELSE 0 END AS turn_cre_rub,
        -- TURN_CRE_VAL
        CASE WHEN a.currency_code NOT IN ('810', '643') THEN COALESCE(t.turn_cre_rub, 0) ELSE 0 END AS turn_cre_val,
        -- TURN_CRE_TOTAL
        COALESCE(t.turn_cre_rub, 0) AS turn_cre_total,

        -- BALANCE_OUT_RUB
        CASE WHEN a.currency_code IN ('810', '643') THEN COALESCE(b_out.balance_out_rub, 0) ELSE 0 END AS balance_out_rub,
        -- BALANCE_OUT_VAL
        CASE WHEN a.currency_code NOT IN ('810', '643') THEN COALESCE(b_out.balance_out_rub, 0) ELSE 0 END AS balance_out_val,
        -- BALANCE_OUT_TOTAL
        COALESCE(b_out.balance_out_rub, 0) AS balance_out_total

    FROM
        "DS".md_account_d a
    LEFT JOIN
        "DS".md_ledger_account_s l
        ON LEFT(a.account_number, 5)::integer = l.ledger_account
    LEFT JOIN LATERAL (
        SELECT balance_out_rub
        FROM "DM".account_balance_f
        WHERE account_rk = a.account_rk AND on_date = v_from_date - INTERVAL '1 day'
        ORDER BY on_date DESC
        LIMIT 1
    ) AS prev_balance ON true
    LEFT JOIN LATERAL (
        SELECT
            SUM(COALESCE(credit_amount_rub, 0)) AS turn_cre_rub,
            SUM(COALESCE(debet_amount_rub, 0)) AS turn_deb_rub
        FROM "DM".account_turnover_f
        WHERE account_rk = a.account_rk AND on_date BETWEEN v_from_date AND v_to_date
    ) AS t ON true
    LEFT JOIN LATERAL (
        SELECT balance_out_rub
        FROM "DM".account_balance_f
        WHERE account_rk = a.account_rk AND on_date = v_to_date
        ORDER BY on_date DESC
        LIMIT 1
    ) AS b_out ON true
    WHERE
        i_OnDate >= a.data_actual_date;

    -- Логи
    v_end_time := NOW();
    v_duration := v_end_time - v_start_time;

    RAISE NOTICE 'Процедура fill_f101_round_f завершена в %', v_end_time;

    INSERT INTO "LOG".etl_log (table_name, start_time, end_time, duration)
    VALUES ('f101_round_f', v_start_time, v_end_time, v_duration);

EXCEPTION
    WHEN OTHERS THEN
        v_end_time := NOW();
        v_duration := v_end_time - v_start_time;
        RAISE NOTICE 'Произошла ошибка в процедуре fill_f101_round_f: %', SQLERRM;
        INSERT INTO "LOG".etl_log (table_name, start_time, end_time, duration)
        VALUES ('f101_round_f', v_start_time, v_end_time, v_duration);
        RAISE;
END;
$$;
