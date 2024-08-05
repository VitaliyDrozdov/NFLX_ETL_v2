-- 1)
CREATE TABLE
    IF NOT EXISTS "DM".account_turnover_f (
        on_date DATE NOT NULL,
        account_rk NUMERIC NOT NULL,
        credit_amount NUMERIC(23, 8),
        credit_amount_rub NUMERIC(23, 8),
        debet_amount NUMERIC(23, 8),
        debet_amount_rub NUMERIC(23, 8)
    );

-- 2)
CREATE TABLE
    IF NOT EXISTS "DM".account_balance_f (
        on_date DATE NOT NULL,
        account_rk NUMERIC NOT NULL,
        balance_out NUMERIC,
        balance_out_rub FLOAT
    );

-- 3)
INSERT INTO
    "DM".account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT
    '2017-12-31',
    account_rk,
    balance_out,
    balance_out * COALESCE(reduced_cource, 1) AS balance_out_rub
FROM
    "DS".ft_balance_f bal
    LEFT JOIN "DS".md_exchange_rate_d ex ON bal.currency_rk = ex.currency_rk
    AND '2017-12-31' >= ex.data_actual_date
    AND (
        '2017-12-31' <= ex.data_actual_end_date
        OR ex.data_actual_end_date IS NULL
    );

CREATE TABLE
    IF NOT EXISTS "DM".f101_round_f (
        FROM_DATE DATE,
        TO_DATE DATE,
        CHAPTER CHAR(1),
        LEDGER_ACCOUNT CHAR(5),
        CHARACTERISTIC CHAR(1),
        BALANCE_IN_RUB NUMERIC(23, 8),
        R_BALANCE_IN_RUB NUMERIC(23, 8),
        BALANCE_IN_VAL NUMERIC(23, 8),
        R_BALANCE_IN_VAL NUMERIC(23, 8),
        BALANCE_IN_TOTAL NUMERIC(23, 8),
        R_BALANCE_IN_TOTAL NUMERIC(23, 8),
        TURN_DEB_RUB NUMERIC(23, 8),
        R_TURN_DEB_RUB NUMERIC(23, 8),
        TURN_DEB_VAL NUMERIC(23, 8),
        R_TURN_DEB_VAL NUMERIC(23, 8),
        TURN_DEB_TOTAL NUMERIC(23, 8),
        R_TURN_DEB_TOTAL NUMERIC(23, 8),
        TURN_CRE_RUB NUMERIC(23, 8),
        R_TURN_CRE_RUB NUMERIC(23, 8),
        TURN_CRE_VAL NUMERIC(23, 8),
        R_TURN_CRE_VAL NUMERIC(23, 8),
        TURN_CRE_TOTAL NUMERIC(23, 8),
        R_TURN_CRE_TOTAL NUMERIC(23, 8),
        BALANCE_OUT_RUB NUMERIC(23, 8),
        R_BALANCE_OUT_RUB NUMERIC(23, 8),
        BALANCE_OUT_VAL NUMERIC(23, 8),
        R_BALANCE_OUT_VAL NUMERIC(23, 8),
        BALANCE_OUT_TOTAL NUMERIC(23, 8),
        R_BALANCE_OUT_TOTAL NUMERIC(23, 8)
    );