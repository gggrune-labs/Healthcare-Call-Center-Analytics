-- ============================================================================
-- Q6: Rolling 30-Day Call Volume & Satisfaction Trends
-- Difficulty: ★★★★☆ (Medium–Hard)
-- ============================================================================
-- BUSINESS QUESTION:
--   Build a daily time series with rolling 30-day averages for call volume
--   and satisfaction score. Leadership uses this to detect emerging trends
--   earlier than monthly reports allow. Also compute cumulative call count
--   (running total) from Jan 1 to show progress toward annual targets.
--
-- SKILLS DEMONSTRATED:
--   • SUM() OVER (ORDER BY … ROWS BETWEEN …) — rolling aggregate
--   • AVG() OVER with frame specification
--   • SUM() OVER (ORDER BY …) — running total (default frame: UNBOUNDED
--     PRECEDING to CURRENT ROW)
--   • generate_series for gap-filling calendar dates
--   • LEFT JOIN to fill days with zero calls
--   • LEAD() to show next-day volume for quick comparison
--
-- OPTIMIZATION NOTES:
--   • generate_series + LEFT JOIN ensures every calendar day appears, even
--     weekends or holidays with zero calls. Without this, rolling averages
--     would be skewed by missing-day gaps.
--   • The ROWS BETWEEN 29 PRECEDING AND CURRENT ROW frame gives an exact
--     30-day trailing window based on row position. Since the calendar CTE
--     guarantees one row per day, ROWS and RANGE produce identical results
--     here — but ROWS is preferred because RANGE with date intervals has
--     quirks with NULL handling in some Postgres versions.
--   • Running total uses the default frame (UNBOUNDED PRECEDING to CURRENT
--     ROW), which is the most efficient — no frame bounds to track.
-- ============================================================================

WITH calendar AS (
    SELECT
        d::DATE AS cal_date
    FROM
        generate_series('2024-01-01'::DATE, '2024-06-30'::DATE, '1 day') AS d
),
daily_stats AS (
    SELECT
        c.cal_date,
        COUNT(cl.call_id)                                       AS daily_calls,
        ROUND(AVG(cl.satisfaction_score)::NUMERIC, 2)           AS daily_avg_sat
    FROM
        calendar c
        LEFT JOIN calls cl ON c.cal_date = cl.call_start::DATE
    GROUP BY
        c.cal_date
)
SELECT
    cal_date,
    daily_calls,
    daily_avg_sat,
    -- Running total of calls from Jan 1
    SUM(daily_calls) OVER (
        ORDER BY cal_date
    ) AS cumulative_calls,
    -- Rolling 30-day average call volume
    ROUND(
        AVG(daily_calls) OVER (
            ORDER BY cal_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC,
    1) AS rolling_30d_avg_volume,
    -- Rolling 30-day average satisfaction
    ROUND(
        AVG(daily_avg_sat) OVER (
            ORDER BY cal_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC,
    2) AS rolling_30d_avg_sat,
    -- Peek at next day's volume
    LEAD(daily_calls, 1) OVER (ORDER BY cal_date) AS next_day_calls
FROM
    daily_stats
ORDER BY
    cal_date;
