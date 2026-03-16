-- ============================================================================
-- Q4: Call Volume & Performance by Shift (Time-of-Day Bucketing)
-- Difficulty: ★★★☆☆ (Medium)
-- ============================================================================
-- BUSINESS QUESTION:
--   The call center operates three shifts: Morning (08:00–11:59),
--   Afternoon (12:00–15:59), and Evening (16:00–19:59). Management wants
--   to know which shift has the highest call volume, longest average
--   handle time, worst satisfaction, and highest abandonment rate.
--   They also want day-of-week patterns within each shift.
--
-- SKILLS DEMONSTRATED:
--   • EXTRACT(HOUR FROM …) for time-of-day bucketing
--   • EXTRACT(ISODOW FROM …) for ISO day-of-week (1=Mon, 7=Sun)
--   • CASE for shift classification
--   • EPOCH conversion for handle time in minutes
--   • NTILE() to divide shifts into performance quartiles by handle time
--   • FILTER clause (PostgreSQL-specific) as alternative to CASE in agg
--
-- OPTIMIZATION NOTES:
--   • EXTRACT(HOUR …) on a TIMESTAMP is a simple arithmetic operation — no
--     function overhead vs. TO_CHAR + string comparison.
--   • NTILE(4) produces quartile buckets in a single pass; equivalent
--     manual percentile calculation would need a self-join or subquery.
--   • FILTER (WHERE …) is Postgres-specific but cleaner than CASE inside
--     COUNT and optimizes identically under the hood.
-- ============================================================================

WITH shift_calls AS (
    SELECT
        c.call_id,
        c.call_start,
        c.call_end,
        c.call_category,
        c.resolution,
        c.satisfaction_score,
        CASE
            WHEN EXTRACT(HOUR FROM c.call_start) BETWEEN  8 AND 11 THEN 'Morning (08–12)'
            WHEN EXTRACT(HOUR FROM c.call_start) BETWEEN 12 AND 15 THEN 'Afternoon (12–16)'
            WHEN EXTRACT(HOUR FROM c.call_start) BETWEEN 16 AND 19 THEN 'Evening (16–20)'
            ELSE 'Off-hours'
        END AS shift_name,
        EXTRACT(ISODOW FROM c.call_start) AS day_of_week_num,
        TO_CHAR(c.call_start, 'Dy')       AS day_of_week_abbr,
        CASE
            WHEN c.call_end IS NOT NULL THEN
                EXTRACT(EPOCH FROM (c.call_end - c.call_start)) / 60.0
        END AS handle_time_min
    FROM
        calls c
)
-- ── Part A: Shift-level summary ─────────────────────────────────────────────
SELECT
    shift_name,
    COUNT(*)                                                    AS total_calls,
    COUNT(*) FILTER (WHERE resolution IS NULL)                  AS abandoned_calls,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE resolution IS NULL)
              / NULLIF(COUNT(*), 0),
        1
    )                                                           AS abandon_rate_pct,
    ROUND(AVG(handle_time_min)::NUMERIC, 1)                     AS avg_handle_min,
    ROUND(AVG(satisfaction_score)::NUMERIC, 2)                  AS avg_satisfaction,
    ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY handle_time_min)::NUMERIC,
        1
    )                                                           AS median_handle_min
FROM
    shift_calls
WHERE
    shift_name <> 'Off-hours'
GROUP BY
    shift_name
ORDER BY
    shift_name;

-- ── Part B: Shift × Day-of-Week heatmap data ───────────────────────────────
-- (Uncomment to run separately or use UNION ALL with Part A)
/*
SELECT
    shift_name,
    day_of_week_abbr,
    day_of_week_num,
    COUNT(*) AS call_count,
    ROUND(AVG(handle_time_min)::NUMERIC, 1) AS avg_handle_min,
    NTILE(4) OVER (
        PARTITION BY shift_name
        ORDER BY COUNT(*) DESC
    ) AS volume_quartile   -- 1 = busiest quartile within shift
FROM
    shift_calls
WHERE
    shift_name <> 'Off-hours'
GROUP BY
    shift_name, day_of_week_abbr, day_of_week_num
ORDER BY
    shift_name, day_of_week_num;
*/
