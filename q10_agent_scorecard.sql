-- ============================================================================
-- Q10: Comprehensive Agent Scorecard (Capstone Query)
-- Difficulty: ★★★★★ (Hard)
-- ============================================================================
-- BUSINESS QUESTION:
--   Build a single executive-ready agent scorecard that combines:
--     • Call volume and month-over-month growth rate
--     • Resolution rate, average handle time, and satisfaction percentile
--     • Comparison to department peers (delta from dept average)
--     • "Streak" metric: longest consecutive run of resolved calls
--     • Tenure-adjusted performance (calls per month since hire)
--
--   This is the all-in-one query the VP of Operations would use for
--   quarterly performance reviews.
--
-- SKILLS DEMONSTRATED:
--   • Multiple CTEs building a layered analysis
--   • Window functions: PERCENT_RANK, LAG for MoM growth, running count
--   • Self-join pattern for department comparison
--   • Gaps-and-islands for streak detection
--   • Date arithmetic for tenure calculation
--   • CASE with multiple conditions
--
-- OPTIMIZATION NOTES:
--   • This query is intentionally complex to demonstrate composition of
--     simpler patterns. In production, materialized views or pre-computed
--     summary tables would replace real-time execution.
--   • The streak detection (gaps-and-islands) is isolated in its own CTE
--     so the window function operates only on per-agent call sequences.
--   • PERCENT_RANK gives a 0–1 percentile that's more intuitive than
--     NTILE for comparing agents ("you're in the 85th percentile").
-- ============================================================================

WITH agent_monthly AS (
    -- Monthly volume per agent
    SELECT
        c.agent_id,
        DATE_TRUNC('month', c.call_start)::DATE    AS call_month,
        COUNT(*)                                     AS monthly_calls,
        COUNT(CASE WHEN c.resolution = 'Resolved' THEN 1 END) AS monthly_resolved,
        ROUND(AVG(c.satisfaction_score)::NUMERIC, 2) AS monthly_avg_sat,
        ROUND(
            AVG(
                EXTRACT(EPOCH FROM (c.call_end - c.call_start)) / 60.0
            )::NUMERIC, 1
        ) AS monthly_avg_handle_min
    FROM
        calls c
    WHERE
        c.call_end IS NOT NULL
    GROUP BY
        c.agent_id, DATE_TRUNC('month', c.call_start)
),
agent_mom AS (
    -- Month-over-month growth rate per agent
    SELECT
        am.*,
        LAG(am.monthly_calls) OVER (
            PARTITION BY am.agent_id ORDER BY am.call_month
        ) AS prev_month_calls,
        CASE
            WHEN LAG(am.monthly_calls) OVER (
                     PARTITION BY am.agent_id ORDER BY am.call_month
                 ) > 0
            THEN ROUND(
                100.0 * (am.monthly_calls - LAG(am.monthly_calls) OVER (
                    PARTITION BY am.agent_id ORDER BY am.call_month
                )) / LAG(am.monthly_calls) OVER (
                    PARTITION BY am.agent_id ORDER BY am.call_month
                )::NUMERIC,
            1)
        END AS mom_growth_pct
    FROM
        agent_monthly am
),
agent_totals AS (
    -- Overall metrics per agent
    SELECT
        a.agent_id,
        a.agent_name,
        a.department,
        a.skill_level,
        a.hire_date,
        -- Tenure in months (as of end of study period)
        -- DATE - DATE returns integer days in PostgreSQL; divide by ~30.44 for months
        ROUND(
            ('2024-06-30'::DATE - a.hire_date)::NUMERIC / 30.44,
            0
        )::INT AS tenure_months,
        COUNT(c.call_id)                                                    AS total_calls,
        COUNT(CASE WHEN c.resolution = 'Resolved' THEN 1 END)              AS total_resolved,
        ROUND(
            100.0 * COUNT(CASE WHEN c.resolution = 'Resolved' THEN 1 END)
                  / NULLIF(COUNT(*), 0)::NUMERIC,
        1)                                                                  AS resolution_rate_pct,
        ROUND(AVG(c.satisfaction_score)::NUMERIC, 2)                        AS avg_satisfaction,
        ROUND(
            AVG(EXTRACT(EPOCH FROM (c.call_end - c.call_start)) / 60.0)::NUMERIC,
        1)                                                                  AS avg_handle_min
    FROM
        call_agents a
        LEFT JOIN calls c ON a.agent_id = c.agent_id AND c.call_end IS NOT NULL
    GROUP BY
        a.agent_id, a.agent_name, a.department, a.skill_level, a.hire_date
),
dept_avgs AS (
    SELECT
        department,
        ROUND(AVG(resolution_rate_pct)::NUMERIC, 1)    AS dept_avg_resolution,
        ROUND(AVG(avg_satisfaction)::NUMERIC, 2)        AS dept_avg_satisfaction,
        ROUND(AVG(avg_handle_min)::NUMERIC, 1)          AS dept_avg_handle_min
    FROM
        agent_totals
    WHERE
        total_calls > 0
    GROUP BY
        department
),
-- Streak detection: longest consecutive "Resolved" run per agent
call_streaks AS (
    SELECT
        agent_id,
        call_start,
        resolution,
        -- Flag non-Resolved calls as streak-breakers
        SUM(CASE WHEN resolution <> 'Resolved' THEN 1 ELSE 0 END) OVER (
            PARTITION BY agent_id
            ORDER BY call_start
        ) AS streak_group
    FROM
        calls
    WHERE
        call_end IS NOT NULL    -- exclude abandoned
),
max_streaks AS (
    SELECT
        agent_id,
        MAX(streak_length) AS longest_resolved_streak
    FROM (
        SELECT
            agent_id,
            streak_group,
            COUNT(*) AS streak_length
        FROM call_streaks
        WHERE resolution = 'Resolved'
        GROUP BY agent_id, streak_group
    ) sub
    GROUP BY agent_id
)
-- ── Final scorecard ─────────────────────────────────────────────────────────
SELECT
    at.agent_name,
    at.department,
    at.skill_level,
    at.hire_date,
    at.tenure_months,
    at.total_calls,
    CASE
        WHEN at.tenure_months > 0
        THEN ROUND(at.total_calls::NUMERIC / at.tenure_months, 1)
        ELSE at.total_calls::NUMERIC
    END                                                         AS calls_per_month,
    at.resolution_rate_pct,
    da.dept_avg_resolution,
    at.resolution_rate_pct - da.dept_avg_resolution             AS resolution_delta,
    at.avg_satisfaction,
    da.dept_avg_satisfaction,
    at.avg_satisfaction - da.dept_avg_satisfaction               AS satisfaction_delta,
    at.avg_handle_min,
    da.dept_avg_handle_min,
    at.avg_handle_min - da.dept_avg_handle_min                  AS handle_delta_min,
    COALESCE(ms.longest_resolved_streak, 0)                     AS longest_resolved_streak,
    -- Satisfaction percentile among all agents
    ROUND(
        PERCENT_RANK() OVER (ORDER BY at.avg_satisfaction)::NUMERIC * 100,
    0)                                                          AS satisfaction_percentile,
    -- Latest month's MoM growth
    latest.mom_growth_pct                                       AS latest_mom_growth_pct,
    -- Composite rating
    CASE
        WHEN at.resolution_rate_pct > da.dept_avg_resolution + 5
             AND at.avg_satisfaction > da.dept_avg_satisfaction
            THEN 'Exceeds expectations'
        WHEN at.resolution_rate_pct < da.dept_avg_resolution - 5
             OR  at.avg_satisfaction < da.dept_avg_satisfaction - 0.5
            THEN 'Needs improvement'
        ELSE 'Meets expectations'
    END                                                         AS overall_rating
FROM
    agent_totals at
    LEFT JOIN dept_avgs da          ON at.department = da.department
    LEFT JOIN max_streaks ms        ON at.agent_id = ms.agent_id
    LEFT JOIN LATERAL (
        -- Get the most recent month's MoM growth for each agent
        SELECT mom_growth_pct
        FROM   agent_mom am
        WHERE  am.agent_id = at.agent_id
        ORDER BY am.call_month DESC
        LIMIT 1
    ) latest ON TRUE
ORDER BY
    at.department,
    at.resolution_rate_pct DESC;
