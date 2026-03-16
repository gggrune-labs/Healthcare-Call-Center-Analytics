-- ============================================================================
-- Q5: Agent Performance vs. Department Average (Self-Join & Correlated Subquery)
-- Difficulty: ★★★☆☆ (Medium)
-- ============================================================================
-- BUSINESS QUESTION:
--   For each agent, compare their average satisfaction score and average
--   handle time against their department's average. Flag agents who are
--   more than 0.5 points below their department's average satisfaction
--   as needing coaching. Show both approaches: self-join and correlated
--   subquery, with a note on when each is preferable.
--
-- SKILLS DEMONSTRATED:
--   • Self-join: agent-level metrics joined to department-level metrics
--   • Correlated subquery: department average computed per-row
--   • CASE for threshold-based flagging
--   • Comparison of two equivalent approaches
--
-- OPTIMIZATION NOTES:
--   • APPROACH A (self-join with CTEs) is preferred here because both
--     agent_metrics and dept_metrics are reusable, and the join is on a
--     small derived table (25 agents → 3 departments). The planner
--     materializes each CTE once.
--   • APPROACH B (correlated subquery) re-executes the subquery for each
--     of the 25 agent rows. With only 3 distinct departments, Postgres
--     may cache results, but the pattern scales worse on larger datasets.
--   • Rule of thumb: prefer self-join/CTE when the comparison group is
--     small and reusable; use correlated subqueries when the correlation
--     condition is complex or row-specific (e.g., "calls within 30 days
--     of this agent's hire date").
-- ============================================================================

-- ── APPROACH A: Self-Join via CTEs (recommended) ────────────────────────────

WITH agent_metrics AS (
    SELECT
        a.agent_id,
        a.agent_name,
        a.department,
        a.skill_level,
        COUNT(*)                                                AS total_calls,
        ROUND(AVG(c.satisfaction_score)::NUMERIC, 2)            AS avg_satisfaction,
        ROUND(
            AVG(
                EXTRACT(EPOCH FROM (c.call_end - c.call_start)) / 60.0
            )::NUMERIC, 1
        )                                                       AS avg_handle_min
    FROM
        call_agents a
        INNER JOIN calls c ON a.agent_id = c.agent_id
    WHERE
        c.call_end IS NOT NULL          -- exclude abandoned calls
        AND c.satisfaction_score IS NOT NULL
    GROUP BY
        a.agent_id, a.agent_name, a.department, a.skill_level
),
dept_metrics AS (
    SELECT
        department,
        ROUND(AVG(avg_satisfaction)::NUMERIC, 2)    AS dept_avg_satisfaction,
        ROUND(AVG(avg_handle_min)::NUMERIC, 1)      AS dept_avg_handle_min
    FROM
        agent_metrics
    GROUP BY
        department
)
SELECT
    am.agent_name,
    am.department,
    am.skill_level,
    am.total_calls,
    am.avg_satisfaction,
    dm.dept_avg_satisfaction,
    am.avg_satisfaction - dm.dept_avg_satisfaction       AS satisfaction_delta,
    am.avg_handle_min,
    dm.dept_avg_handle_min,
    am.avg_handle_min - dm.dept_avg_handle_min          AS handle_time_delta_min,
    CASE
        WHEN am.avg_satisfaction < dm.dept_avg_satisfaction - 0.5
            THEN 'Needs coaching'
        WHEN am.avg_satisfaction > dm.dept_avg_satisfaction + 0.5
            THEN 'Top performer'
        ELSE 'On track'
    END AS performance_flag
FROM
    agent_metrics am
    INNER JOIN dept_metrics dm ON am.department = dm.department
ORDER BY
    am.department, satisfaction_delta;

-- ── APPROACH B: Correlated Subquery (shown for comparison) ──────────────────
/*
SELECT
    a.agent_name,
    a.department,
    COUNT(*)                                            AS total_calls,
    ROUND(AVG(c.satisfaction_score)::NUMERIC, 2)        AS avg_satisfaction,
    (
        SELECT ROUND(AVG(c2.satisfaction_score)::NUMERIC, 2)
        FROM   calls c2
               INNER JOIN call_agents a2 ON c2.agent_id = a2.agent_id
        WHERE  a2.department = a.department
               AND c2.satisfaction_score IS NOT NULL
    ) AS dept_avg_satisfaction
FROM
    call_agents a
    INNER JOIN calls c ON a.agent_id = c.agent_id
WHERE
    c.satisfaction_score IS NOT NULL
GROUP BY
    a.agent_id, a.agent_name, a.department
ORDER BY
    a.department, avg_satisfaction;
*/
