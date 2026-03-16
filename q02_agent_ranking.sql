-- ============================================================================
-- Q2: Agent Performance Ranking by Resolution Rate
-- Difficulty: ★★☆☆☆ (Easy–Medium)
-- ============================================================================
-- BUSINESS QUESTION:
--   Rank all agents by their first-call resolution rate (calls resolved
--   without escalation, transfer, or callback). Include each agent's
--   total call count, resolution rate, and their rank within their
--   department plus their overall rank.
--
-- SKILLS DEMONSTRATED:
--   • Window functions: RANK(), DENSE_RANK(), ROW_NUMBER()
--   • CASE inside COUNT for conditional aggregation
--   • PARTITION BY for department-level vs. overall ranking
--   • JOIN between calls and call_agents
--
-- OPTIMIZATION NOTES:
--   • A CTE isolates the aggregation step so window functions operate on
--     the 25-row agent summary, not the ~600-row call table. This keeps
--     the window frame tiny.
--   • RANK vs DENSE_RANK: RANK is used for department ranking (gaps show
--     true positional distance); DENSE_RANK for overall ranking (no gaps
--     means easier tier assignment for bonus calculations).
--   • ROW_NUMBER provides a tiebreaker ordered by total calls descending,
--     which the business can use for alphabetical-break scenarios.
-- ============================================================================

WITH agent_metrics AS (
    SELECT
        a.agent_id,
        a.agent_name,
        a.department,
        a.skill_level,
        COUNT(*)                                                    AS total_calls,
        COUNT(CASE WHEN c.resolution = 'Resolved' THEN 1 END)      AS resolved_calls,
        ROUND(
            100.0 * COUNT(CASE WHEN c.resolution = 'Resolved' THEN 1 END)
                  / NULLIF(COUNT(*), 0),
            1
        )                                                           AS resolution_rate_pct
    FROM
        call_agents a
        INNER JOIN calls c ON a.agent_id = c.agent_id
    GROUP BY
        a.agent_id, a.agent_name, a.department, a.skill_level
)
SELECT
    agent_name,
    department,
    skill_level,
    total_calls,
    resolved_calls,
    resolution_rate_pct,
    RANK()       OVER (PARTITION BY department ORDER BY resolution_rate_pct DESC)
        AS dept_rank,
    DENSE_RANK() OVER (ORDER BY resolution_rate_pct DESC)
        AS overall_dense_rank,
    ROW_NUMBER() OVER (ORDER BY resolution_rate_pct DESC, total_calls DESC)
        AS overall_row_num
FROM
    agent_metrics
ORDER BY
    overall_dense_rank, department;
