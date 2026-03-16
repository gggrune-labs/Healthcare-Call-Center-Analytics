-- ============================================================================
-- Q9: Member Risk Scoring — High-Utilization & Low-Satisfaction Detection
-- Difficulty: ★★★★★ (Hard)
-- ============================================================================
-- BUSINESS QUESTION:
--   Build a composite risk score for each member who called in 2024.
--   The score combines:
--     (1) Call frequency quartile (NTILE 4 — top quartile = highest risk)
--     (2) Average satisfaction score (lower = higher risk)
--     (3) Escalation count (more escalations = higher risk)
--     (4) Percentage of calls that were repeat calls within 7 days
--
--   Assign a risk tier: Critical, High, Medium, Low based on the
--   composite. Include member demographics (age, plan type, state)
--   for downstream segmentation.
--
-- SKILLS DEMONSTRATED:
--   • Multiple layered CTEs building on each other
--   • NTILE() for quartile bucketing
--   • LAG for repeat-call detection (reused from Q3)
--   • CASE with compound conditions for tier assignment
--   • DATE_PART('year', AGE(…)) for age calculation
--   • Combining window functions with standard aggregation
--
-- OPTIMIZATION NOTES:
--   • The CTE chain (call_gaps → member_agg → member_scored) avoids
--     repeating the LAG calculation. Each CTE is materialized once.
--   • NTILE operates on the member_agg result (one row per member) so
--     the window frame is small.
--   • The composite score uses a simple weighted sum rather than a
--     machine learning model — appropriate for an operational dashboard.
--   • Index on (member_id, call_start) supports the LAG partition.
-- ============================================================================

WITH call_gaps AS (
    -- Step 1: Compute gap between consecutive calls per member
    SELECT
        call_id,
        member_id,
        call_start,
        resolution,
        satisfaction_score,
        LAG(call_start) OVER (PARTITION BY member_id ORDER BY call_start) AS prev_start,
        CASE
            WHEN LAG(call_start) OVER (PARTITION BY member_id ORDER BY call_start) IS NOT NULL
                 AND call_start - LAG(call_start) OVER (PARTITION BY member_id ORDER BY call_start)
                     <= INTERVAL '7 days'
            THEN 1
            ELSE 0
        END AS is_repeat_within_7d
    FROM
        calls
),
member_agg AS (
    -- Step 2: Aggregate to one row per member
    SELECT
        cg.member_id,
        COUNT(*)                                                        AS total_calls,
        ROUND(AVG(cg.satisfaction_score)::NUMERIC, 2)                   AS avg_satisfaction,
        COUNT(CASE WHEN cg.resolution = 'Escalated' THEN 1 END)        AS escalation_count,
        SUM(cg.is_repeat_within_7d)                                     AS repeat_7d_count,
        ROUND(
            100.0 * SUM(cg.is_repeat_within_7d) / NULLIF(COUNT(*), 0),
            1
        )                                                               AS repeat_7d_pct,
        COUNT(CASE WHEN cg.resolution IS NULL THEN 1 END)              AS abandoned_count
    FROM
        call_gaps cg
    GROUP BY
        cg.member_id
),
member_scored AS (
    -- Step 3: Compute quartiles and composite risk score
    SELECT
        ma.*,
        NTILE(4) OVER (ORDER BY ma.total_calls DESC)     AS frequency_quartile,
        -- Composite: higher = more risk (scale 0–100)
        -- Frequency:    top quartile (1) → 25 pts, bottom (4) → 0 pts
        -- Satisfaction:  score 1 → 25 pts, score 5 → 0 pts
        -- Escalation %: up to 25 pts
        -- Repeat %:     up to 25 pts
        ROUND(
            (
                -- frequency component (0–25)
                (5 - NTILE(4) OVER (ORDER BY ma.total_calls DESC)) * 25.0 / 4
                -- satisfaction component (0–25): invert so low sat = high risk
                + CASE
                    WHEN ma.avg_satisfaction IS NOT NULL
                    THEN (5.0 - ma.avg_satisfaction) / 4.0 * 25
                    ELSE 12.5  -- missing → middle risk
                  END
                -- escalation component (0–25)
                + LEAST(ma.escalation_count * 5.0, 25)
                -- repeat-call component (0–25)
                + LEAST(ma.repeat_7d_pct / 4.0, 25)
            )::NUMERIC,
        1) AS risk_score
    FROM
        member_agg ma
)
SELECT
    ms.member_id,
    p.member_name,
    p.plan_type,
    p.state,
    DATE_PART('year', AGE('2024-06-30'::DATE, p.date_of_birth))::INT AS member_age,
    ms.total_calls,
    ms.avg_satisfaction,
    ms.escalation_count,
    ms.repeat_7d_count,
    ms.repeat_7d_pct,
    ms.abandoned_count,
    ms.frequency_quartile,
    ms.risk_score,
    CASE
        WHEN ms.risk_score >= 60 THEN 'Critical'
        WHEN ms.risk_score >= 40 THEN 'High'
        WHEN ms.risk_score >= 20 THEN 'Medium'
        ELSE 'Low'
    END AS risk_tier
FROM
    member_scored ms
    INNER JOIN policy_holders p ON ms.member_id = p.member_id
ORDER BY
    ms.risk_score DESC;
