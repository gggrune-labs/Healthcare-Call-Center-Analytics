-- ============================================================================
-- Q3: Repeat Callers Within 7 Days
-- Difficulty: ★★★☆☆ (Medium)
-- ============================================================================
-- BUSINESS QUESTION:
--   Identify every call where the same member called back within 7 calendar
--   days of their previous call. These "rapid repeat" calls indicate
--   unresolved issues and are a key quality metric. For each repeat call,
--   show the member, the gap in days, the categories of both calls, and
--   whether the prior call's resolution was non-final (Escalated,
--   Transferred, or Callback).
--
-- SKILLS DEMONSTRATED:
--   • LAG() window function with PARTITION BY member_id ORDER BY call_start
--   • Date interval arithmetic (call_start - prev_call_start)
--   • Filtering on window function results via CTE (cannot use in WHERE)
--   • CASE to flag "expected" vs. "unexpected" repeat calls
--
-- OPTIMIZATION NOTES:
--   • LAG avoids a self-join. A self-join approach would require
--     O(n²) comparisons per member; LAG scans each partition once.
--   • The composite index idx_calls_member_start (member_id, call_start)
--     supports the PARTITION BY … ORDER BY without an extra sort.
--   • The CTE is necessary because window functions cannot appear in WHERE.
--     A subquery would work identically here — CTE is chosen for readability.
-- ============================================================================

WITH calls_with_prev AS (
    SELECT
        c.call_id,
        c.member_id,
        p.member_name,
        c.call_start,
        c.call_category,
        c.resolution,
        LAG(c.call_start)    OVER w AS prev_call_start,
        LAG(c.call_category) OVER w AS prev_call_category,
        LAG(c.resolution)    OVER w AS prev_resolution
    FROM
        calls c
        INNER JOIN policy_holders p ON c.member_id = p.member_id
    WINDOW w AS (PARTITION BY c.member_id ORDER BY c.call_start)
)
SELECT
    call_id,
    member_id,
    member_name,
    prev_call_start,
    call_start,
    EXTRACT(DAY FROM call_start - prev_call_start)  AS days_since_prev,
    prev_call_category,
    call_category,
    prev_resolution,
    resolution,
    CASE
        WHEN prev_resolution IN ('Escalated', 'Transferred', 'Callback')
            THEN 'Expected follow-up'
        WHEN prev_resolution IS NULL
            THEN 'Prior call abandoned'
        ELSE 'Unexpected repeat'
    END AS repeat_type
FROM
    calls_with_prev
WHERE
    prev_call_start IS NOT NULL                                  -- not the member's first call
    AND call_start - prev_call_start <= INTERVAL '7 days'        -- within 7-day window
ORDER BY
    member_id, call_start;
