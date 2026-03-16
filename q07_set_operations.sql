-- ============================================================================
-- Q7: Caller Segmentation Using Set Operations
-- Difficulty: ★★★★☆ (Medium–Hard)
-- ============================================================================
-- BUSINESS QUESTION:
--   Segment the member population using set-based logic:
--   (a) Members who called about Claims but NEVER about Billing.
--   (b) Members who called about BOTH Claims and Pharmacy (cross-service).
--   (c) Members who have at least one escalated call — use EXISTS vs IN
--       and explain the difference.
--   (d) Members who called in Q1 but NOT in Q2 (churned from the call
--       center — potentially resolved their issues, or left the plan).
--
-- SKILLS DEMONSTRATED:
--   • EXCEPT, INTERSECT
--   • EXISTS vs. IN with explanation
--   • Date-range filtering with set logic
--   • CASE + aggregation hybrid
--
-- OPTIMIZATION NOTES:
--   • EXCEPT / INTERSECT: Postgres implements these as hash-based set
--     operations. They implicitly DISTINCT the results, which is correct
--     here (we want unique member_ids).
--   • EXISTS vs IN:
--       - EXISTS short-circuits: it stops scanning the subquery as soon as
--         one matching row is found. Best when the subquery returns many
--         rows per match (e.g., "does this member have ANY escalated call?").
--       - IN materializes the full subquery result set into a hash table,
--         then probes it. Best when the subquery returns a small, distinct
--         list (e.g., a list of 10 agent_ids).
--       - For NULL safety: IN returns NULL if the subquery contains NULLs
--         and no match is found; EXISTS always returns TRUE/FALSE.
--   • All four queries hit idx_calls_member_id or idx_calls_category.
-- ============================================================================

-- ── (a) Members who called about Claims but NEVER Billing ───────────────────
--    EXCEPT removes Billing callers from the Claims set.

SELECT member_id FROM calls WHERE call_category = 'Claims'
EXCEPT
SELECT member_id FROM calls WHERE call_category = 'Billing';

-- ── (b) Members who called about BOTH Claims AND Pharmacy ───────────────────
--    INTERSECT keeps only member_ids present in both sets.

SELECT member_id FROM calls WHERE call_category = 'Claims'
INTERSECT
SELECT member_id FROM calls WHERE call_category = 'Pharmacy';

-- ── (c) Members with at least one escalated call ────────────────────────────
--    EXISTS approach (preferred when checking existence of related rows):

SELECT DISTINCT
    p.member_id,
    p.member_name,
    p.plan_type
FROM
    policy_holders p
WHERE EXISTS (
    SELECT 1
    FROM   calls c
    WHERE  c.member_id = p.member_id
           AND c.resolution = 'Escalated'
);

--    Equivalent IN approach (shown for comparison):
/*
SELECT DISTINCT
    p.member_id,
    p.member_name,
    p.plan_type
FROM
    policy_holders p
WHERE
    p.member_id IN (
        SELECT c.member_id
        FROM   calls c
        WHERE  c.resolution = 'Escalated'
    );
*/
-- NOTE: Both produce identical results. EXISTS is marginally faster here
-- because the calls table may have multiple escalated rows per member,
-- and EXISTS stops at the first match. IN must collect all matching
-- member_ids before probing.

-- ── (d) Q1 callers who did NOT call in Q2 ("call center churn") ─────────────
--    EXCEPT cleanly expresses "in set A but not set B."

SELECT member_id
FROM   calls
WHERE  call_start >= '2024-01-01' AND call_start < '2024-04-01'   -- Q1

EXCEPT

SELECT member_id
FROM   calls
WHERE  call_start >= '2024-04-01' AND call_start < '2024-07-01'   -- Q2

ORDER BY member_id;
