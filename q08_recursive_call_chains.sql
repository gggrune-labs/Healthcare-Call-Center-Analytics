-- ============================================================================
-- Q8: Repeat-Caller Chains via Recursive CTE
-- Difficulty: ★★★★★ (Hard)
-- ============================================================================
-- BUSINESS QUESTION:
--   Some members call repeatedly in rapid succession (each call within 7
--   days of the previous one). Build a "call chain" — a sequence of
--   consecutive calls where each link is ≤7 days from the prior link.
--   A chain breaks when the gap exceeds 7 days.
--
--   For each chain, report: the member, chain length, total span in days,
--   and whether the chain ended with a successful resolution.
--
--   This tells the quality team which members are stuck in loops and how
--   long those loops last.
--
-- SKILLS DEMONSTRATED:
--   • Recursive CTE (WITH RECURSIVE)
--   • Non-recursive CTE for numbered sequencing
--   • LAG / LEAD for gap detection
--   • Chain aggregation with GROUP BY on chain_id
--
-- OPTIMIZATION NOTES:
--   • The recursive CTE here is bounded: it can only recurse as deep as
--     the maximum number of calls a single member has (~15 in this dataset).
--     The termination condition (next call within 7 days) prunes branches.
--   • An alternative non-recursive approach uses LAG to detect chain breaks
--     and SUM() OVER to assign chain IDs (shown as Approach B below).
--     The non-recursive version is generally faster because recursive CTEs
--     disable some Postgres optimizations. However, the recursive version
--     is included because it demonstrates the pattern for interviews.
--   • idx_calls_member_start is critical for both approaches.
-- ============================================================================

-- ── APPROACH A: Recursive CTE ───────────────────────────────────────────────

WITH RECURSIVE numbered_calls AS (
    -- Assign row numbers per member to enable "next call" lookups
    SELECT
        call_id,
        member_id,
        call_start,
        call_category,
        resolution,
        ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY call_start) AS call_seq
    FROM
        calls
),
chains AS (
    -- Anchor: every call is the potential start of a chain
    SELECT
        nc.call_id,
        nc.member_id,
        nc.call_start     AS chain_start,
        nc.call_start     AS current_call_start,
        nc.resolution     AS current_resolution,
        nc.call_seq,
        1                 AS chain_length,
        nc.call_id        AS chain_anchor_id
    FROM
        numbered_calls nc

    UNION ALL

    -- Recursive step: extend the chain if the next call is within 7 days
    SELECT
        next_call.call_id,
        ch.member_id,
        ch.chain_start,
        next_call.call_start,
        next_call.resolution,
        next_call.call_seq,
        ch.chain_length + 1,
        ch.chain_anchor_id
    FROM
        chains ch
        INNER JOIN numbered_calls next_call
            ON  next_call.member_id = ch.member_id
            AND next_call.call_seq  = ch.call_seq + 1
    WHERE
        next_call.call_start - ch.current_call_start <= INTERVAL '7 days'
),
-- Keep only the longest chain starting from each anchor
max_chains AS (
    SELECT DISTINCT ON (member_id, chain_anchor_id)
        member_id,
        chain_anchor_id,
        chain_start,
        current_call_start  AS chain_end,
        chain_length,
        current_resolution  AS final_resolution,
        EXTRACT(DAY FROM current_call_start - chain_start) AS chain_span_days
    FROM
        chains
    ORDER BY
        member_id, chain_anchor_id, chain_length DESC
)
SELECT
    mc.member_id,
    p.member_name,
    mc.chain_start,
    mc.chain_end,
    mc.chain_length,
    mc.chain_span_days,
    mc.final_resolution,
    CASE
        WHEN mc.final_resolution = 'Resolved' THEN 'Chain resolved'
        WHEN mc.final_resolution IS NULL       THEN 'Abandoned'
        ELSE 'Still unresolved'
    END AS chain_outcome
FROM
    max_chains mc
    INNER JOIN policy_holders p ON mc.member_id = p.member_id
WHERE
    mc.chain_length >= 2        -- only multi-call chains
ORDER BY
    mc.chain_length DESC,
    mc.member_id,
    mc.chain_start;

-- ── APPROACH B: Non-Recursive (gap-and-island) — generally preferred ────────
/*
WITH ordered_calls AS (
    SELECT
        call_id,
        member_id,
        call_start,
        resolution,
        LAG(call_start) OVER (PARTITION BY member_id ORDER BY call_start) AS prev_start
    FROM calls
),
flagged AS (
    SELECT
        *,
        CASE
            WHEN prev_start IS NULL
                 OR call_start - prev_start > INTERVAL '7 days'
            THEN 1
            ELSE 0
        END AS new_chain_flag
    FROM ordered_calls
),
chained AS (
    SELECT
        *,
        SUM(new_chain_flag) OVER (
            PARTITION BY member_id ORDER BY call_start
        ) AS chain_id
    FROM flagged
)
SELECT
    member_id,
    chain_id,
    MIN(call_start)    AS chain_start,
    MAX(call_start)    AS chain_end,
    COUNT(*)           AS chain_length,
    EXTRACT(DAY FROM MAX(call_start) - MIN(call_start)) AS chain_span_days
FROM chained
GROUP BY member_id, chain_id
HAVING COUNT(*) >= 2
ORDER BY chain_length DESC, member_id;
*/
