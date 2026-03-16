-- ============================================================================
-- Q1: Monthly Call Volume by Category (Pivot Report)
-- Difficulty: ★☆☆☆☆ (Easy)
-- ============================================================================
-- BUSINESS QUESTION:
--   Operations leadership wants a single-query pivot table showing how many
--   calls each category received per month (Jan–Jun 2024). This replaces
--   the spreadsheet pivot they currently build manually each quarter.
--
-- SKILLS DEMONSTRATED:
--   • CASE expressions inside aggregate functions (conditional aggregation)
--   • DATE_TRUNC / EXTRACT for month grouping
--   • COALESCE for clean zero-fill
--
-- OPTIMIZATION NOTES:
--   • Conditional aggregation in a single pass is O(n) — far cheaper than
--     running 6 separate GROUP BY queries or using CROSSTAB.
--   • The idx_calls_call_start index supports the implicit sort but the
--     query does not filter on date range, so a sequential scan is expected
--     and appropriate for full-table reporting.
-- ============================================================================

SELECT
    TO_CHAR(DATE_TRUNC('month', call_start), 'YYYY-MM')    AS call_month,
    COUNT(*)                                                 AS total_calls,
    COUNT(CASE WHEN call_category = 'Claims'     THEN 1 END) AS claims,
    COUNT(CASE WHEN call_category = 'Benefits'   THEN 1 END) AS benefits,
    COUNT(CASE WHEN call_category = 'Billing'    THEN 1 END) AS billing,
    COUNT(CASE WHEN call_category = 'Enrollment' THEN 1 END) AS enrollment,
    COUNT(CASE WHEN call_category = 'Pharmacy'   THEN 1 END) AS pharmacy,
    COUNT(CASE WHEN call_category = 'Prior Auth' THEN 1 END) AS prior_auth
FROM
    calls
GROUP BY
    DATE_TRUNC('month', call_start)
ORDER BY
    call_month;
