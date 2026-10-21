-- Package to repository mapping with status, rule and confidence (Phase 7, ADR-037). One row
-- per package and candidate repository; claims naming one GitHub id under different names (a
-- rename) are one candidate, shown under the current name. Independent source families: the
-- registry (npm registry `repository` field and ecosyste.ms, which derives its URL from that
-- same registry metadata, count once together) and an OSV GIT range; OSV FIX references only
-- support. Candidates a reviewer rejected do not count as rivals. Rules, first match wins:
--   review              a reviewed decision (seed resolve_decisions) overrides everything
--   sources_agree       both families name it and no other candidate has one: auto_exact
--   contested           1 family against a rival named only by a FIX reference in an
--                       advisory about this package alone: both suggested (review CSV)
--   single_source       1 family, no rival, the GitHub id is known: accepted
--   single_source_unverified  as above without a known id: suggested
--   most_sources        strictly more families than every rival: accepted
--   llm                 a tie decided by the language model at confidence >= 0.8, only when it
--                       was asked about exactly the current tied candidates
--   tie                 a tie without such a choice: suggested (review CSV)
--   outvoted            fewer families than a rival: rejected
--   fix_reference_only  only OSV FIX references: suggested
WITH claims AS (
    SELECT
        c.*,
        i.repo_id,
        i.id_rule,
        i.current_name,
        i.seen_in_gharchive
    FROM {{ ref('int_package_repo_claims') }} AS c
    LEFT JOIN {{ ref('int_repo_identity') }} AS i
        ON c.repo_host = 'github.com' AND c.repo_full_name = i.repo_name_lower
),

keyed AS (
    SELECT
        *,
        CASE
            WHEN repo_id IS NOT NULL THEN 'github.com#' || CAST(repo_id AS STRING)
            ELSE repo_host || '/' || repo_full_name
        END AS repo_key
    FROM claims
),

grouped AS (
    SELECT
        package_key,
        repo_key,
        MIN(repo_host) AS repo_host,
        MIN(COALESCE(current_name, repo_full_name)) AS repo_full_name,
        MAX(repo_id) AS repo_id,
        -- the rule that resolved the current name, else any (names merged by id)
        COALESCE(
            MIN(CASE WHEN repo_full_name = current_name THEN id_rule END), MIN(id_rule)
        ) AS repo_id_rule,
        MAX(CASE WHEN seen_in_gharchive THEN 1 ELSE 0 END) = 1 AS seen_in_gharchive,
        CAST(COUNT(DISTINCT repo_full_name) AS BIGINT) AS claimed_names,
        MAX(CASE WHEN source = 'ecosystems' THEN 1 ELSE 0 END) AS by_ecosystems,
        MAX(CASE WHEN source = 'npm_registry' THEN 1 ELSE 0 END) AS by_npm_registry,
        MAX(CASE WHEN source = 'osv_git_range' THEN 1 ELSE 0 END) AS by_osv_git_range,
        MAX(CASE WHEN source = 'osv_fix_sole' THEN 1 ELSE 0 END) AS by_osv_fix_sole,
        MAX(CASE WHEN source = 'osv_fix_shared' THEN 1 ELSE 0 END) AS by_osv_fix_shared,
        MAX(CASE WHEN in_subpath THEN 1 ELSE 0 END) = 1 AS in_subpath,
        CAST(SUM(CASE WHEN source LIKE 'osv%' THEN evidence ELSE 0 END) AS BIGINT)
            AS osv_evidence_vulns
    FROM keyed
    GROUP BY package_key, repo_key
),

scored AS (
    SELECT
        g.*,
        CASE WHEN g.by_ecosystems = 1 OR g.by_npm_registry = 1 THEN 1 ELSE 0 END
        + g.by_osv_git_range AS independent_sources,
        d.decision
    FROM grouped AS g
    LEFT JOIN {{ ref('resolve_decisions') }} AS d
        ON
            d.subject = 'package_repo'
            AND g.package_key = d.subject_key
            AND g.repo_key = d.candidate
),

ranked AS (
    -- tallies over the candidates still in play (a reviewer's rejection removes a rival)
    SELECT
        s.*,
        CASE WHEN s.decision = 'rejected' THEN -1 ELSE s.independent_sources END AS in_play,
        MAX(CASE WHEN s.decision = 'rejected' THEN -1 ELSE s.independent_sources END)
            OVER (PARTITION BY s.package_key) AS max_independent,
        COUNT(*) OVER (PARTITION BY s.package_key) AS candidates,
        MAX(CASE WHEN s.decision = 'accepted' THEN 1 ELSE 0 END)
            OVER (PARTITION BY s.package_key) AS package_has_accepted_review
    FROM scored AS s
),

counted AS (
    SELECT
        r.*,
        SUM(CASE WHEN r.in_play = r.max_independent THEN 1 ELSE 0 END)
            OVER (PARTITION BY r.package_key) AS at_max,
        SUM(CASE WHEN r.in_play > 0 THEN 1 ELSE 0 END)
            OVER (PARTITION BY r.package_key) AS supported,
        MAX(CASE WHEN r.in_play = 0 AND r.by_osv_fix_sole = 1 THEN 1 ELSE 0 END)
            OVER (PARTITION BY r.package_key) AS fix_rival
    FROM ranked AS r
),

with_llm AS (
    SELECT
        c.*,
        l.repo_key AS llm_repo_key,
        l.confidence AS llm_confidence,
        -- the stored choice counts only if it was asked about exactly the current tie
        SUM(
            CASE
                WHEN
                    c.in_play = c.max_independent
                    AND ('|' || l.candidates || '|') LIKE ('%|' || c.repo_key || '|%')
                    THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY c.package_key) AS tie_listed,
        LENGTH(l.candidates) - LENGTH(REPLACE(l.candidates, '|', '')) + 1 AS listed,
        MAX(CASE WHEN c.in_play = c.max_independent AND c.repo_key = l.repo_key THEN 1 ELSE 0 END)
            OVER (PARTITION BY c.package_key) AS choice_is_tied
    FROM counted AS c
    LEFT JOIN {{ ref('stg_resolve__llm_choices') }} AS l
        ON c.package_key = l.package_key AND l.outcome = 'chosen'
),

decided AS (
    SELECT
        *,
        CASE
            WHEN decision IS NOT NULL THEN 'review'
            WHEN package_has_accepted_review = 1 THEN 'review'
            WHEN in_play >= 2 AND supported = 1 THEN 'sources_agree'
            WHEN
                max_independent = 1 AND supported = 1 AND fix_rival = 1
                AND (in_play = 1 OR by_osv_fix_sole = 1)
                THEN 'contested'
            WHEN in_play = 1 AND supported = 1 AND repo_id IS NOT NULL THEN 'single_source'
            WHEN in_play = 1 AND supported = 1 THEN 'single_source_unverified'
            WHEN in_play >= 1 AND in_play = max_independent AND at_max = 1 THEN 'most_sources'
            WHEN
                in_play >= 1 AND in_play = max_independent
                AND llm_repo_key IS NOT NULL AND llm_confidence >= 0.8
                AND choice_is_tied = 1 AND tie_listed = at_max AND listed = at_max
                THEN 'llm'
            WHEN in_play >= 1 AND in_play = max_independent THEN 'tie'
            WHEN in_play < max_independent THEN 'outvoted'
            ELSE 'fix_reference_only'
        END AS rule
    FROM with_llm
)

SELECT
    d.package_key,
    u.ecosystem,
    u.package_name,
    u.scope,
    d.repo_key,
    d.repo_host,
    d.repo_full_name,
    d.repo_id,
    d.repo_id_rule,
    d.seen_in_gharchive,
    d.claimed_names,
    d.by_ecosystems = 1 AS by_ecosystems,
    d.by_npm_registry = 1 AS by_npm_registry,
    d.by_osv_git_range = 1 AS by_osv_git_range,
    d.by_osv_fix_sole = 1 AS by_osv_fix_sole,
    d.by_osv_fix_shared = 1 AS by_osv_fix_shared,
    d.in_subpath,
    d.osv_evidence_vulns,
    CAST(d.independent_sources AS BIGINT) AS independent_sources,
    CAST(d.candidates AS BIGINT) AS candidates,
    CASE
        WHEN d.decision IS NOT NULL THEN d.decision
        WHEN d.rule = 'review' THEN 'rejected'
        WHEN d.rule = 'sources_agree' THEN 'auto_exact'
        WHEN d.rule IN ('single_source', 'most_sources') THEN 'accepted'
        WHEN d.rule = 'llm' AND d.llm_repo_key = d.repo_key THEN 'accepted'
        WHEN d.rule IN ('llm', 'outvoted') THEN 'rejected'
        ELSE 'suggested'
    END AS status,
    d.rule,
    CASE
        WHEN d.rule IN ('review', 'sources_agree') THEN 'high'
        WHEN d.rule IN ('single_source', 'most_sources', 'llm', 'outvoted') THEN 'medium'
        ELSE 'low'
    END AS confidence,
    CASE
        WHEN d.rule = 'review' THEN 'review'
        WHEN d.rule = 'llm' THEN 'llm'
        ELSE 'rules'
    END AS decided_by,
    CASE WHEN d.rule = 'llm' THEN d.llm_confidence END AS llm_confidence
FROM decided AS d
INNER JOIN {{ ref('int_package_universe') }} AS u ON d.package_key = u.package_key
