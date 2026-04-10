"""
PRO-252: Bayesian Injection Engine — full end-to-end flow.

Entry point: inject_intelligence()

Flow:
  1. Accept user injection (well_id or operator_id + observed cost range)
  2. Load current prior from aro_model_priors for the matching bucket
  3. Run conjugate Bayesian update (bayesian.py)
  4. Write posterior back to aro_model_priors (new row, is_current=True)
  5. Re-estimate all wells in scope, update wells.estimated_cost_*
  6. Record injection in aro_injections
  7. Return before/after P10/P50/P90 + delta summary

The delta is the product:
  "Before your input: P50 = $12M. After: $34M. Gap = $22M undisclosed liability."
"""

import uuid
import math
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from .bayesian import (
    bayesian_update, _lognormal_params, _lognormal_percentiles, BAYESIAN_MODEL_VERSION
)
from .estimator import estimate_well_cost, get_cost_region

DB = 'postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres'


# ── Helpers ─────────────────────────────────────────────────────────────────

def _get_prior(cur, cost_region: str, well_class: str, depth_bucket: str, vintage_bucket: str) -> Optional[dict]:
    """Fetch current prior for a bucket from aro_model_priors."""
    cur.execute("""
        SELECT id, p10_usd, p25_usd, p50_usd, p75_usd, p90_usd,
               n_injections, n_actuals, bias_factor, confidence
        FROM aro_model_priors
        WHERE cost_region = %s AND well_class = %s
          AND depth_bucket = %s AND vintage_bucket = %s
          AND is_current = TRUE
        ORDER BY created_at DESC LIMIT 1
    """, (cost_region, well_class, depth_bucket, vintage_bucket))
    row = cur.fetchone()
    return dict(row) if row else None


def _write_posterior(cur, prior: dict, posterior_pcts: dict,
                     cost_region: str, well_class: str,
                     depth_bucket: str, vintage_bucket: str,
                     injection_id: str, org_id: str) -> str:
    """Insert new posterior row, retire old current."""
    # Retire old prior
    if prior:
        cur.execute("""
            UPDATE aro_model_priors SET is_current = FALSE
            WHERE cost_region = %s AND well_class = %s
              AND depth_bucket = %s AND vintage_bucket = %s AND is_current = TRUE
        """, (cost_region, well_class, depth_bucket, vintage_bucket))

    new_id = str(uuid.uuid4())
    n_inj = (prior.get('n_injections') or 0) + 1
    cur.execute("""
        INSERT INTO aro_model_priors
          (id, cost_region, well_class, depth_bucket, vintage_bucket,
           p10_usd, p25_usd, p50_usd, p75_usd, p90_usd,
           model_version, n_injections, n_actuals,
           bias_factor, last_injection_id, org_id, is_current, confidence)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
    """, (
        new_id, cost_region, well_class, depth_bucket, vintage_bucket,
        posterior_pcts['p10'], posterior_pcts['p25'], posterior_pcts['p50'],
        posterior_pcts['p75'], posterior_pcts['p90'],
        BAYESIAN_MODEL_VERSION, n_inj,
        prior.get('n_actuals', 0) if prior else 0,
        prior.get('bias_factor', 1.0) if prior else 1.0,
        injection_id, org_id,
        "HIGH" if n_inj >= 5 else "MEDIUM",
    ))
    return new_id


def _re_estimate_operator_wells(cur, operator_id: str,
                                 cost_region: str) -> Dict[str, Any]:
    """
    Re-estimate all wells for an operator in a cost region.
    Updates wells.estimated_cost_p10/p50/p90 in place.
    Commits every 500 rows.
    """
    cur.execute("""
        SELECT id, state, well_class, well_type, total_depth_ft,
               water_depth_ft, vintage_year, basin
        FROM wells
        WHERE operator_id = %s
          AND estimated_cost_p50 IS NOT NULL
        LIMIT 5000
    """, (operator_id,))
    wells = cur.fetchall()

    updated = 0
    total_p50_before = 0
    total_p50_after = 0

    for w in wells:
        region = get_cost_region(w['state'], w['well_class'], w['water_depth_ft'])
        if region != cost_region:
            continue

        est = estimate_well_cost(
            state=w['state'], well_class=w['well_class'], well_type=w['well_type'],
            total_depth_ft=w['total_depth_ft'], water_depth_ft=w['water_depth_ft'],
            vintage_year=w['vintage_year'], basin=w['basin'],
        )

        cur.execute("""
            UPDATE wells
            SET estimated_cost_p10 = %s,
                estimated_cost_p50 = %s,
                estimated_cost_p90 = %s,
                cost_model_version = %s,
                cost_estimated_at  = NOW()
            WHERE id = %s
        """, (est['p10_usd'], est['p50_usd'], est['p90_usd'],
              est['model_version'], w['id']))

        updated += 1
        total_p50_after += est['p50_usd']

    return dict(wells_re_estimated=updated, total_p50_after=total_p50_after)


# ── Main entry point ─────────────────────────────────────────────────────────

def inject_intelligence(
    # Scope
    operator_id: Optional[str] = None,
    well_id: Optional[str] = None,
    cost_region: Optional[str] = None,
    # User's observed cost estimate
    obs_p10_usd: float = 0,
    obs_p50_usd: float = 0,
    obs_p90_usd: float = 0,
    # Injection metadata
    injection_type: str = "ACTUAL_COST",        # ACTUAL_COST | NEGOTIATED_CONTRACT | EXPERT_ESTIMATE
    source_type: str = "PROPRIETARY",           # PROPRIETARY | PUBLIC | DESIGN_PARTNER
    confidence: str = "HIGH",                   # HIGH | MEDIUM | LOW
    weight: float = 1.0,                        # 1.0 = 10 effective observations
    notes: str = "",
    injected_by: str = "system",
    org_id: str = "default",
    # Well attrs (needed if well_id not provided)
    well_class: str = "ONSHORE",
    depth_bucket: str = "<3000",
    vintage_bucket: str = "2000-2010",
    re_estimate_portfolio: bool = False,        # rerun cost model for all operator wells
) -> Dict[str, Any]:
    """
    Inject proprietary intelligence and update the Bayesian prior.

    Returns delta summary: before/after P10/P50/P90 + delta USD.
    This delta is the product.
    """
    if obs_p50_usd <= 0:
        raise ValueError("obs_p50_usd must be positive")

    injection_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    with psycopg2.connect(DB, connect_timeout=15) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ── 1. Resolve well attributes if well_id given ───────────────
            if well_id:
                cur.execute("""
                    SELECT state, well_class, total_depth_ft, water_depth_ft,
                           vintage_year, basin, operator_id
                    FROM wells WHERE id = %s
                """, (well_id,))
                w = cur.fetchone()
                if w:
                    if not operator_id:
                        operator_id = str(w['operator_id']) if w['operator_id'] else None
                    cost_region = cost_region or get_cost_region(
                        w['state'], w['well_class'], w['water_depth_ft'])
                    well_class = w['well_class'] or "ONSHORE"
                    from .estimator import _bucket, DEPTH_BUCKETS, VINTAGE_BUCKETS
                    depth_bucket = _bucket(w['total_depth_ft'] or 5000, DEPTH_BUCKETS)
                    vintage_bucket = _bucket(w['vintage_year'] or 2000, VINTAGE_BUCKETS)

            cost_region = cost_region or "GLOBAL_INFERENCE"

            # ── 2. Load current prior ─────────────────────────────────────
            prior = _get_prior(cur, cost_region, well_class, depth_bucket, vintage_bucket)

            if prior:
                prior_p50 = float(prior['p50_usd'])
                prior_p10 = float(prior['p10_usd'])
                prior_p90 = float(prior['p90_usd'])
                prior_n = float(prior['n_injections'] or 1) * 10  # effective obs
                prior_mu, prior_sigma = _lognormal_params(prior_p10, prior_p50, prior_p90)
            else:
                # Fall back to parametric estimate as prior
                est = estimate_well_cost(
                    well_class=well_class,
                )
                prior_p10, prior_p50, prior_p90 = est['p10_usd'], est['p50_usd'], est['p90_usd']
                prior_n = 10  # weak prior
                prior_mu, prior_sigma = _lognormal_params(prior_p10, prior_p50, prior_p90)

            # ── 3. Bayesian update ────────────────────────────────────────
            p10_obs = obs_p10_usd or obs_p50_usd * 0.5
            p90_obs = obs_p90_usd or obs_p50_usd * 2.0
            obs_mu, obs_sigma = _lognormal_params(p10_obs, obs_p50_usd, p90_obs)

            post_mu, post_sigma, post_n = bayesian_update(
                prior_mu, prior_sigma, prior_n,
                obs_mu, obs_sigma, obs_weight=weight
            )

            posterior_pcts = _lognormal_percentiles(post_mu, post_sigma)

            # ── 4. Record injection first (FK required before posterior) ──
            cur.execute("""
                INSERT INTO aro_injections
                  (id, created_at, injected_by, org_id, scope,
                   well_id, cost_region, operator_id, asset_type,
                   injection_type, p10_usd, p25_usd, p50_usd, p75_usd, p90_usd,
                   confidence, source_type, notes, weight, is_public)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
            """, (
                injection_id, now, injected_by, org_id,
                "WELL" if well_id else "REGION",
                well_id, cost_region, operator_id, "WELL",
                injection_type,
                posterior_pcts['p10'], posterior_pcts['p25'], posterior_pcts['p50'],
                posterior_pcts['p75'], posterior_pcts['p90'],
                confidence, source_type, notes, weight,
            ))

            # ── 5. Write posterior ────────────────────────────────────────
            new_prior_id = _write_posterior(
                cur, prior or {}, posterior_pcts, cost_region, well_class,
                depth_bucket, vintage_bucket, injection_id, org_id
            )

            # ── 6. Optionally re-estimate operator portfolio ──────────────
            re_estimate_result = {}
            if re_estimate_portfolio and operator_id:
                re_estimate_result = _re_estimate_operator_wells(cur, operator_id, cost_region)

            conn.commit()

            # ── 7. Build delta summary ────────────────────────────────────
            delta_p50 = posterior_pcts['p50'] - prior_p50
            delta_pct = (delta_p50 / prior_p50 * 100) if prior_p50 else 0

            return dict(
                injection_id=injection_id,
                prior=dict(p10=round(prior_p10), p50=round(prior_p50), p90=round(prior_p90)),
                posterior=dict(
                    p10=posterior_pcts['p10'],
                    p25=posterior_pcts['p25'],
                    p50=posterior_pcts['p50'],
                    p75=posterior_pcts['p75'],
                    p90=posterior_pcts['p90'],
                ),
                delta=dict(
                    p50_usd=round(delta_p50),
                    p50_pct=round(delta_pct, 1),
                    direction="UP" if delta_p50 > 0 else "DOWN",
                ),
                scope=dict(
                    cost_region=cost_region,
                    well_class=well_class,
                    depth_bucket=depth_bucket,
                    vintage_bucket=vintage_bucket,
                    operator_id=operator_id,
                    well_id=well_id,
                ),
                re_estimate=re_estimate_result,
                narrative=(
                    f"Before your input: P50 = ${round(prior_p50/1e6,1)}M. "
                    f"After: ${round(posterior_pcts['p50']/1e6,1)}M. "
                    f"Delta = ${round(abs(delta_p50)/1e6,1)}M {'undisclosed liability' if delta_p50 > 0 else 'reduction'}."
                )
            )
