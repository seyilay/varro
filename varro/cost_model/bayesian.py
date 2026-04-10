"""
PRO-252: Bayesian update engine.
When a user injects intelligence, this module:
1. Pulls the current prior for that bucket
2. Applies the injection as a likelihood update
3. Writes the posterior back to aro_model_priors
4. Re-estimates any wells in scope
"""

import math, json, psycopg2, psycopg2.extras
from datetime import datetime, timezone
from .estimator import estimate_well_cost, MODEL_VERSION
from .regions import REGION_CONFIG

DB = 'postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres'
BAYESIAN_MODEL_VERSION = f"{MODEL_VERSION}-bayesian"


def _lognormal_params(p10, p50, p90):
    """Recover lognormal mu/sigma from P10/P50/P90."""
    mu = math.log(p50)
    # Average sigma estimate from p10 and p90
    s1 = (math.log(p50) - math.log(p10)) / 1.282
    s2 = (math.log(p90) - math.log(p50)) / 1.282
    sigma = (s1 + s2) / 2
    return mu, max(sigma, 0.01)


def _lognormal_percentiles(mu, sigma):
    """Return P10/P25/P50/P75/P90 from lognormal params."""
    return {
        'p10': round(math.exp(mu - 1.282 * sigma)),
        'p25': round(math.exp(mu - 0.674 * sigma)),
        'p50': round(math.exp(mu)),
        'p75': round(math.exp(mu + 0.674 * sigma)),
        'p90': round(math.exp(mu + 1.282 * sigma)),
    }


def bayesian_update(prior_mu, prior_sigma, prior_n,
                    obs_mu, obs_sigma, obs_weight=1.0):
    """
    Conjugate Bayesian update for lognormal model.
    prior: current model (mu, sigma, effective_n)
    obs:   new injection (mu, sigma, weight)
    Returns: posterior (mu, sigma, new_n)
    """
    # Convert weight to effective observations
    obs_n = obs_weight * 10  # weight=1.0 ~ 10 observations

    # Precision-weighted update
    prior_precision = prior_n / (prior_sigma ** 2) if prior_sigma > 0 else 1
    obs_precision = obs_n / (obs_sigma ** 2) if obs_sigma > 0 else 1

    posterior_precision = prior_precision + obs_precision
    posterior_mu = (prior_precision * prior_mu + obs_precision * obs_mu) / posterior_precision
    posterior_sigma = math.sqrt(1.0 / posterior_precision)
    posterior_n = prior_n + obs_n

    return posterior_mu, posterior_sigma, posterior_n


def apply_injection(injection: dict, conn=None) -> dict:
    """
    Apply a single injection to the model.
    injection: dict with keys matching aro_injections columns
    Returns: dict with updated posterior percentiles
    """
    close_conn = conn is None
    if conn is None:
        conn = psycopg2.connect(DB, connect_timeout=15, options='-c statement_timeout=30s')
    cur = conn.cursor()

    scope = injection.get('scope', 'REGION')
    cost_region = injection.get('cost_region')
    well_class = injection.get('well_class', 'ONSHORE')
    depth_bucket = injection.get('depth_bucket', '3000-8000')
    vintage_bucket = injection.get('vintage_bucket', '2000-2010')
    org_id = injection.get('org_id')
    weight = float(injection.get('weight', 1.0))

    # Get injection P10/P50/P90
    inj_p10 = injection.get('p10_usd')
    inj_p50 = injection.get('p50_usd')
    inj_p90 = injection.get('p90_usd')
    if not all([inj_p10, inj_p50, inj_p90]):
        raise ValueError("Injection must have p10_usd, p50_usd, p90_usd")

    # Get current prior for this bucket
    cur.execute("""
        SELECT p10_usd, p50_usd, p90_usd, n_injections, n_actuals
        FROM aro_model_priors
        WHERE cost_region = %s AND well_class = %s
          AND depth_bucket = %s AND vintage_bucket = %s
          AND is_current = TRUE
          AND (org_id = %s OR org_id IS NULL)
        ORDER BY org_id NULLS LAST
        LIMIT 1
    """, (cost_region, well_class, depth_bucket, vintage_bucket, org_id))

    prior_row = cur.fetchone()
    if prior_row:
        prior_p10, prior_p50, prior_p90, prior_n, prior_n_actuals = prior_row
        prior_n = prior_n or 0
    else:
        # Bootstrap from parametric model
        est = estimate_well_cost(
            well_class=well_class,
            total_depth_ft={'<3000':1500,'3000-8000':5000,'8000-15000':11000,'15000+':18000}[depth_bucket],
            vintage_year={'<1970':1960,'1970-1985':1977,'1985-2000':1992,'2000-2010':2005,'2010+':2015}[vintage_bucket],
        )
        prior_p10, prior_p50, prior_p90 = est['p10_usd'], est['p50_usd'], est['p90_usd']
        prior_n = 0
        prior_n_actuals = 0

    # Convert to lognormal params
    prior_mu, prior_sigma = _lognormal_params(float(prior_p10), float(prior_p50), float(prior_p90))
    obs_mu, obs_sigma = _lognormal_params(float(inj_p10), float(inj_p50), float(inj_p90))

    # Bayesian update
    effective_prior_n = max(prior_n, 5)  # minimum prior weight
    post_mu, post_sigma, post_n = bayesian_update(
        prior_mu, prior_sigma, effective_prior_n,
        obs_mu, obs_sigma, weight
    )

    # Posterior percentiles
    posterior = _lognormal_percentiles(post_mu, post_sigma)
    n_actuals = prior_n_actuals + (1 if injection.get('source_type') == 'ACTUAL' else 0)

    confidence_map = {1: 'HIGH', 2: 'MEDIUM', 3: 'LOW'}
    tier = REGION_CONFIG.get(cost_region or 'GLOBAL_INFERENCE', (3, 1.0))[0]
    confidence = 'HIGH' if int(post_n) >= 30 else 'MEDIUM' if int(post_n) >= 10 else confidence_map.get(tier, 'LOW')

    # Mark old prior as not current
    if prior_row:
        cur.execute("""
            UPDATE aro_model_priors SET is_current = FALSE
            WHERE cost_region = %s AND well_class = %s
              AND depth_bucket = %s AND vintage_bucket = %s
              AND is_current = TRUE
              AND (org_id = %s OR org_id IS NULL)
        """, (cost_region, well_class, depth_bucket, vintage_bucket, org_id))

    # Write new posterior
    cur.execute("""
        INSERT INTO aro_model_priors
          (cost_region, well_class, depth_bucket, vintage_bucket,
           p10_usd, p25_usd, p50_usd, p75_usd, p90_usd,
           model_version, n_injections, n_actuals, confidence,
           last_injection_id, org_id, is_current)
        VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s, TRUE)
    """, (
        cost_region, well_class, depth_bucket, vintage_bucket,
        posterior['p10'], posterior['p25'], posterior['p50'],
        posterior['p75'], posterior['p90'],
        BAYESIAN_MODEL_VERSION, int(post_n), n_actuals, confidence,
        injection.get('id'), org_id
    ))

    conn.commit()
    if close_conn:
        conn.close()

    delta_pct = round((posterior['p50'] - float(prior_p50)) / float(prior_p50) * 100, 1)
    return {
        'prior':     {'p10': float(prior_p10), 'p50': float(prior_p50), 'p90': float(prior_p90)},
        'posterior': posterior,
        'delta_pct': delta_pct,
        'n_injections': int(post_n),
        'confidence': confidence,
        'bucket': f"{cost_region}/{well_class}/{depth_bucket}/{vintage_bucket}",
    }
