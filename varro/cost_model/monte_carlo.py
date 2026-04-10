"""
PRO-252: Monte Carlo Engine — 10,000 simulations per operator/well set.

Sampled variables (per PRO-252 spec):
  - cost_base: lognormal(mu, sigma) from posterior
  - cost_escalation: normal(1.0, 0.08) annual over project duration
  - discount_rate: uniform(0.03, 0.10)
  - regulatory_delay_years: triangular(0, 1.0, 4.0)
  - equipment_condition: beta(2, 3) multiplier [0.8, 1.6]

Output: P5/P10/P25/P50/P75/P90/P95 per asset and per portfolio.
"""

from __future__ import annotations
import numpy as np
from typing import Dict, List, Optional, Any

N_SIMULATIONS = 10_000
RNG_SEED = 42  # deterministic for audit trail; caller can override


def _lognormal_params(p10: float, p50: float, p90: float) -> tuple[float, float]:
    """Derive lognormal mu/sigma from P10/P50/P90 estimates."""
    import math
    mu = math.log(p50)
    # Use both P10 and P90 to estimate sigma robustly
    if p10 and p10 > 0 and p90 and p90 > 0:
        sigma_from_p10 = (mu - math.log(p10)) / 1.2816
        sigma_from_p90 = (math.log(p90) - mu) / 1.2816
        sigma = (sigma_from_p10 + sigma_from_p90) / 2
    elif p90 and p90 > 0:
        sigma = (math.log(p90) - mu) / 1.2816
    else:
        sigma = 0.5  # fallback: moderate uncertainty
    return mu, max(sigma, 0.05)


def run_well_monte_carlo(
    p10_usd: float,
    p50_usd: float,
    p90_usd: float,
    project_duration_years: float = 2.0,
    n_simulations: int = N_SIMULATIONS,
    seed: Optional[int] = RNG_SEED,
    include_regulatory_delay: bool = True,
    include_cost_escalation: bool = True,
) -> Dict[str, Any]:
    """
    Run Monte Carlo for a single well.

    Returns P5/P10/P25/P50/P75/P90/P95 plus mean, std, and full sample array.
    """
    rng = np.random.default_rng(seed)

    mu, sigma = _lognormal_params(p10_usd, p50_usd, p90_usd)

    # --- Sample base cost (lognormal) ---
    base_costs = rng.lognormal(mean=mu, sigma=sigma, size=n_simulations)

    # --- Cost escalation: normal around 1.0, compounds over project duration ---
    if include_cost_escalation:
        annual_esc = rng.normal(1.0, 0.08, size=n_simulations)
        esc_mult = np.power(np.clip(annual_esc, 0.90, 1.30), project_duration_years)
    else:
        esc_mult = np.ones(n_simulations)

    # --- Regulatory delay (triangular): adds time-cost at ~$200k/month holding ---
    if include_regulatory_delay:
        delay_years = rng.triangular(0, 1.0, 4.0, size=n_simulations)
        delay_cost = delay_years * 12 * 200_000  # $200k/month holding cost
    else:
        delay_cost = np.zeros(n_simulations)

    # --- Equipment condition (beta): worse condition → higher cost ---
    equip_mult = 0.8 + rng.beta(2, 3, size=n_simulations) * 0.8  # range [0.8, 1.6]

    # --- Total simulated cost ---
    total = base_costs * esc_mult * equip_mult + delay_cost
    total = np.sort(total)

    def q(p):
        return float(np.percentile(total, p))

    return {
        "n_simulations": n_simulations,
        "p05_usd": round(q(5)),
        "p10_usd": round(q(10)),
        "p25_usd": round(q(25)),
        "p50_usd": round(q(50)),
        "p75_usd": round(q(75)),
        "p90_usd": round(q(90)),
        "p95_usd": round(q(95)),
        "mean_usd": round(float(np.mean(total))),
        "std_usd": round(float(np.std(total))),
        "cv": round(float(np.std(total) / np.mean(total)), 3),  # coefficient of variation
        "model_version": "1.2.0",
        "method": "lognormal_monte_carlo",
    }


def run_portfolio_monte_carlo(
    wells: List[Dict[str, float]],
    correlation: float = 0.3,
    n_simulations: int = N_SIMULATIONS,
    seed: Optional[int] = RNG_SEED,
) -> Dict[str, Any]:
    """
    Portfolio Monte Carlo: sum of correlated well costs.

    wells: list of {"p10_usd": ..., "p50_usd": ..., "p90_usd": ..., "id": ...}
    correlation: inter-well cost correlation (0.3 = moderate; weather, mobilisation)

    Returns portfolio P5–P95.
    """
    rng = np.random.default_rng(seed)
    n = len(wells)
    if n == 0:
        return {"error": "No wells provided"}

    # Build correlation matrix (constant off-diagonal)
    corr = np.full((n, n), correlation)
    np.fill_diagonal(corr, 1.0)
    L = np.linalg.cholesky(corr)

    # Sample correlated standard normals
    z = rng.standard_normal((n, n_simulations))
    z_corr = L @ z  # shape (n, n_simulations)

    # Convert to lognormal for each well
    portfolio_totals = np.zeros(n_simulations)
    well_results = []
    for i, well in enumerate(wells):
        mu, sigma = _lognormal_params(
            well.get("p10_usd", well.get("p50_usd", 1e6) * 0.5),
            well.get("p50_usd", 1e6),
            well.get("p90_usd", well.get("p50_usd", 1e6) * 2.0),
        )
        # Convert correlated normal to lognormal
        well_costs = np.exp(mu + sigma * z_corr[i])
        portfolio_totals += well_costs
        well_results.append({
            "id": well.get("id", f"well_{i}"),
            "p50_usd": round(float(np.median(well_costs))),
        })

    portfolio_totals = np.sort(portfolio_totals)

    def q(p):
        return round(float(np.percentile(portfolio_totals, p)))

    return {
        "n_wells": n,
        "n_simulations": n_simulations,
        "correlation": correlation,
        "p05_usd": q(5),
        "p10_usd": q(10),
        "p25_usd": q(25),
        "p50_usd": q(50),
        "p75_usd": q(75),
        "p90_usd": q(90),
        "p95_usd": q(95),
        "mean_usd": round(float(np.mean(portfolio_totals))),
        "diversification_benefit_pct": round(
            (1 - float(np.median(portfolio_totals)) /
             sum(w["p50_usd"] for w in well_results)) * 100, 1
        ),
        "well_breakdowns": well_results,
        "model_version": "1.2.0",
        "method": "correlated_lognormal_portfolio_mc",
    }


def apply_injection_delta(
    base_mc: Dict[str, Any],
    prior_p50: float,
    posterior_p50: float,
    n_simulations: int = N_SIMULATIONS,
    seed: Optional[int] = RNG_SEED,
) -> Dict[str, Any]:
    """
    Fast re-render after Bayesian injection: apply delta multiplier to base MC.
    delta_mult = posterior_p50 / prior_p50
    """
    if prior_p50 <= 0:
        return base_mc
    delta_mult = posterior_p50 / prior_p50
    updated = {k: v for k, v in base_mc.items()}
    for key in ("p05_usd", "p10_usd", "p25_usd", "p50_usd", "p75_usd", "p90_usd", "p95_usd", "mean_usd"):
        if key in updated and isinstance(updated[key], (int, float)):
            updated[key] = round(updated[key] * delta_mult)
    updated["delta_multiplier"] = round(delta_mult, 4)
    updated["method"] = "injection_delta_applied"
    return updated
