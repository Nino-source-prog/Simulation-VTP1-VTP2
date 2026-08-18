#!/usr/bin/env python3
"""Analyse reproductible des résultats de ``vtp-dsr-comparison.cc``.

Le script ne dépend que de la bibliothèque standard Python. Il produit :

* un résumé descriptif par configuration et politique ;
* une table appariée TRAD–VTP1–VTP2, graine par graine ;
* des statistiques appariées pour VTP1-TRAD, VTP2-VTP1 et VTP2-TRAD ;
* le test exact de Wilcoxon par énumération des signes des rangs absolus ;
* la correction de Holm pour la famille confirmatoire ;
* l'estimateur de Hodges-Lehmann et un intervalle bootstrap apparié ;
* des tests exacts de McNemar pour des critères de crise ;
* la table appariée du terme analytique VTP2-VTP1 ;
* des contrôles bloquants d'instrumentation, de conservation et des nouveaux
  compteurs exacts original/retransmission selon C(t) et P(t) ;
* une clé expérimentale complète, des intervalles de Student et des résumés
  robustes incluant min, max, p90 et p95.

Le chargeur répare automatiquement un défaut connu du format CSV v4.4 : une
valeur ``TR_samples`` dupliquée avant ``discovery_episode_ms`` décale toutes les
colonnes suivantes d'une position. La réparation est signalée à l'écran.

Exemple
-------
python3 analyze-vtp-results.py --input vtp-dsr-raw.csv --output-dir analysis

Pour limiter une analyse :
python3 analyze-vtp-results.py --input vtp-dsr-raw.csv --output-dir analysis \\
    --scenario full-independent --speed 20
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import random
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


# Toutes les métriques utiles au résumé. Les champs absents du CSV sont ignorés.
KEY_METRICS: Tuple[str, ...] = (
    "pdr_pct",
    "e2e_delay_ms",
    "penalized_delay_ms",
    "jitter_ms",
    "recovery_latency_ms",
    "recovery_latency_median_ms",
    "recovery_latency_p95_ms",
    "recovery_latency_max_ms",
    "tx_original_submissions",
    "tx_retrans_submissions",
    "phy_original_attempts",
    "phy_retrans_attempts",  # métrique primaire ajoutée
    "phy_late_attempts",
    "attempts_bad_channel",
    "originals_bad_channel",
    "retransmissions_bad_channel",
    "attempts_without_route",
    "originals_without_route",
    "retransmissions_without_route",
    "original_c0_p0",
    "original_c0_p1",
    "original_c1_p0",
    "original_c1_p1",
    "retransmission_c0_p0",
    "retransmission_c0_p1",
    "retransmission_c1_p0",
    "retransmission_c1_p1",
    "bad_channel_split_delta",
    "no_route_split_delta",
    "original_state_balance_delta",
    "retransmission_state_balance_delta",
    "graph_path_samples",
    "graph_path_available_fraction",
    "cache_graph_agreement_fraction",
    "cache_false_positive_fraction",
    "cache_false_negative_fraction",
    "cache_false_positive_rate",
    "cache_false_negative_rate",
    "policy_graph_agreement_fraction",
    "policy_false_positive_fraction",
    "policy_false_negative_fraction",
    "policy_false_positive_rate",
    "policy_false_negative_rate",
    "cache_graph_balance_delta",
    "policy_graph_balance_delta",
    "rto_expirations",
    "rto_retransmissions",
    "recovery_retransmissions",
    "blocked_rto_expirations",
    "expired",
    "expired_wait_channel",
    "expired_wait_route",
    "expired_wait_both",
    "expired_in_flight",
    "pending_end",
    "acked",
    "rx_unique",
    "duplicates",
    "network_energy_j",
    "energy_uj_per_delivered_bit",
    "TR_ms",
    "discovery_episode_ms",
    "discovery_completed_count",
    "discovery_failed_count",
    "discovery_incomplete_count",
    "discovery_completion_match_misses",
    "rreq_timer_cancelled_count",
    "rreq_timer_incomplete_count",
    "rreq_timer_balance_delta",
    "fR_per_s",
    "pi_p",
    "route_mean_outage_ms",
    "Rp_ms",
    "path_wait_component_ms",
    "reset_cost_ms",
)

# Métriques de la table appariée. Elles peuvent être remplacées par --metrics.
DEFAULT_PAIRED_METRICS: Tuple[str, ...] = (
    "phy_retrans_attempts",
    "jitter_ms",
    "e2e_delay_ms",
    "network_energy_j",
    "pdr_pct",
    "penalized_delay_ms",
    "tx_retrans_submissions",
    "energy_uj_per_delivered_bit",
    "expired",
    "attempts_without_route",
    "originals_without_route",
    "retransmissions_without_route",
    "attempts_bad_channel",
    "originals_bad_channel",
    "retransmissions_bad_channel",
    "blocked_rto_expirations",
    "recovery_latency_ms",
)

# Famille confirmatoire décrite dans le manuscrit.
DEFAULT_PRIMARY_METRICS: Tuple[str, ...] = (
    "phy_retrans_attempts",
    "jitter_ms",
    "e2e_delay_ms",
    "network_energy_j",
)

# Sens d'amélioration pour les comptes "en faveur / contre".
HIGHER_IS_BETTER = {"pdr_pct", "rx_unique", "acked"}

POLICY_COMPARISONS: Tuple[Tuple[str, str], ...] = (
    ("VTP1", "TRAD"),
    ("VTP2", "VTP1"),
    ("VTP2", "TRAD"),
)

# Les paramètres qui définissent une configuration expérimentale dans le CSV.
# Les champs absents sont simplement omis.
DESIGN_FIELDS: Tuple[str, ...] = (
    "schema_version",
    "campaign_id",
    "configuration_hash",
    "rng_seed",
    "scenario",
    "speed",
    "nodes",
    "pause_s",
    "area_x_m",
    "area_y_m",
    "range_m",
    "sim_time_s",
    "app_start_s",
    "generation_stop_s",
    "app_stop_margin_s",
    "packet_size_bytes",
    "generation_interval_s",
    "rto_initial_s",
    "rto_max_s",
    "lifetime_s",
    "max_attempts",
    "route_poll_s",
    "dsr_grace_s",
    "force_route_available",
    "route_cache_probe_mode",
    "recovery_pacing_s",
    "outage_base_start_s",
    "outage_on_duration_s",
    "outage_off_duration_s",
    "outage_cycles_requested",
    "physical_channel_outage",
    "graph_path_diagnostic_enabled",
    "graph_path_poll_s",
)


@dataclass(frozen=True)
class CrisisSpec:
    name: str
    metric: str
    operator: str
    threshold: float

    def is_crisis(self, value: float) -> bool:
        if not math.isfinite(value):
            return False
        if self.operator == "lt":
            return value < self.threshold
        if self.operator == "le":
            return value <= self.threshold
        if self.operator == "gt":
            return value > self.threshold
        if self.operator == "ge":
            return value >= self.threshold
        raise ValueError(f"Opérateur de crise inconnu: {self.operator}")


DEFAULT_CRISES: Tuple[CrisisSpec, ...] = (
    CrisisSpec("pdr_below_99", "pdr_pct", "lt", 99.0),
    CrisisSpec("expired_above_20", "expired", "gt", 20.0),
    CrisisSpec("penalized_delay_above_2000", "penalized_delay_ms", "gt", 2000.0),
    CrisisSpec("submissions_above_900", "tx_retrans_submissions", "gt", 900.0),
    CrisisSpec("route_outage_above_10000", "route_mean_outage_ms", "gt", 10000.0),
)


def as_float(row: Mapping[str, str], key: str) -> float:
    value = row.get(key, "")
    try:
        return float(value) if value not in (None, "") else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def as_int(row: Mapping[str, str], key: str) -> int:
    value = as_float(row, key)
    if not math.isfinite(value):
        raise ValueError(f"Valeur entière absente ou invalide pour {key}: {row.get(key)!r}")
    return int(round(value))


def clean_values(values: Iterable[float]) -> List[float]:
    return [v for v in values if math.isfinite(v)]


def mean(values: Sequence[float]) -> float:
    clean = clean_values(values)
    return statistics.fmean(clean) if clean else float("nan")


def median(values: Sequence[float]) -> float:
    clean = clean_values(values)
    return statistics.median(clean) if clean else float("nan")


def sample_sd(values: Sequence[float]) -> float:
    clean = clean_values(values)
    return statistics.stdev(clean) if len(clean) >= 2 else 0.0


def percentile(values: Sequence[float], probability: float) -> float:
    """Percentile linéaire de type 7, compatible avec l'usage descriptif courant."""
    clean = sorted(clean_values(values))
    if not clean:
        return float("nan")
    if len(clean) == 1:
        return clean[0]
    p = min(1.0, max(0.0, probability))
    position = (len(clean) - 1) * p
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[lower]
    fraction = position - lower
    return clean[lower] * (1.0 - fraction) + clean[upper] * fraction


def student_t_975(df: int) -> float:
    """Approximation de t_(0.975, df), précise pour l'usage descriptif."""
    if df <= 0:
        return float("nan")
    # Valeurs exactes usuelles pour les petits échantillons.
    table = {
        1: 12.706204736, 2: 4.302652730, 3: 3.182446305,
        4: 2.776445105, 5: 2.570581836, 6: 2.446911851,
        7: 2.364624252, 8: 2.306004135, 9: 2.262157163,
        10: 2.228138852, 11: 2.200985160, 12: 2.178812830,
        13: 2.160368656, 14: 2.144786688, 15: 2.131449546,
        16: 2.119905299, 17: 2.109815578, 18: 2.100922040,
        19: 2.093024054, 20: 2.085963447, 21: 2.079613845,
        22: 2.073873068, 23: 2.068657610, 24: 2.063898562,
        25: 2.059538553, 26: 2.055529439, 27: 2.051830516,
        28: 2.048407142, 29: 2.045229642, 30: 2.042272456,
    }
    if df in table:
        return table[df]
    # Développement de Cornish-Fisher autour du quantile normal.
    z = 1.959963984540054
    v = float(df)
    return (z + (z**3 + z) / (4.0 * v)
            + (5.0*z**5 + 16.0*z**3 + 3.0*z) / (96.0*v**2)
            + (3.0*z**7 + 19.0*z**5 + 17.0*z**3 - 15.0*z) / (384.0*v**3))


def student_ci95_half_width(values: Sequence[float]) -> float:
    clean = clean_values(values)
    if len(clean) < 2:
        return 0.0
    return student_t_975(len(clean) - 1) * statistics.stdev(clean) / math.sqrt(len(clean))


def stable_seed(base_seed: int, *parts: object) -> int:
    text = "|".join(str(p) for p in (base_seed,) + parts)
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def load_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str], int]:
    """Charge le CSV et répare automatiquement le décalage connu de la v4.4."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise SystemExit(f"Fichier vide: {path}") from exc

        header = [name.strip() for name in header]
        expected = len(header)
        repaired = 0
        rows: List[Dict[str, str]] = []

        tr_samples_index = header.index("TR_samples") if "TR_samples" in header else -1
        discovery_index = (
            header.index("discovery_episode_ms") if "discovery_episode_ms" in header else -1
        )

        for line_number, raw in enumerate(reader, start=2):
            if not raw or all(not cell.strip() for cell in raw):
                continue
            row = list(raw)

            # Défaut connu de v4.4 : TR_samples est écrit deux fois dans la ligne.
            if (
                len(row) == expected + 1
                and tr_samples_index >= 0
                and discovery_index == tr_samples_index + 1
                and row[tr_samples_index] == row[discovery_index]
            ):
                del row[discovery_index]
                repaired += 1

            if len(row) != expected:
                raise SystemExit(
                    f"Ligne {line_number}: {len(row)} colonnes pour {expected} en-têtes. "
                    "Corriger le producteur CSV ou examiner la ligne brute."
                )
            rows.append(dict(zip(header, row)))

    if not rows:
        raise SystemExit(f"Aucune donnée dans {path}")
    return rows, header, repaired


def filter_rows(
    rows: Sequence[Mapping[str, str]],
    scenarios: Sequence[str],
    speeds: Sequence[float],
) -> List[Mapping[str, str]]:
    scenario_set = set(scenarios)
    result: List[Mapping[str, str]] = []
    for row in rows:
        if scenario_set and row.get("scenario", "") not in scenario_set:
            continue
        if speeds:
            speed = as_float(row, "speed")
            if not any(math.isclose(speed, target, rel_tol=0.0, abs_tol=1e-9) for target in speeds):
                continue
        result.append(row)
    if not result:
        raise SystemExit("Aucune ligne après application des filtres.")
    return result


def active_design_fields(header: Sequence[str]) -> Tuple[str, ...]:
    return tuple(field for field in DESIGN_FIELDS if field in header)


def design_key(row: Mapping[str, str], fields: Sequence[str]) -> Tuple[str, ...]:
    return tuple(row.get(field, "") for field in fields)


def design_dict(key: Sequence[str], fields: Sequence[str]) -> Dict[str, str]:
    return dict(zip(fields, key))


def write_summary(
    rows: Sequence[Mapping[str, str]],
    output: Path,
    fields_in_file: Sequence[str],
    design_fields: Sequence[str],
    crises: Sequence[CrisisSpec],
) -> None:
    groups: Dict[Tuple[Tuple[str, ...], str], List[Mapping[str, str]]] = defaultdict(list)
    for row in rows:
        groups[(design_key(row, design_fields), row.get("policy", "unknown"))].append(row)

    metrics = [metric for metric in KEY_METRICS if metric in fields_in_file]
    active_crises = [crisis for crisis in crises if crisis.metric in fields_in_file]
    fields = list(design_fields) + ["policy", "n_runs"]
    for metric in metrics:
        fields.extend(
            (
                f"{metric}_n",
                f"{metric}_mean",
                f"{metric}_median",
                f"{metric}_sd",
                f"{metric}_min",
                f"{metric}_q25",
                f"{metric}_q75",
                f"{metric}_p90",
                f"{metric}_p95",
                f"{metric}_max",
                f"{metric}_student_ci95_half_width",
            )
        )
    for crisis in active_crises:
        fields.extend((f"crisis_{crisis.name}_count", f"crisis_{crisis.name}_fraction"))

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for (key, policy), group in sorted(groups.items(), key=lambda item: (item[0][0], item[0][1])):
            out: Dict[str, object] = design_dict(key, design_fields)
            out.update({"policy": policy, "n_runs": len(group)})
            for metric in metrics:
                values = clean_values(as_float(row, metric) for row in group)
                out[f"{metric}_n"] = len(values)
                out[f"{metric}_mean"] = mean(values)
                out[f"{metric}_median"] = median(values)
                out[f"{metric}_sd"] = sample_sd(values)
                out[f"{metric}_min"] = min(values) if values else float("nan")
                out[f"{metric}_q25"] = percentile(values, 0.25)
                out[f"{metric}_q75"] = percentile(values, 0.75)
                out[f"{metric}_p90"] = percentile(values, 0.90)
                out[f"{metric}_p95"] = percentile(values, 0.95)
                out[f"{metric}_max"] = max(values) if values else float("nan")
                out[f"{metric}_student_ci95_half_width"] = student_ci95_half_width(values)
            for crisis in active_crises:
                count = sum(crisis.is_crisis(as_float(row, crisis.metric)) for row in group)
                out[f"crisis_{crisis.name}_count"] = count
                out[f"crisis_{crisis.name}_fraction"] = count / len(group) if group else float("nan")
            writer.writerow(out)


def build_index(
    rows: Sequence[Mapping[str, str]], design_fields: Sequence[str]
) -> Tuple[
    Dict[Tuple[Tuple[str, ...], int, str], Mapping[str, str]],
    List[Tuple[Tuple[str, ...], int]],
]:
    indexed: Dict[Tuple[Tuple[str, ...], int, str], Mapping[str, str]] = {}
    runs = set()
    for row in rows:
        key = design_key(row, design_fields)
        run = as_int(row, "run")
        policy = row.get("policy", "")
        compound = (key, run, policy)
        if compound in indexed:
            raise SystemExit(
                "Doublon de configuration/policy/run détecté. Ajoutez au CSV un identifiant "
                "de campagne ou séparez les fichiers avant analyse: " + str(compound)
            )
        indexed[compound] = row
        runs.add((key, run))
    return indexed, sorted(runs)


def write_paired_values(
    indexed: Mapping[Tuple[Tuple[str, ...], int, str], Mapping[str, str]],
    runs: Sequence[Tuple[Tuple[str, ...], int]],
    output: Path,
    metrics: Sequence[str],
    design_fields: Sequence[str],
) -> None:
    fields = list(design_fields) + [
        "run",
        "outage_phase_s",
        "outage_first_start_s",
        "metric",
        "TRAD",
        "VTP1",
        "VTP2",
        "VTP1_minus_TRAD",
        "VTP2_minus_VTP1",
        "VTP2_minus_TRAD",
    ]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for key, run in runs:
            policy_rows = {
                policy: indexed.get((key, run, policy)) for policy in ("TRAD", "VTP1", "VTP2")
            }
            for metric in metrics:
                values = {
                    policy: as_float(row, metric) if row is not None else float("nan")
                    for policy, row in policy_rows.items()
                }
                out: Dict[str, object] = design_dict(key, design_fields)
                metadata_row = next((row for row in policy_rows.values() if row is not None), None)
                out.update(
                    {
                        "run": run,
                        "outage_phase_s": as_float(metadata_row, "outage_phase_s") if metadata_row else float("nan"),
                        "outage_first_start_s": as_float(metadata_row, "outage_first_start_s") if metadata_row else float("nan"),
                        "metric": metric,
                        "TRAD": values["TRAD"],
                        "VTP1": values["VTP1"],
                        "VTP2": values["VTP2"],
                        "VTP1_minus_TRAD": values["VTP1"] - values["TRAD"],
                        "VTP2_minus_VTP1": values["VTP2"] - values["VTP1"],
                        "VTP2_minus_TRAD": values["VTP2"] - values["TRAD"],
                    }
                )
                writer.writerow(out)


def midranks(values: Sequence[float]) -> List[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i + 1
        while j < len(indexed) and math.isclose(
            indexed[j][1], indexed[i][1], rel_tol=1e-12, abs_tol=1e-12
        ):
            j += 1
        # Rangs 1..n : moyenne du bloc i+1 à j.
        rank = ((i + 1) + j) / 2.0
        for k in range(i, j):
            ranks[indexed[k][0]] = rank
        i = j
    return ranks


def exact_wilcoxon_signed_rank(differences: Sequence[float], zero_tol: float = 1e-12) -> Tuple[float, int, float]:
    """Test exact bilatéral par distribution de toutes les affectations de signes.

    Les zéros sont supprimés. Les valeurs absolues ex æquo reçoivent des mid-ranks.
    La distribution exacte est calculée par programmation dynamique, donc trente
    paires ne nécessitent pas d'énumérer explicitement 2^30 configurations.
    """
    diffs = [d for d in differences if math.isfinite(d) and abs(d) > zero_tol]
    m = len(diffs)
    if m == 0:
        return 1.0, 0, 0.0

    ranks = midranks([abs(d) for d in diffs])
    scaled_ranks = [int(round(rank * 2.0)) for rank in ranks]
    observed = sum(rank for rank, d in zip(scaled_ranks, diffs) if d > 0)
    total_rank = sum(scaled_ranks)

    distribution: Dict[int, int] = {0: 1}
    for rank in scaled_ranks:
        updated: Dict[int, int] = defaultdict(int)
        for rank_sum, count in distribution.items():
            updated[rank_sum] += count
            updated[rank_sum + rank] += count
        distribution = dict(updated)

    observed_distance = abs(2 * observed - total_rank)
    extreme = sum(
        count for rank_sum, count in distribution.items()
        if abs(2 * rank_sum - total_rank) >= observed_distance
    )
    total_assignments = 1 << m
    p_value = min(1.0, extreme / total_assignments)
    return p_value, m, observed / 2.0


def hodges_lehmann(differences: Sequence[float]) -> float:
    clean = clean_values(differences)
    if not clean:
        return float("nan")
    walsh = [(clean[i] + clean[j]) / 2.0 for i in range(len(clean)) for j in range(i, len(clean))]
    return statistics.median(walsh)


def bootstrap_hl_interval(
    differences: Sequence[float],
    repetitions: int,
    seed: int,
    alpha: float = 0.05,
) -> Tuple[float, float]:
    clean = clean_values(differences)
    if not clean or repetitions <= 0:
        return float("nan"), float("nan")
    if len(clean) == 1:
        return clean[0], clean[0]
    rng = random.Random(seed)
    n = len(clean)
    estimates: List[float] = []
    append = estimates.append
    for _ in range(repetitions):
        sample = [clean[rng.randrange(n)] for _ in range(n)]
        append(hodges_lehmann(sample))
    return percentile(estimates, alpha / 2.0), percentile(estimates, 1.0 - alpha / 2.0)


def holm_adjust(p_values: Sequence[float]) -> List[float]:
    m = len(p_values)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: p_values[i])
    adjusted = [1.0] * m
    running = 0.0
    for rank, index in enumerate(order):
        candidate = min(1.0, (m - rank) * p_values[index])
        running = max(running, candidate)
        adjusted[index] = running
    return adjusted


def direction_counts(metric: str, differences: Sequence[float], zero_tol: float = 1e-12) -> Tuple[int, int, int]:
    favor = against = ties = 0
    higher = metric in HIGHER_IS_BETTER
    for diff in clean_values(differences):
        if abs(diff) <= zero_tol:
            ties += 1
        elif (diff > 0 and higher) or (diff < 0 and not higher):
            favor += 1
        else:
            against += 1
    return favor, against, ties


def collect_differences(
    indexed: Mapping[Tuple[Tuple[str, ...], int, str], Mapping[str, str]],
    runs: Sequence[Tuple[Tuple[str, ...], int]],
    key: Tuple[str, ...],
    first: str,
    second: str,
    metric: str,
) -> Tuple[List[float], List[int]]:
    differences: List[float] = []
    paired_runs: List[int] = []
    for run_key, run in runs:
        if run_key != key:
            continue
        row_first = indexed.get((key, run, first))
        row_second = indexed.get((key, run, second))
        if row_first is None or row_second is None:
            continue
        value_first = as_float(row_first, metric)
        value_second = as_float(row_second, metric)
        if math.isfinite(value_first) and math.isfinite(value_second):
            differences.append(value_first - value_second)
            paired_runs.append(run)
    return differences, paired_runs


def write_paired_statistics(
    indexed: Mapping[Tuple[Tuple[str, ...], int, str], Mapping[str, str]],
    runs: Sequence[Tuple[Tuple[str, ...], int]],
    output: Path,
    metrics: Sequence[str],
    primary_metrics: Sequence[str],
    design_fields: Sequence[str],
    bootstrap_reps: int,
    bootstrap_seed: int,
    holm_scope: str,
) -> List[Dict[str, object]]:
    keys = sorted({key for key, _ in runs})
    records: List[Dict[str, object]] = []

    for key in keys:
        for first, second in POLICY_COMPARISONS:
            comparison = f"{first}-{second}"
            for metric in metrics:
                differences, paired_runs = collect_differences(
                    indexed, runs, key, first, second, metric
                )
                if not differences:
                    continue
                p_value, n_nonzero, statistic = exact_wilcoxon_signed_rank(differences)
                hl = hodges_lehmann(differences)
                ci_seed = stable_seed(bootstrap_seed, key, comparison, metric)
                ci_low, ci_high = bootstrap_hl_interval(
                    differences, bootstrap_reps, ci_seed
                )
                favor, against, ties = direction_counts(metric, differences)
                record: Dict[str, object] = design_dict(key, design_fields)
                record.update(
                    {
                        "comparison": comparison,
                        "first_policy": first,
                        "second_policy": second,
                        "metric": metric,
                        "n_pairs": len(differences),
                        "n_nonzero": n_nonzero,
                        "runs": ";".join(str(run) for run in paired_runs),
                        "mean_difference": mean(differences),
                        "median_difference": median(differences),
                        "sd_difference": sample_sd(differences),
                        "q25_difference": percentile(differences, 0.25),
                        "q75_difference": percentile(differences, 0.75),
                        "hodges_lehmann": hl,
                        "hl_bootstrap_ci_low": ci_low,
                        "hl_bootstrap_ci_high": ci_high,
                        "bootstrap_repetitions": bootstrap_reps,
                        "bootstrap_seed": ci_seed,
                        "favor_first": favor,
                        "against_first": against,
                        "ties": ties,
                        "wilcoxon_w_plus": statistic,
                        "wilcoxon_exact_p": p_value,
                        "primary_family": int(metric in primary_metrics),
                        "holm_scope": holm_scope,
                        "holm_family_size": "",
                        "holm_adjusted_p": "",
                    }
                )
                records.append(record)

    # Holm est défini explicitement par l'utilisateur :
    # - configuration (défaut) : toutes les métriques primaires et les trois contrastes ;
    # - contrast : famille séparée pour chaque contraste.
    grouped_indices: Dict[Tuple[object, ...], List[int]] = defaultdict(list)
    for index, record in enumerate(records):
        if int(record["primary_family"]) != 1:
            continue
        key = tuple(str(record[field]) for field in design_fields)
        family: Tuple[object, ...]
        if holm_scope == "contrast":
            family = (key, str(record["comparison"]))
        else:
            family = (key,)
        grouped_indices[family].append(index)

    for indices in grouped_indices.values():
        p_values = [float(records[i]["wilcoxon_exact_p"]) for i in indices]
        adjusted = holm_adjust(p_values)
        for i, p_adj in zip(indices, adjusted):
            records[i]["holm_family_size"] = len(indices)
            records[i]["holm_adjusted_p"] = p_adj

    fields = list(design_fields) + [
        "comparison",
        "first_policy",
        "second_policy",
        "metric",
        "n_pairs",
        "n_nonzero",
        "runs",
        "mean_difference",
        "median_difference",
        "sd_difference",
        "q25_difference",
        "q75_difference",
        "hodges_lehmann",
        "hl_bootstrap_ci_low",
        "hl_bootstrap_ci_high",
        "bootstrap_repetitions",
        "bootstrap_seed",
        "favor_first",
        "against_first",
        "ties",
        "wilcoxon_w_plus",
        "wilcoxon_exact_p",
        "primary_family",
        "holm_scope",
        "holm_family_size",
        "holm_adjusted_p",
    ]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    return records


def binomial_cdf_half(k: int, n: int) -> float:
    if n < 0 or k < 0:
        return 0.0
    k = min(k, n)
    numerator = sum(math.comb(n, i) for i in range(k + 1))
    return numerator / (2 ** n)


def exact_mcnemar_p(first_only: int, second_only: int) -> float:
    discordant = first_only + second_only
    if discordant == 0:
        return 1.0
    k = min(first_only, second_only)
    return min(1.0, 2.0 * binomial_cdf_half(k, discordant))


def write_mcnemar(
    indexed: Mapping[Tuple[Tuple[str, ...], int, str], Mapping[str, str]],
    runs: Sequence[Tuple[Tuple[str, ...], int]],
    output: Path,
    crises: Sequence[CrisisSpec],
    design_fields: Sequence[str],
) -> None:
    keys = sorted({key for key, _ in runs})
    fields = list(design_fields) + [
        "comparison",
        "crisis_name",
        "metric",
        "operator",
        "threshold",
        "n_pairs",
        "both_crisis",
        "first_only_crisis",
        "second_only_crisis",
        "neither_crisis",
        "discordant_pairs",
        "mcnemar_exact_p",
    ]
    records: List[Dict[str, object]] = []

    for key in keys:
        for first, second in POLICY_COMPARISONS:
            comparison = f"{first}-{second}"
            for crisis in crises:
                both = first_only = second_only = neither = 0
                n_pairs = 0
                for run_key, run in runs:
                    if run_key != key:
                        continue
                    row_first = indexed.get((key, run, first))
                    row_second = indexed.get((key, run, second))
                    if row_first is None or row_second is None:
                        continue
                    value_first = as_float(row_first, crisis.metric)
                    value_second = as_float(row_second, crisis.metric)
                    if not (math.isfinite(value_first) and math.isfinite(value_second)):
                        continue
                    n_pairs += 1
                    event_first = crisis.is_crisis(value_first)
                    event_second = crisis.is_crisis(value_second)
                    if event_first and event_second:
                        both += 1
                    elif event_first:
                        first_only += 1
                    elif event_second:
                        second_only += 1
                    else:
                        neither += 1

                if n_pairs == 0:
                    continue
                record: Dict[str, object] = design_dict(key, design_fields)
                record.update(
                    {
                        "comparison": comparison,
                        "crisis_name": crisis.name,
                        "metric": crisis.metric,
                        "operator": crisis.operator,
                        "threshold": crisis.threshold,
                        "n_pairs": n_pairs,
                        "both_crisis": both,
                        "first_only_crisis": first_only,
                        "second_only_crisis": second_only,
                        "neither_crisis": neither,
                        "discordant_pairs": first_only + second_only,
                        "mcnemar_exact_p": exact_mcnemar_p(first_only, second_only),
                    }
                )
                records.append(record)

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)


def write_paired_model(
    indexed: Mapping[Tuple[Tuple[str, ...], int, str], Mapping[str, str]],
    runs: Sequence[Tuple[Tuple[str, ...], int]],
    output: Path,
    design_fields: Sequence[str],
) -> None:
    fields = list(design_fields) + [
        "run",
        "vtp1_recovery_ms",
        "vtp2_recovery_ms",
        "observed_extra_vtp2_ms",
        "pi_p_Rp_ms",
        "reset_cost_ms",
        "computed_direct_extra_vtp2_ms",
        "computed_vtp2_ms",
        "model_residual_ms",
        "vtp1_penalized_delay_ms",
        "vtp2_penalized_delay_ms",
        "observed_penalized_extra_ms",
    ]
    records: List[Dict[str, object]] = []
    for key, run in runs:
        vtp1 = indexed.get((key, run, "VTP1"))
        vtp2 = indexed.get((key, run, "VTP2"))
        if vtp1 is None or vtp2 is None:
            continue
        l1 = as_float(vtp1, "recovery_latency_ms")
        l2 = as_float(vtp2, "recovery_latency_ms")
        d1p = as_float(vtp1, "penalized_delay_ms")
        d2p = as_float(vtp2, "penalized_delay_ms")
        path_component = as_float(vtp2, "path_wait_component_ms")
        reset_component = as_float(vtp2, "reset_cost_ms")
        if not all(math.isfinite(v) for v in (l1, l2, d1p, d2p, path_component, reset_component)):
            continue
        computed_extra = path_component + reset_component
        computed_vtp2 = l1 + computed_extra
        out: Dict[str, object] = design_dict(key, design_fields)
        out.update(
            {
                "run": run,
                "vtp1_recovery_ms": l1,
                "vtp2_recovery_ms": l2,
                "observed_extra_vtp2_ms": l2 - l1,
                "pi_p_Rp_ms": path_component,
                "reset_cost_ms": reset_component,
                "computed_direct_extra_vtp2_ms": computed_extra,
                "computed_vtp2_ms": computed_vtp2,
                "model_residual_ms": l2 - computed_vtp2,
                "vtp1_penalized_delay_ms": d1p,
                "vtp2_penalized_delay_ms": d2p,
                "observed_penalized_extra_ms": d2p - d1p,
            }
        )
        records.append(out)

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)


def validate_instrumentation(
    rows: Sequence[Mapping[str, str]],
    fields_in_file: Sequence[str],
    design_fields: Sequence[str],
    strict: bool,
) -> None:
    errors: List[str] = []
    fields = set(fields_in_file)

    def nonzero_count(field: str, tolerance: float = 0.5) -> int:
        if field not in fields:
            return 0
        return sum(
            (not math.isfinite(as_float(row, field)))
            or abs(as_float(row, field)) > tolerance
            for row in rows
        )

    schema_v46 = any(as_float(row, "schema_version") >= 4.6 for row in rows)
    required_v46 = {
        "campaign_id", "configuration_hash", "outage_schedule_hash",
        "rng_seed", "rng_run", "route_cache_probe_mode",
        "bad_channel_split_delta", "no_route_split_delta",
        "original_state_balance_delta", "retransmission_state_balance_delta",
        "rreq_timer_balance_delta", "cache_graph_balance_delta",
        "policy_graph_balance_delta", "phy_retrans_attempts", "phy_late_attempts",
    }
    if schema_v46:
        missing = sorted(required_v46 - fields)
        if missing:
            errors.append("colonnes v4.6 absentes: " + ", ".join(missing))

    print("\nContrôles d'invariants")
    invariant_fields = (
        "conservation_delta",
        "rto_balance_delta",
        "bad_channel_split_delta",
        "no_route_split_delta",
        "original_state_balance_delta",
        "retransmission_state_balance_delta",
        "rreq_timer_balance_delta",
        "cache_graph_balance_delta",
        "policy_graph_balance_delta",
        "rreq_timer_match_misses",
        "discovery_completion_match_misses",
        "pending_end",
        "socket_errors",
    )
    for field in invariant_fields:
        if field not in fields:
            if schema_v46:
                errors.append(f"invariant absent: {field}")
            continue
        count = nonzero_count(field)
        print(f"  {field:<40}: {count}/{len(rows)}")
        if count:
            errors.append(f"{field} non nul/invalide sur {count} ligne(s)")

    trace_fields = (
        "dsr_trace_rreq",
        "dsr_trace_discovery_completed",
        "dsr_trace_discovery_failed",
        "dsr_trace_timer_armed",
        "dsr_trace_timeout",
    )
    for field in trace_fields:
        if field not in fields:
            if schema_v46:
                errors.append(f"trace absente du CSV: {field}")
            continue
        bad = sum(as_float(row, field) < 1 for row in rows)
        print(f"  {field:<40}: {bad} déconnexion(s)")
        if bad:
            errors.append(f"{field} déconnectée sur {bad} ligne(s)")

    split_specs = (
        ("attempts_without_route", "originals_without_route",
         "retransmissions_without_route", "séparation P(t)=0"),
        ("attempts_bad_channel", "originals_bad_channel",
         "retransmissions_bad_channel", "séparation C(t)=0"),
    )
    for total_field, original_field, retrans_field, label in split_specs:
        if {total_field, original_field, retrans_field}.issubset(fields):
            bad = sum(
                abs(as_float(row, total_field) - as_float(row, original_field)
                    - as_float(row, retrans_field)) > 0.5
                for row in rows
            )
            print(f"  {label:<40}: {bad}/{len(rows)}")
            if bad:
                errors.append(f"{label} incohérente sur {bad} ligne(s)")

    state_fields = {
        "tx_original_submissions", "tx_retrans_submissions",
        "original_c0_p0", "original_c0_p1", "original_c1_p0", "original_c1_p1",
        "retransmission_c0_p0", "retransmission_c0_p1",
        "retransmission_c1_p0", "retransmission_c1_p1",
    }
    if state_fields.issubset(fields):
        for label, total, cells in (
            ("matrice originales", "tx_original_submissions",
             ("original_c0_p0", "original_c0_p1", "original_c1_p0", "original_c1_p1")),
            ("matrice retransmissions", "tx_retrans_submissions",
             ("retransmission_c0_p0", "retransmission_c0_p1",
              "retransmission_c1_p0", "retransmission_c1_p1")),
        ):
            bad = sum(
                abs(as_float(row, total) - sum(as_float(row, cell) for cell in cells)) > 0.5
                for row in rows
            )
            print(f"  {label:<40}: {bad}/{len(rows)}")
            if bad:
                errors.append(f"{label} incohérente sur {bad} ligne(s)")

    # Appariement expérimental strict : un triplet complet et mêmes métadonnées
    # de phase/calendrier pour chaque configuration et chaque graine.
    grouped: Dict[Tuple[Tuple[str, ...], int], List[Mapping[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(design_key(row, design_fields), as_int(row, "run"))].append(row)

    incomplete = phase_mismatch = schedule_mismatch = seed_mismatch = 0
    for (_, _run), group in grouped.items():
        policies = [row.get("policy", "") for row in group]
        if sorted(policies) != ["TRAD", "VTP1", "VTP2"]:
            incomplete += 1
        if "outage_phase_s" in fields and len({row.get("outage_phase_s", "") for row in group}) != 1:
            phase_mismatch += 1
        if "outage_schedule_hash" in fields and len({row.get("outage_schedule_hash", "") for row in group}) != 1:
            schedule_mismatch += 1
        if "rng_run" in fields and len({row.get("rng_run", "") for row in group}) != 1:
            seed_mismatch += 1

    print(f"  triplets incomplets/dupliqués{'':<16}: {incomplete}/{len(grouped)}")
    print(f"  phase outage non appariée{'':<17}: {phase_mismatch}/{len(grouped)}")
    print(f"  calendrier non apparié{'':<20}: {schedule_mismatch}/{len(grouped)}")
    print(f"  rng_run non apparié{'':<24}: {seed_mismatch}/{len(grouped)}")
    if incomplete:
        errors.append(f"{incomplete} triplet(s) TRAD/VTP1/VTP2 incomplet(s) ou dupliqué(s)")
    if phase_mismatch:
        errors.append(f"{phase_mismatch} phase(s) non appariée(s)")
    if schedule_mismatch:
        errors.append(f"{schedule_mismatch} calendrier(s) non apparié(s)")
    if seed_mismatch:
        errors.append(f"{seed_mismatch} rng_run non apparié(s)")

    if errors:
        message = "\nÉCHEC DE VALIDATION:\n- " + "\n- ".join(errors)
        if strict:
            raise SystemExit(message)
        print("AVERTISSEMENT (mode non strict):" + message)
    else:
        print("  validation stricte                      : OK")


def parse_crisis(text: str) -> CrisisSpec:
    # Format: nom|métrique|lt|seuil
    parts = text.split("|")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "Une crise doit être écrite nom|metrique|operateur|seuil, "
            "par exemple pdr95|pdr_pct|lt|95"
        )
    name, metric, operator, threshold = parts
    if operator not in {"lt", "le", "gt", "ge"}:
        raise argparse.ArgumentTypeError("Opérateur attendu: lt, le, gt ou ge")
    try:
        threshold_value = float(threshold)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Seuil de crise invalide") from exc
    return CrisisSpec(name, metric, operator, threshold_value)


def parse_metric_list(text: str) -> Tuple[str, ...]:
    metrics = tuple(item.strip() for item in text.split(",") if item.strip())
    if not metrics:
        raise argparse.ArgumentTypeError("La liste de métriques est vide.")
    return metrics


def write_markdown_report(
    records: Sequence[Mapping[str, object]],
    output: Path,
    design_fields: Sequence[str],
) -> None:
    primary = [record for record in records if int(record["primary_family"]) == 1]
    with output.open("w", encoding="utf-8") as handle:
        handle.write("# Résultats appariés VTP/DSR\n\n")
        handle.write(
            "Les différences sont calculées comme première politique moins seconde politique. "
            "Pour les métriques de coût, une différence négative favorise la première politique.\n\n"
        )
        for record in primary:
            design = ", ".join(f"{field}={record[field]}" for field in design_fields)
            handle.write(
                f"- **{design}; {record['comparison']}; {record['metric']}**: "
                f"n={record['n_pairs']}, médiane={float(record['median_difference']):.6g}, "
                f"H-L={float(record['hodges_lehmann']):.6g} "
                f"[{float(record['hl_bootstrap_ci_low']):.6g}, "
                f"{float(record['hl_bootstrap_ci_high']):.6g}], "
                f"p exact={float(record['wilcoxon_exact_p']):.6g}, "
                f"p Holm={record['holm_adjusted_p']}.\n"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="vtp-dsr-raw.csv", help="CSV brut produit par ns-3")
    parser.add_argument("--output-dir", default="vtp-analysis", help="Répertoire de sortie")
    parser.add_argument("--scenario", action="append", default=[], help="Filtre de scénario, répétable")
    parser.add_argument("--speed", action="append", type=float, default=[], help="Filtre de vitesse, répétable")
    parser.add_argument(
        "--metrics",
        type=parse_metric_list,
        default=DEFAULT_PAIRED_METRICS,
        help="Métriques appariées séparées par des virgules",
    )
    parser.add_argument(
        "--primary-metrics",
        type=parse_metric_list,
        default=DEFAULT_PRIMARY_METRICS,
        help="Famille Holm séparée par des virgules",
    )
    parser.add_argument("--bootstrap-reps", type=int, default=10000)
    parser.add_argument("--bootstrap-seed", type=int, default=12345)
    parser.add_argument(
        "--holm-scope", choices=("configuration", "contrast"),
        default="configuration",
        help="Famille de Holm: configuration=12 tests primaires, contrast=4 tests par contraste",
    )
    parser.add_argument(
        "--no-strict", action="store_true",
        help="Autorise l'analyse exploratoire malgré des invariants violés ou des triplets incomplets",
    )
    parser.add_argument(
        "--crisis",
        action="append",
        type=parse_crisis,
        default=[],
        help="Critère nom|metrique|operateur|seuil ; opérateur parmi lt, le, gt, ge",
    )
    parser.add_argument(
        "--no-default-crises", action="store_true", help="N'ajoute pas les cinq crises par défaut"
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows, header, repaired = load_rows(input_path)
    rows = filter_rows(rows, args.scenario, args.speed)
    design_fields = active_design_fields(header)

    metrics = tuple(metric for metric in args.metrics if metric in header)
    missing_metrics = [metric for metric in args.metrics if metric not in header]
    if missing_metrics:
        print("AVERTISSEMENT: métriques absentes du CSV: " + ", ".join(missing_metrics))
    primary_metrics = tuple(metric for metric in args.primary_metrics if metric in metrics)

    crises = list(args.crisis)
    if not args.no_default_crises:
        crises = list(DEFAULT_CRISES) + crises
    crises = [crisis for crisis in crises if crisis.metric in header]

    validate_instrumentation(rows, header, design_fields, strict=not args.no_strict)
    indexed, runs = build_index(rows, design_fields)

    summary_path = output_dir / "vtp-dsr-summary.csv"
    paired_values_path = output_dir / "vtp-dsr-paired-values.csv"
    paired_stats_path = output_dir / "vtp-dsr-paired-statistics.csv"
    mcnemar_path = output_dir / "vtp-dsr-mcnemar.csv"
    model_path = output_dir / "vtp-dsr-model-paired.csv"
    markdown_path = output_dir / "vtp-dsr-statistical-report.md"

    write_summary(rows, summary_path, header, design_fields, crises)
    write_paired_values(indexed, runs, paired_values_path, metrics, design_fields)
    records = write_paired_statistics(
        indexed,
        runs,
        paired_stats_path,
        metrics,
        primary_metrics,
        design_fields,
        args.bootstrap_reps,
        args.bootstrap_seed,
        args.holm_scope,
    )
    write_mcnemar(indexed, runs, mcnemar_path, crises, design_fields)
    write_paired_model(indexed, runs, model_path, design_fields)
    write_markdown_report(records, markdown_path, design_fields)

    if repaired:
        print(
            f"\nRéparation automatique v4.4: {repaired} ligne(s) réalignée(s) "
            "après suppression du TR_samples dupliqué."
        )

    print("\nFichiers produits")
    for path in (
        summary_path,
        paired_values_path,
        paired_stats_path,
        mcnemar_path,
        model_path,
        markdown_path,
    ):
        print(f"  {path}")


if __name__ == "__main__":
    main()
