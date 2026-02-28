#!/usr/bin/env python3
"""
WarpSchema — Local ROI Assessment CLI (warp-check)
====================================================
Analyse a JSON file of event payloads against a canonical schema, detect
schema drift using deterministic heuristics (Levenshtein distance on keys,
type checking), and produce a professional summary report.

This tool runs **entirely offline** — no Docker or network access required.

Requirements (stdlib only):
    Python ≥ 3.10

Usage:
    python warp-check.py sample_payloads.json
    python warp-check.py data.json --cost-per-minute 95.0 --mttr 12.0
    python warp-check.py data.json --output json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ============================================================================
# Canonical schema definition
# ============================================================================
# Each entry maps a required key name to its expected Python type(s).
CANONICAL_SCHEMA: dict[str, tuple[type, ...]] = {
    "event_id": (str,),
    "timestamp": (int, float),
    "user_id": (int,),
    "action": (str,),
    "metadata": (dict,),
}

# Maximum Levenshtein distance to consider a misspelled key "healable"
MAX_HEAL_DISTANCE = 2


# ============================================================================
# Levenshtein distance (stdlib, no external deps)
# ============================================================================
def levenshtein(a: str, b: str) -> int:
    """Compute the Levenshtein edit-distance between two strings."""
    if len(a) < len(b):
        return levenshtein(b, a)
    if not b:
        return len(a)

    prev_row = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr_row = [i + 1]
        for j, cb in enumerate(b):
            cost = 0 if ca == cb else 1
            curr_row.append(min(
                curr_row[j] + 1,          # insert
                prev_row[j + 1] + 1,      # delete
                prev_row[j] + cost,       # substitute
            ))
        prev_row = curr_row
    return prev_row[-1]


# ============================================================================
# Data structures
# ============================================================================
@dataclass
class DriftIssue:
    """A single detected schema deviation."""
    event_id: str
    issue_type: str          # missing_key | misspelled_key | wrong_type | extra_key
    field: str
    detail: str
    healable: bool
    confidence: float        # 0.0 – 1.0


@dataclass
class AnalysisResult:
    """Aggregated result for all payloads."""
    total_payloads: int = 0
    clean_payloads: int = 0
    drifted_payloads: int = 0
    issues: list[DriftIssue] = field(default_factory=list)
    healable_count: int = 0
    total_confidence: float = 0.0


# ============================================================================
# Analysis engine
# ============================================================================
def _best_match(key: str, candidates: set[str]) -> tuple[str, int] | None:
    """Find the closest canonical key within MAX_HEAL_DISTANCE."""
    best: tuple[str, int] | None = None
    for c in candidates:
        d = levenshtein(key, c)
        if d <= MAX_HEAL_DISTANCE and (best is None or d < best[1]):
            best = (c, d)
    return best


def analyse_payload(payload: dict[str, Any], index: int) -> list[DriftIssue]:
    """Check one payload against the canonical schema."""
    issues: list[DriftIssue] = []
    eid = payload.get("event_id", f"<index-{index}>")
    present_keys = set(payload.keys())
    canonical_keys = set(CANONICAL_SCHEMA.keys())

    # -- missing / misspelled keys ------------------------------------------
    missing = canonical_keys - present_keys
    extra = present_keys - canonical_keys

    for mkey in missing:
        # Is there a close match among the extra keys?
        match = _best_match(mkey, extra)
        if match:
            matched_key, dist = match
            confidence = 1.0 - (dist / (len(mkey) + 1))
            issues.append(DriftIssue(
                event_id=eid,
                issue_type="misspelled_key",
                field=mkey,
                detail=f"Likely misspelling: '{matched_key}' → '{mkey}' (edit distance {dist})",
                healable=True,
                confidence=round(confidence, 3),
            ))
            extra.discard(matched_key)  # consumed
        else:
            issues.append(DriftIssue(
                event_id=eid,
                issue_type="missing_key",
                field=mkey,
                detail=f"Required key '{mkey}' is absent with no close match",
                healable=False,
                confidence=0.0,
            ))

    # -- extra (unexpected) keys --------------------------------------------
    for ekey in extra:
        issues.append(DriftIssue(
            event_id=eid,
            issue_type="extra_key",
            field=ekey,
            detail=f"Unexpected key '{ekey}' not in canonical schema",
            healable=True,
            confidence=0.85,
        ))

    # -- type mismatches on present canonical keys --------------------------
    for ckey, expected_types in CANONICAL_SCHEMA.items():
        if ckey in payload:
            val = payload[ckey]
            if not isinstance(val, expected_types):
                expected_names = "/".join(t.__name__ for t in expected_types)
                issues.append(DriftIssue(
                    event_id=eid,
                    issue_type="wrong_type",
                    field=ckey,
                    detail=f"Expected {expected_names}, got {type(val).__name__}: {val!r}",
                    healable=True,
                    confidence=0.75,
                ))

    return issues


def run_analysis(payloads: list[dict[str, Any]]) -> AnalysisResult:
    """Run the full analysis across all payloads."""
    result = AnalysisResult(total_payloads=len(payloads))

    for idx, payload in enumerate(payloads):
        issues = analyse_payload(payload, idx)
        if issues:
            result.drifted_payloads += 1
            result.issues.extend(issues)
        else:
            result.clean_payloads += 1

    result.healable_count = sum(1 for i in result.issues if i.healable)
    result.total_confidence = sum(i.confidence for i in result.issues if i.healable)
    return result


# ============================================================================
# Reporting
# ============================================================================
DIVIDER = "═" * 72


def print_text_report(
    result: AnalysisResult,
    cost_per_minute: float,
    mttr_minutes: float,
) -> None:
    """Render a human-readable CLI report."""
    healable_pct = (
        (result.healable_count / len(result.issues) * 100) if result.issues else 100.0
    )
    avg_confidence = (
        (result.total_confidence / result.healable_count) if result.healable_count else 0.0
    )
    # Downtime prevented = number of drifted payloads that would have caused
    # an incident × user-supplied MTTR × cost-per-minute.
    incidents_prevented = result.drifted_payloads if result.healable_count else 0
    downtime_minutes = incidents_prevented * mttr_minutes
    cost_saved = downtime_minutes * cost_per_minute

    print()
    print(DIVIDER)
    print("  WarpSchema — Schema Drift Analysis Report")
    print(DIVIDER)
    print()
    print(f"  Payloads analysed        : {result.total_payloads}")
    print(f"  Clean payloads           : {result.clean_payloads}")
    print(f"  Payloads with drift      : {result.drifted_payloads}")
    print(f"  Total issues detected    : {len(result.issues)}")
    print()
    print(DIVIDER)
    print("  Heal-ability Summary")
    print(DIVIDER)
    print(f"  Healable issues          : {result.healable_count} / {len(result.issues)}")
    print(f"  % Healed                 : {healable_pct:.1f}%")
    print(f"  Avg Confidence Score     : {avg_confidence:.3f}")
    print()
    print(DIVIDER)
    print("  Estimated Downtime Prevention")
    print(DIVIDER)
    print(f"  MTTR (user-supplied)     : {mttr_minutes:.1f} min")
    print(f"  Cost per minute          : ${cost_per_minute:,.2f}")
    print(f"  Incidents prevented      : {incidents_prevented}")
    print(f"  Downtime prevented       : {downtime_minutes:.1f} min")
    print(f"  Estimated cost saved     : ${cost_saved:,.2f}")
    print()

    if result.issues:
        print(DIVIDER)
        print("  Issue Details")
        print(DIVIDER)
        for issue in result.issues:
            tag = "✓ healable" if issue.healable else "✗ manual"
            print(f"  [{tag}] {issue.event_id} | {issue.issue_type:16s} | "
                  f"{issue.field:14s} | {issue.detail}")
        print()

    print(DIVIDER)
    print("  Analysis method: deterministic (Levenshtein key matching + type check)")
    print("  For ML-powered healing, run the full gateway: docker compose up")
    print(DIVIDER)
    print()


def print_json_report(
    result: AnalysisResult,
    cost_per_minute: float,
    mttr_minutes: float,
) -> None:
    """Render the report as machine-readable JSON."""
    healable_pct = (
        (result.healable_count / len(result.issues) * 100) if result.issues else 100.0
    )
    avg_confidence = (
        (result.total_confidence / result.healable_count) if result.healable_count else 0.0
    )
    incidents_prevented = result.drifted_payloads if result.healable_count else 0
    downtime_minutes = incidents_prevented * mttr_minutes
    cost_saved = downtime_minutes * cost_per_minute

    report = {
        "total_payloads": result.total_payloads,
        "clean_payloads": result.clean_payloads,
        "drifted_payloads": result.drifted_payloads,
        "total_issues": len(result.issues),
        "healable_issues": result.healable_count,
        "healed_pct": round(healable_pct, 2),
        "avg_confidence": round(avg_confidence, 4),
        "mttr_minutes": mttr_minutes,
        "cost_per_minute": cost_per_minute,
        "incidents_prevented": incidents_prevented,
        "downtime_prevented_minutes": round(downtime_minutes, 2),
        "estimated_cost_saved": round(cost_saved, 2),
        "issues": [
            {
                "event_id": i.event_id,
                "type": i.issue_type,
                "field": i.field,
                "detail": i.detail,
                "healable": i.healable,
                "confidence": i.confidence,
            }
            for i in result.issues
        ],
    }
    print(json.dumps(report, indent=2))


# ============================================================================
# CLI entry-point
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="WarpSchema local schema-drift analyser and ROI estimator",
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to a JSON file containing an array of event payloads",
    )
    parser.add_argument(
        "--cost-per-minute",
        type=float,
        default=50.0,
        help="Estimated cost of pipeline downtime per minute in USD (default: 50)",
    )
    parser.add_argument(
        "--mttr",
        type=float,
        default=15.0,
        help="Mean Time To Resolve a schema-drift incident in minutes (default: 15)",
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Report format (default: text)",
    )
    args = parser.parse_args()

    # -- Load payloads ------------------------------------------------------
    if not args.input_file.exists():
        print(f"Error: file not found — {args.input_file}", file=sys.stderr)
        sys.exit(1)

    try:
        payloads = json.loads(args.input_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Error: invalid JSON — {exc}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(payloads, list):
        print("Error: expected a JSON array of payload objects.", file=sys.stderr)
        sys.exit(1)

    # -- Analyse & report ---------------------------------------------------
    result = run_analysis(payloads)

    if args.output == "json":
        print_json_report(result, args.cost_per_minute, args.mttr)
    else:
        print_text_report(result, args.cost_per_minute, args.mttr)


if __name__ == "__main__":
    main()
