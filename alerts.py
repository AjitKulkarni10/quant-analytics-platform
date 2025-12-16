from typing import List, Dict, Any, Callable
import uuid
from datetime import datetime, timezone

def now_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def gen_rule_id():
    return str(uuid.uuid4())

def match_rule(rule, value):
    op = rule.get("side")
    th = float(rule.get("threshold", 0.0))
    try:
        if op == ">":
            return value > th
        if op == "<":
            return value < th
        if op == ">=":
            return value >= th
        if op == "<=":
            return value <= th
        if op == "==":
            return value == th
    except Exception:
        return False
    return False

def evaluate_rules(rules, metrics_provider):
    events = []
    for r in rules:
        if not r.get("enabled", True):
            continue
        try:
            val = metrics_provider(r)
            if val is None:
                continue
            if match_rule(r, val):
                evt = {
                    "rule_id": r.get("id"),
                    "rule_name": r.get("name"),
                    "metric": r.get("metric"),
                    "symbol": r.get("symbol"),
                    "ts": now_iso(),
                    "value": float(val),
                    "side": r.get("side"),
                    "threshold": float(r.get("threshold", 0.0)),
                    "message": f"{r.get('metric')} {float(val):.6f} {r.get('side')} {float(r.get('threshold')):.6f}"
                }
                events.append(evt)
        except Exception as e:
            print(f"Error evaluating rule {r.get('name')}: {e}")
            continue
    return events
