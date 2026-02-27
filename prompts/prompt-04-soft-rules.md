# AdmitGuard — Soft Rules (Exception Triggers)

Soft rules send a candidate to the **Exception sheet** for manual review instead of outright rejecting them. These are borderline cases within a configurable buffer of the cutoff.

---

## Rules

| Field | Condition | Result |
|---|---|---|
| Total Percentage | Below `Min Percentage` but within `Exception Buffer PCT` (default 1%) | ⚠️ Exception |
| CGPA | Below `Min CGPA` but within `Exception Buffer CGPA` (default 0.1) | ⚠️ Exception |

---

## Examples (with default thresholds: Min % = 60, Min CGPA = 6.0)

| Candidate | Value | Outcome |
|---|---|---|
| Percentage = 59.5% | Within 1% of 60% | ⚠️ Exception |
| Percentage = 58.9% | More than 1% below 60% | ❌ Rejected |
| CGPA = 5.95 | Within 0.1 of 6.0 | ⚠️ Exception |
| CGPA = 5.85 | More than 0.1 below 6.0 | ❌ Rejected |

---

## Config Sheet Parameters

```
Exception Buffer PCT    →  1.0   (percentage points)
Exception Buffer CGPA   →  0.1   (CGPA points)
```

Change these values in the Config sheet — no code change needed.

---

## What Happens Next

1. Reviewer opens Exception sheet
2. Sets **Status** column to `Approved` or `Rejected`
3. Adds a **Reviewer Remark**
4. Re-runs the script → Approved rows move to Clean Data
