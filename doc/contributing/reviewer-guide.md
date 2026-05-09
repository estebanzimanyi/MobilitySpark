<!--
Copyright(c) MobilityDB Contributors

This documentation is licensed under a
Creative Commons Attribution-Share Alike 3.0 License
https://creativecommons.org/licenses/by-sa/3.0/
-->

# MobilitySpark PR Reviewer Guide

Quick reference for anyone reviewing open pull requests in **MobilitySpark** and its JMEOS dependency.
Updated in the same commit as any PR that changes PR state or adds new branches.
**Last updated: 2026-05-09 — 5 open PRs across MobilityDB/MobilitySpark + MobilityDB/JMEOS (PRs #8, #9, #11, #12).**

---

## How to find this guide

- **In the repo:** `doc/contributing/reviewer-guide.md`
- **Rule:** every commit that opens, closes, or restructures a PR must update this file in the same commit. A one-liner status change is enough; a fuller rewrite is needed when the dependency graph changes.

---

## CI legend

| Symbol | Meaning |
|--------|---------|
| ✅ | All checks green |
| ❌ | Real failure — needs investigation before review |
| ⏳ | CI running |
| ❓ | No CI result yet |
| ⚠️ | macOS/Windows non-blocking (`continue-on-error: true`) |

---

## Dependency chain — land in this order

```
MobilityDB/JMEOS
  PR #9  JashanReel:fix-tests-using-docker    (multi-module Maven layout; needs cleanup review)
    └─► PR #12  estebanzimanyi:fix/multimodule-with-split-interface  (split JNR-FFI → ARM64/macOS fix)
          └─► MobilityDB/MobilitySpark
                PR #7   fix/license-main-java  (CI bootstrap — stacks on JMEOS above)
                  └─► PR #5  feat/jmeos-1.3-berlinmod-poc  (JMEOS 1.3 + BerlinMOD)
```

**PR #11** (`estebanzimanyi:fix/split-meos-library-interface`) targeted the flat `src/` structure
and is now superseded by `fix/multimodule-with-split-interface` (targets `jmeos-core/`). PR #11
can be closed once the new integration branch is opened as a PR on MobilityDB/JMEOS.

**PR #8** (`SachaDelsaux:JMEOS_v1.3`) is subsumed by PR #9 — recommended for closure (comment posted).

**PR #6** (`ci/bootstrap-dev`) is superseded by PR #7 for CI purposes; close when #7 merges.
**PR #2** (`doc/jmeos-1.3-bump-plan`) is a standalone tracking doc; can merge any time.

---

## MobilityDB/JMEOS — open PRs

| PR | Branch | Description | CI | Notes |
|----|--------|-------------|----|-------|
| #9 | `JashanReel:fix-tests-using-docker` | Multi-module Maven layout (`jmeos-core/`); MEOS 1.3 API; test fixes | ❓ | **Land first** — needs cleanup (IDE files, binary blobs, *.class, squash 78 commits) |
| #11 | `estebanzimanyi:fix/split-meos-library-interface` | Split for flat `src/` structure — superseded by `fix/multimodule-with-split-interface` | ✅ | Close after new PR opens |
| #8 | `SachaDelsaux:JMEOS_v1.3` | JMEOS v1.3 upgrade — subsumed by #9 | ❓ | Recommended for closure (comment posted) |

| #12 | `estebanzimanyi:fix/multimodule-with-split-interface` | Split `MeosLibrary` 1685-method interface into 4 `public static` sub-interfaces for JNR-FFI; removes binary blobs + `.class` + debug files; updates `.gitignore` | Stacked on PR #9; land after #9 is cleaned up |

---

## MobilityDB/MobilitySpark — open PRs

| PR | Branch | Description | CI | Notes |
|----|--------|-------------|----|-------|
| #7 | `estebanzimanyi:fix/license-main-java` | Multi-platform CI bootstrap: license check, compile, 51 unit tests on Linux; macOS/Windows non-blocking | ✅⚠️ | **Land second** (after JMEOS #11) |
| #5 | `estebanzimanyi:feat/jmeos-1.3-berlinmod-poc` | JMEOS 1.3 + BerlinMOD Q1–Q17 + TemporalParquet edge-to-cloud pipeline; 37/37 tests | ✅ | **Land last** — stacks on #7 |
| #6 | `estebanzimanyi:ci/bootstrap-dev` | Original CI workflow file | ✅ | Close when #7 merges (superseded) |
| #2 | `doc/jmeos-1.3-bump-plan` | JMEOS 1.3 upgrade tracking doc | ❓ | Standalone — merge any time |

---

## estebanzimanyi/MobilitySpark fork — staging PRs

These PRs exist on the fork and are awaiting upstream review after the above chain merges.

| PR | Branch | Description | CI | Notes |
|----|--------|-------------|----|-------|
| #1 | `feat/udf-parity-phase2` | Expand UDF surface: 141 new UDFs in 7 classes + JMEOS-1.5 sub-interface fix; 57/57 tests | ✅ | Depends on JMEOS #11 being merged upstream |

### UDF breakdown for fork PR #1

| Class | UDFs | Summary |
|-------|------|---------|
| `ConstructorUDFs` | 18 | Text-literal constructors for temporal/span/set/box types |
| `AccessorUDFs` | 25 | start/end/min/max value, shift, scale, atSpan, atSpanset |
| `SpanAlgebraUDFs` | 23 | Span+set topology predicates + algebraic operations |
| `AnalyticsUDFs` | 12 | tfloat math, tnumber integral/twavg, tpoint length/speed/azimuth/direction |
| `PredicateUDFs` | 36 | Temporal order comparisons + ever/always lifting |
| `OutputUDFs` | 15 | Text serialisation, metadata, instant/sequence navigation |
| `TemporalLiftingUDFs` | 30 | Lifted comparisons (tbool), arithmetic, deltaValue, tprecision, tsample |

Total: 40 pre-existing + 141 new = **181 UDFs**.

---

## Review checklist

For every MobilitySpark / JMEOS PR, verify:

- [ ] PostgreSQL License header on every `.java` file
- [ ] `meos_initialize()` in `@BeforeAll`; **no** `meos_finalize()` in `@AfterAll`
- [ ] Surefire `forkCount=1 reuseForks=false` preserved in `pom.xml`
- [ ] New UDF: `null` input → `null` output (STRICT semantics)
- [ ] New UDF: `DBL_MAX` MEOS return → `null` Java return (NAD sentinel)
- [ ] No large binary data files (> 10 MB) in the commit
- [ ] CI green on Linux before requesting merge
