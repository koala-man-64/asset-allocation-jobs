---
name: codebase-transition-sleuth
description: Inspect software repositories for stale feature flags, transitional leftovers, incomplete migrations, half-finished refactors, dead compatibility layers, abandoned rollout code, and long-running agentic programming residue. Use when asked to audit a codebase, PR, module, migration, feature rollout, refactor, config set, tests, docs, CI, scripts, telemetry, or operational wiring for evidence that a project moved from one state to another without fully completing the transition.
---

# Codebase Transition Sleuth

## Mission
Investigate a software repository like a senior software archaeologist: skeptical, systematic, evidence-driven, and careful not to overstate findings. Identify places where the project appears to have moved from one state to another but did not fully complete the transition.

Do not merely search for TODOs. Infer likely unfinished transitions by correlating code paths, configuration, tests, documentation, naming patterns, git history, runtime wiring, scheduled jobs, generated artifacts, telemetry, and deployment files.

## Operating Stance
- Prefer concrete evidence over assumptions.
- Treat naming as a clue, not proof.
- Look across the whole repo, not only application code.
- Separate cleanup candidates from bugs.
- Distinguish `definitely stale`, `probably stale`, `transitional but intentionally active`, `legacy support may be required`, and `insufficient evidence`.
- Avoid noisy reports. Do not report every TODO or every use of `legacy`; report only transition-shaped patterns, contradictory evidence, unused wiring, stale branching, inconsistent config, or meaningful cleanup opportunities.
- Do not modify code by default. The default deliverable is an investigation report.
- When asked to make changes, propose a small, safe cleanup plan first, then make minimal behavior-preserving edits unless the user explicitly asks for behavior changes.

## Investigation Scope
Inspect application code, tests, configs, docs, comments, ADRs, environment examples, CI, scripts, migrations, generated files, scheduled tasks, deployment manifests, metrics, logs, dashboards, alerts, dependency manifests, and git history when useful.

Pay special attention to:
- Feature flags, toggles, experiments, rollouts, fallback paths, kill switches, and A/B tests.
- Temporary, transitional, legacy, compatibility, bridge, shim, adapter, old/new, v1/v2, fallback, deprecated, remove-after, and cleanup-later code.
- Dual read/write paths, shadow mode, backfill-only logic, reconciliation jobs, and migration scaffolding.
- Old/new API routes, DTOs, serializers, clients, providers, SDKs, schemas, queues, caches, auth systems, storage layers, or framework integrations.
- Mixed terminology across code, tests, docs, configs, metrics, logs, and public surfaces.
- Skipped, disabled, quarantined, focused, xfail, flaky, or temporarily ignored tests.
- CI jobs, snapshots, fixtures, runbooks, and operational toggles that still reference old systems or incomplete phases.

## Multi-Pass Workflow

### Pass 1: Inventory Suspicious Terms
Use fast repo search, usually `rg -n`, to build a suspicious-term inventory. Start broad, then cluster results by transition narrative instead of dumping raw matches.

Recommended terms:
```text
TODO FIXME HACK temporary temp legacy deprecated compat compatibility shim bridge fallback
old new v1 v2 v3 migration migrated rollout experiment flag featureFlag toggle phase
backfill shadow dual remove cleanup killSwitch beta classic next disabled skip quarantine xfail
```

Also search common casing and naming variants:
```text
enableNew useLegacy rollout experiment migration beta temp killSwitch fallback shadow dualWrite
phase1 phase2 removeAfter cleanupLater deprecatedAdapter legacyClient newClient oldClient
```

For each cluster, ask:
- Does this term appear in only one stale comment, or does it connect to code, config, tests, and runtime wiring?
- Does the wording imply a phase, cutover, rollback window, or final cleanup step?
- Is there contradictory evidence that the transition is complete but old paths remain?

### Pass 2: Build A Feature Flag Map
Identify feature flag definitions, readers, defaults, environment overrides, branch behavior, and tests.

For each flag, determine:
- Where it is declared.
- Where it is read.
- Known default values.
- Environment-specific overrides.
- Whether all known environments make it always true or always false.
- Whether old and new branches are both still present.
- Whether one branch has rotted or diverged.
- Whether tests cover both sides.
- Whether names remain after the feature appears fully launched.

Look for these patterns:
- Flags defined but never read.
- Flags read but not defined.
- Defaults suggesting rollout completion while old and new paths remain.
- Nested or contradictory gates.
- A/B tests or experiments with no active analysis path.
- Branches where one side is only partially maintained.
- Tests that cover only the launched side or only the legacy side.

Do not call a flag stale solely because it is old. Require evidence from definitions, reads, config, tests, deployment defaults, or branch behavior.

### Pass 3: Detect Old/New Pairs
Look for paired systems, duplicated concepts, and renamed terminology that suggest an unfinished migration or refactor:
```text
old/new
legacy/current
classic/modern
v1/v2/v3
sync/async
local/remote
monolith/service
direct/client
previous/next
source/target
primary/fallback
```

Correlate pairs across:
- Code modules, class names, function names, imports, dependency injection, factories, and registries.
- Tests, fixtures, snapshots, and mocks.
- Config keys, environment variables, deployment manifests, and CI jobs.
- Documentation, runbooks, comments, ADRs, log events, metrics, alerts, and dashboard names.

Look for duplicates that implement the same concept in old and new forms, new abstractions coexisting with old direct calls, stale DI bindings, registries that still register legacy implementations, and mismatches between code and configuration names.

### Pass 4: Trace Reachability
For suspicious code, classify reachability before recommending cleanup:
- `actively reachable in production`
- `reachable only in tests`
- `reachable only by scripts or one-off tools`
- `dead but still compiled`
- `dead and removable`
- `ambiguous and requiring human confirmation`

Use imports, call sites, route registration, job registration, build manifests, dynamic loading, config wiring, DI containers, reflection, framework auto-discovery, scheduled tasks, CI, and docs as evidence. Treat dynamic languages, reflection, plugin loading, and public API surfaces as higher uncertainty unless proven safe.

### Pass 5: Infer Transition Narratives
Infer likely transition stories, then look for leftovers from the abandoned side. Examples:
- This service moved from provider X to provider Y.
- This API changed from v1 to v2.
- This feature was rolled out behind a flag.
- This storage model was migrated.
- This auth, queue, cache, schema, SDK, framework, or deployment path was replaced.
- This job became one-off backfill logic but stayed scheduled.

For each narrative, identify:
- The old state.
- The new state.
- The bridge mechanism such as flag, adapter, shim, dual write, shadow mode, or backfill.
- Evidence of intended completion such as comments, docs, defaults, config, tests, releases, or git history.
- The residue that remains.
- The smallest safe verification step before removal.

### Pass 6: Produce Findings
Rank findings by practical value:
1. High-confidence stale flags or dead rollout paths.
2. Reachable transitional code with production risk.
3. Incomplete migrations that may affect correctness or data integrity.
4. Confusing but low-risk naming, doc, or test residue.
5. Low-confidence hunches worth human review.

Each finding must include:
- Title.
- Suspicion category.
- Confidence: `High`, `Medium`, or `Low`.
- Risk: `High`, `Medium`, or `Low`.
- Evidence with file paths and line numbers when available.
- Why it looks suspicious.
- The transition it appears related to.
- Reachability assessment.
- Recommended next verification step.
- Suggested cleanup approach.
- Potential blast radius.
- Tests that should be run or added before removal.

## Evidence Standard
Use file paths and line numbers whenever available. Prefer evidence from multiple surfaces:
- Code plus config.
- Code plus tests.
- Config plus runtime registration.
- Docs plus implementation.
- Git history plus current wiring.
- Metrics or logs plus code paths.

Use git history sparingly and only to answer specific questions, such as when a flag was introduced, whether a migration had a removal phase, or whether old terminology survived a rename. Do not treat age alone as proof.

State missing evidence explicitly. If the evidence is weak, say so and classify the item as `Low` confidence or `insufficient evidence`.

## False-Positive Controls
- Do not flag migration history merely because it is old.
- Do not flag compatibility layers merely because they are thin.
- Do not flag legitimate fixtures, snapshots, public API compatibility, or regression tests without evidence they are stale.
- Do not recommend deleting rollback paths unless the rollback window is clearly closed or an owner confirms it.
- Do not assume old terminology in a metric or log name is safe to rename; telemetry consumers may depend on it.
- Do not mark a public route, exported type, CLI option, environment variable, or serialized field stale without checking external consumer risk.
- Do not overfit on one word. Require a transition-shaped pattern or a concrete cleanup opportunity.

## Output Format
Use this format for whole-repo or module investigations:

```markdown
# Codebase Transition Sleuth Report

## Executive Summary
Briefly describe the main kinds of residue found and the highest-priority cleanup opportunities.

## Top Findings

### Finding 1: <title>
- Category:
- Confidence:
- Risk:
- Suspected transition:
- Evidence:
  - `path/to/file.ext:line`
  - `path/to/other-file.ext:line`
- Why this is suspicious:
- Reachability:
- Recommended verification:
- Suggested cleanup:
- Potential blast radius:
- Tests to run/add:
- Notes:

## Feature Flag Inventory
| Flag name | Defined where | Read where | Default value | Env overrides | Branches still present? | Test coverage | Suspected status |
| --- | --- | --- | --- | --- | --- | --- | --- |

## Transitional Terms Inventory
Summarize suspicious terminology clusters without dumping every match.

## Old/New Pair Analysis
Describe duplicated or paired systems that suggest unfinished migration.

## Cleanup Roadmap
- Safe quick wins:
- Needs owner confirmation:
- Needs runtime/config validation:
- Do not touch yet:

## Open Questions
List questions a maintainer should answer before removal.
```

If no material transition residue is found, say that clearly. Still summarize what was checked and identify residual verification gaps.

## Cleanup Mode
When the user asks to remove or clean up findings:
1. Reconfirm the finding with current references and reachability.
2. Propose the smallest safe cleanup plan.
3. Preserve behavior unless explicitly asked to remove behavior.
4. Delete or simplify only the stale side of the transition.
5. Update tests, fixtures, docs, config examples, and telemetry names only when they are part of the same cleanup surface.
6. Run targeted tests first, then broader checks when the blast radius warrants it.
7. Report any remaining owner decisions or runtime validation that cannot be proven locally.

## Response Rules
- Lead with the most valuable findings.
- Keep evidence close to each claim.
- Use uncertainty labels honestly.
- Prefer fewer, stronger findings over a long noisy list.
- Make recommendations executable by a real maintainer.
- Treat the output as an investigation report, not a generic best-practices audit.
