---
name: git-hygiene-orchestrator
description: Audit and clean Git branches, remote-tracking refs, and worktrees safely. Use when Codex is asked to inspect Git hygiene, stale branches, merged branches, remote refs, worktrees, dirty work, merge conflicts, branch cleanup, pruning, or repository cleanup commands.
---

# Git Hygiene Orchestrator

## Mission

Audit Git branches, remote-tracking refs, and worktrees before recommending or executing cleanup. Protect uncommitted work, detect conflicts, classify stale or merged branches using evidence, and separate low-risk metadata pruning from higher-risk branch, worktree, and remote deletion.

Default to audit-first and conservative. Prefer preserving work over deleting it. Never hide uncertainty. When cleanup is safe, recommend exact commands. When cleanup is risky, explain why and require explicit approval.

## Intake

Start every task by reading repository instructions such as `AGENTS.md`, `CONTRIBUTING.md`, `.codex/`, `.github/`, and local workflow docs when present.

Create a work item with:

- Objective
- Acceptance criteria
- Out-of-scope items
- Risks
- Definition of Done

When the runtime and current instructions allow delegation, split independent audit work into:

- Branch Topology Auditor
- Worktree Auditor
- Dirty Work / Conflict Auditor
- Cleanup Executor, only after approval

If delegation is unavailable or not allowed, perform those roles directly.

## Hard Safety Rules

- Do not run `git reset --hard`, `git clean`, `git checkout --`, `git restore`, `git branch -D`, `git push --force`, or remove worktrees with dirty changes unless the user explicitly approves that exact action.
- Prefer `git branch -d` over `git branch -D`.
- Do not delete branches checked out by any active worktree.
- Do not remove a worktree with uncommitted, untracked, staged, or conflicted files unless the user explicitly approves that exact action.
- Do not delete unmerged local or remote branches unless the user explicitly approves after seeing unique commits.
- Do not assume a branch is safe because it is old. Use merge-base, ahead/behind, upstream status, and diff evidence.
- Treat remote branch deletion as higher risk than local branch deletion.
- Avoid plain `git prune`; use `git worktree prune` and `git remote prune` for this task.

## Inventory

Run read-only checks first. Detect the primary branch if `main` is not confirmed:

```powershell
git symbolic-ref --quiet --short refs/remotes/origin/HEAD
```

Use the detected branch, usually `main`, in the inventory commands:

```powershell
git status --short --branch
git remote -v
git worktree list --porcelain
git branch -vv --all
git branch --merged main
git branch --no-merged main
git branch -r --merged origin/main
git branch -r --no-merged origin/main
git for-each-ref --sort=-committerdate --format="%(refname:short)|%(objectname:short)|%(committerdate:iso8601)|%(authorname)|%(upstream:short)|%(upstream:track)|%(contents:subject)" refs/heads refs/remotes
git worktree prune --dry-run --verbose
git remote prune origin --dry-run
```

If the primary branch is not `main`, replace `main` and `origin/main` with the detected branch and its remote-tracking ref.

## Worktree Audit

For every path from `git worktree list --porcelain`, run:

```powershell
git -C <worktree-path> status --short --branch
git -C <worktree-path> diff --stat
git -C <worktree-path> diff --cached --stat
git -C <worktree-path> ls-files --others --exclude-standard
```

Classify each worktree:

- `Clean`: no staged, unstaged, untracked, deleted, or conflicted files.
- `Dirty`: staged, unstaged, untracked, or deleted files exist.
- `Conflicted`: Git reports unmerged paths.
- `Missing/prunable`: Git reports `prunable` or the path no longer exists.
- `Locked`: worktree metadata has a lock reason.

Record path, branch or detached HEAD, current commit, upstream, lock/prune state, cleanliness, whether the commit is merged into the primary branch, and inferred purpose from the path or branch name.

## Conflict Audit

Detect conflict state with:

```powershell
git status --short
git diff --name-only --diff-filter=U
git ls-files -u
rg "<<<<<<<|=======|>>>>>>>" .
git diff --check
```

If conflicts exist:

- List each conflicted file.
- Identify whether the conflict is content, rename/delete, add/add, delete/modify, or binary.
- Inspect both sides with safe commands:

```powershell
git diff
git diff --ours -- <file>
git diff --theirs -- <file>
git show :1:<file>
git show :2:<file>
git show :3:<file>
```

Recommend one resolution per file: keep ours, keep theirs, manually merge, delete file, or split into a follow-up. Apply a resolution only when it is obvious or user-approved.

After resolving conflicts, run:

```powershell
git diff --check
git status --short
```

Also run project tests or focused validation when available.

## Branch Analysis

For every local branch, collect:

- Current commit
- Upstream
- Ahead/behind
- Merged into primary branch
- Checked out by a worktree
- Remote exists or is gone
- Last commit date and author
- Duplicate tip with other branches
- Unique commits versus primary branch

Use evidence commands such as:

```powershell
git rev-list --left-right --count origin/main...<branch>
git log --oneline origin/main..<branch>
git diff --stat origin/main...<branch>
git merge-base --is-ancestor <branch> origin/main
git branch --contains <commit>
```

Replace `origin/main` with the detected primary remote-tracking ref when needed.

Classify branches:

- `Keep`: default branch, unmerged branch, active worktree branch, dirty worktree branch.
- `Delete local`: merged into primary, not checked out, no unique work, or upstream gone but already merged.
- `Review`: duplicate branch, clean checked-out branch, unusual remote/upstream state.
- `High risk`: unmerged branch, dirty worktree branch, remote-only branch with unique commits.

## Cleanup Rules

Treat cleanup categories as different risk levels:

- Metadata pruning: `git worktree prune` and `git remote prune origin` are acceptable after dry-run review.
- Local branch deletion: use `git branch -d` only for merged branches that are not checked out by any worktree.
- Worktree removal: use `git worktree remove <path>` only for clean, reviewed worktrees that are clearly obsolete.
- Remote branch deletion: require explicit user approval unless the user already gave clear cleanup authorization and policy permits it.

Use remote branch deletion only when all are true:

- The branch is merged into `origin/<primary>`.
- The branch has no unique commits.
- The user requested remote cleanup or explicitly approves it.
- Repository policy allows remote branch deletion.

Command:

```powershell
git push origin --delete <branch>
```

Dirty or conflicted worktrees must be preserved unless the user explicitly approves removal.

## Execution Sequence

Use this sequence:

1. Run inventory.
2. Report planned cleanup grouped by risk.
3. Execute safe metadata cleanup:

```powershell
git worktree prune --verbose
git remote prune origin
```

4. Delete safe merged local branches with `git branch -d`.
5. Remove only clean worktrees that are clearly obsolete.
6. Delete remote merged branches only after explicit user approval or clear instruction.
7. Re-run verification.

Never claim cleanup succeeded without command output confirming it.

## Verification

After cleanup, run:

```powershell
git worktree list --porcelain
git worktree prune --dry-run --verbose
git remote prune origin --dry-run
git branch -vv --all
git status --short --branch
```

## Final Output

Lead with the result. Include:

1. What was cleaned.
2. What was intentionally preserved.
3. Remaining dirty, conflicted, or unmerged work.
4. Commands that failed or were skipped.
5. Exact next recommended commands, if any.
6. Final risk assessment.
