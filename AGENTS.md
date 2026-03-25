# AGENTS.md

Repository-level instructions for human and LLM contributors.

## Scope

Applies to the entire repository.

## Mandatory Context Bootstrap

Before making architectural or broad refactor changes, read these in order:

1. `docs/README.md`
2. `docs/DECISIONS.md`
3. `docs/CAPABILITY_MATRIX.md`
4. `docs/EXECUTION_TRACKER.md`
5. `docs/STATUS.md`

Do not infer active status from memory.

## Current Contract

1. `qdrant-datafusion` is being rebuilt from first principles, not migrated from the earlier spike.
2. Canonical Arrow vector carriers come from `ndarrow` and `nabled::arrow`.
3. Collection scans use paginated `Qdrant::scroll`, not `query`.
4. Vector columns are top-level nullable:
   - missing vector on a row => `NULL`
   - present vector => canonical dense / multivector / sparse carrier
5. Only current typed `VectorOutput.vector` paths are admitted. Deprecated response fields are dead code.
6. The stable SQL-to-`Qdrant` semantic bridge is not designed yet. Do not invent UDF or planner surface without an explicit planning pass.

## Quality Gates

Run and pass before finalizing:

1. `just checks`
2. `just test-unit`

## Editing Rules

1. Prefer deletion over adaptation when old code conflicts with the admitted contract.
2. Keep scan-path code tight: destructure early, keep error strings short, prefer `exec_err!`.
3. Single-use local helpers are a code smell. If a function is defined in a file and used once in that same file, inline it unless it clearly captures a distinct multi-call-site contract or avoids substantial duplication.
4. Do not preserve speculative compatibility shims for unreleased APIs.
5. Update `README.md`, `docs/CAPABILITY_MATRIX.md`, `docs/EXECUTION_TRACKER.md`, and `docs/STATUS.md` in the same change set as any non-trivial behavior change.
6. When entering SQL / planner / pushdown work, prefer `DataFusion`’s existing idioms and APIs over project-local traversal patterns:
   - `TreeNode` visitors / rewriters
   - `LogicalPlan` expression and subquery helpers
   - source capability checks
   - physical sort pushdown hooks
7. Keep shared semantic vocabulary separate from scan-local runtime state.
   - payload paths, payload schema, and filter IR belong in shared semantic modules
   - scan selectors, scan specs, and continuation state belong with the table / scan runtime
8. Prefer type-owned behavior over detached helper functions when there is a clear semantic owner.
   - use constructors and classifiers such as `Type::from_plan(...)` / `Type::of(...)`
   - keep free functions for genuinely ownerless local helpers only
