# CLAUDE.md — Project Guidelines for Claude Code

> 12 rules, 4 from Karpathy's original observations + 8 earned across 30 codebases.
> Every rule addresses a specific, recurring failure mode.

---

## 1. Think Before Coding
**Addresses: silent wrong assumptions, hidden confusion, missing tradeoffs.**

- State your assumptions explicitly before writing code. If multiple interpretations exist, list them and ask — do not pick one silently.
- When the request is ambiguous, ask one focused clarifying question. Do not guess.
- If you're confused by something in the codebase, say so. Do not proceed past confusion.
- Push back when a simpler approach exists, or when the request conflicts with existing architecture.

## 2. Simplicity First
**Addresses: overengineering, speculative abstractions, bloated interfaces.**

- Write the minimum code that solves the stated problem. Nothing more.
- No abstractions for single-use code. No "for future" flexibility that wasn't asked for.
- No error handling for scenarios that cannot occur in practice.
- If your output feels overcomplicated, it is. Delete and rewrite.
- **Test:** Would a senior engineer call this overengineered? If yes, simplify.

## 3. Surgical Changes
**Addresses: orthogonal edits, touching code you shouldn't, silent refactors.**

- Every changed line must trace directly to the user's request.
- Do NOT "improve" adjacent code, comments, formatting, or variable names.
- Do NOT refactor things that aren't broken. Do NOT delete pre-existing dead code unless explicitly asked.
- Match the existing code style, even if you'd do it differently.
- Clean up ONLY the orphans your own changes create (unused imports, dead variables).

## 4. Goal-Driven Execution
**Addresses: vague task interpretation, endless loops, no definition of done.**

- Before writing code, define what "done" means in verifiable, machine-checkable terms.
- Transform vague requests:
  - "Add validation" → "Write tests for invalid inputs, then make them pass."
  - "Fix the bug" → "Write a test that reproduces it, then make it pass."
  - "Refactor X" → "Ensure all existing tests pass before and after."
- For multi-step tasks, state a numbered plan with verification checkpoints before starting.

## 5. Verify, Don't Assume
**Addresses: code that "looks right" but doesn't actually work.**

- Before claiming a bug is fixed, write a test that reliably reproduces it, fix the code, and run the test. Only when it passes is the bug fixed.
- After any code change, run the relevant test suite. If no tests exist, write a minimal smoke test.
- Never rely on visual inspection alone. Execution is the only truth.

## 6. Debug Systematically
**Addresses: confident wrong diagnosis, guessing at fixes.**

- Read the FULL error message and stack trace before forming a hypothesis.
- Reproduce the problem before attempting a fix. Confirm you're fixing the real issue.
- Change ONE variable at a time. If a fix doesn't work, revert before trying something else.
- State your diagnosis and expected outcome before applying a fix. If the outcome doesn't match, stop and reassess.

## 7. Surface Uncertainty
**Addresses: hallucinations masquerading as confidence.**

- Distinguish clearly between what you KNOW (in the codebase, documented, tested) and what you're INFERRING.
- When operating on partial information, flag it: "I can see X but not Y. I'm proceeding assuming Z — correct?"
- If a library, API, or pattern is unfamiliar, check documentation or the codebase before using it. Do not invent APIs.

## 8. Preserve Intent
**Addresses: stripping comments, removing intentional quirks, deleting "why" context.**

- Comments that explain WHY (not what) are sacred. Never remove them unless they're factually wrong.
- If code looks "weird," assume it's intentional until proven otherwise. Research git blame or ask before changing it.
- When you must change commented intent, preserve the original reason as context in your response.

## 9. Own Your Mess
**Addresses: generated files left behind, incomplete cleanup, broken builds after changes.**

- If you create temporary files, delete them when done. If you add a dependency, verify it doesn't break the build.
- After a multi-file change, do a self-audit: list every file you touched and confirm each change was necessary.
- If your change breaks a test you didn't anticipate, fix it — don't leave it for the user.

## 10. Respect the Stack
**Addresses: introducing new patterns, libraries, or paradigms that clash with the existing stack.**

- Use the project's existing libraries, patterns, and conventions. Do not introduce a new dependency unless explicitly asked.
- Do not rewrite existing code in a different style or paradigm because you "prefer" it.
- If the project uses plain functions, don't introduce classes. If it uses React, don't suddenly switch to a different pattern.

## 11. Fail Fast, Fail Loud
**Addresses: silent degradation, swallowed errors, graceful-failure-as-bug-factory.**

- Errors should surface immediately with actionable messages. Never silently swallow exceptions.
- If a precondition isn't met, fail at startup (or at the earliest possible point), not 10 steps later with a cryptic error.
- When handling an error, include enough context for the next person to diagnose it without reading the source.

## 12. Leave It Better Than Metadata
**Addresses: accumulating cruft across sessions, invisible tech debt.**

- If your task touches a file with clear problems (dead code, broken formatting, outdated patterns), MENTION it — but don't fix it unless asked.
- Track and report technical debt you observe. The human decides whether to act on it.
- After completing a task, offer a 1-line summary of anything you noticed that might be worth a follow-up.
