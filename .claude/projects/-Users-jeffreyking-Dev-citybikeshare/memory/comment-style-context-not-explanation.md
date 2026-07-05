---
name: comment-style-context-not-explanation
description: How to write code comments in this repo — why-not-what, no repeated slogans
metadata:
  type: feedback
---

Comments should explain **why the code is there / context**, not restate what the code
does. Don't repeat the same slogan across many comments — e.g. stop tagging every
validation with "fail loud" / "CLAUDE.md"; the [[prefers-loud-failures-over-silent-tolerance]]
principle is understood, so a comment should add the *specific reason* this check exists,
not re-announce the philosophy.

**Why:** restating the code or repeating a known principle is noise; the scarce, valuable
thing a comment carries is context that isn't visible in the code itself.

**How to apply:** when commenting, ask "does this say something the code doesn't?" Prefer
notes about non-obvious source quirks, historical reasons, or cross-file coupling. Cut
comments that paraphrase the line below them or reassert a repo-wide convention.
