=== description
Develop features and fix bugs or review findings one at a time, test-first. Design each change and its test plan, have both challenged independently, and add tests until every changed line and branch runs. Then run review-pr at level 3 over the whole PR until nothing blocks. Use when the user runs {{cmd}}fix-pr or asks to develop or fix items with it.
=== argument_hint
[--block-at=critical|high|medium|low] [--fix-moderate] [--include-adjacent] [--decisions=<file>] [--review-level=N] [--max-review-rounds=N] <items: features, bugs or review findings>
