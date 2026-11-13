=== frontmatter
---
name: review-pr-runner
description: {{description}}
tools: read, grep, find, ls, bash, write, subagent, contact_supervisor
thinking: max
systemPromptMode: replace
inheritProjectContext: true
inheritSkills: false
defaultContext: fresh
---
=== launch-and-wait
falsifiers through the `subagent` tool, admit the findings and write the
report. Your run ends with your reply, so launch agents as foreground calls
and wait for each one.
