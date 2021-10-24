name: 🐞 Bug report
description: Create a bug report to help us improve QuestDB
title: "[Bug]: "
labels: [kind/bug, ]
assignees: 
- 
body:
- type: markdown
  attributes:
    value: |
      Thanks for taking the time to fill out this bug report! Please fill the form in English!
- type: checkboxes
  attributes:
    label: Is there an existing issue for this?
    description: Please search to see if an issue already exists for the bug you encountered.
    options:
    - label: I have searched the existing issues
      required: true
- type: textarea
  attributes:
    label: Current Behavior
    description: A concise description of what you're experiencing.
    placeholder: |
      When I do <X>, <Y> happens and I see the error message attached below:
      ```...```
  validations:
    required: true
- type: textarea
  attributes:
    label: Expected Behavior
    description: A concise description of what you expected to happen.
    placeholder: When I do <X>, <Z> should happen instead.
  validations:
    required: false

- type: textarea
  attributes:
    label: Steps To Reproduce
    description: Steps to reproduce the behavior.
    placeholder: |
      1. In this environment...
      2. With this config...
      3. Run '...'
      4. See error...
    render: markdown
  validations:
    required: false

- type: textarea
  attributes:
    label: Anything else?
    description: |
      Links? References? Anything that will give us more context about the issue you are encountering!
  validations:
    required: false
