<div align="center">
  <a href="https://questdb.io/" target="blank"><img alt="QuestDB Logo" src="https://questdb.io/img/questdb-logo-themed.svg" width="305px"/></a>
</div>
<p>&nbsp;</p>

# SWE 261P Project Part 4: Continuous Integration of QuestDB

**Team Member: Jane He, Fengnan Sun, Ming-Hua Tsai**

**GitHub username: [SiyaoIsHiding](https://github.com/SiyaoIsHiding), [SoniaSun810](https://github.com/SoniaSun810), [alimhtsai](https://github.com/alimhtsai)**

- [Continuous Integration Overview](#continuous-integration-overview)
  - [What is Continuous Integration (CI)?](#what-is-continuous-integration-ci)
  - [Why do Continuous Integration (CI)?](#why-do-continuous-integration-ci)
  - [Common practices of Continuous Integration (CI)](#common-practices-of-continuous-integration-ci)
- [Our Github Action 1: Maven Build and Test](#our-github-action-1-maven-build-and-test)
  - [Configuration](#configuration)
  - [Outcome](#outcome)
- [Our Github Action 2: Markdown To PDF](#our-github-action-2-markdown-to-pdf)
  - [Configuration](#configuration-1)
  - [Outcome](#outcome-1)
- [Existing Github Action: Danger - Validate PR Title](#existing-github-action-danger---validate-pr-title)
- [Existing Azure Pipelines](#existing-azure-pipelines)

----

## Continuous Integration Overview

### What is Continuous Integration (CI)?
Continuous Integration (CI) is a software development practice in which developers merge their code changes frequently into a central repository, and each merge is automatically built, tested, and verified by an automated build system.

### Why do Continuous Integration (CI)?
The main purpose of CI is to catch and correct errors early in the development process, before they become bigger problems, by continually integrating new code changes and testing them for errors.

The benefits of implementing a Continuous Integration (CI) system are numerous. Some of the main advantages of CI are:

1. Early detection of bugs and errors: CI allows developers to catch and fix errors early in the development process, which saves time and reduces the cost of fixing errors later on.
2. Improved code quality: By integrating new code changes frequently and testing them for errors, CI helps ensure that code is of higher quality, more stable, and more reliable.
3. Faster delivery of new features: By accelerating the process of integrating and deploying code changes, it enables frequent releases of new features and, as a result, feedback.
4. Better collaboration: CI promotes better collaboration among team members by providing a shared platform for development and testing, improving communication, and reducing the risk of conflicts between team members.

### Common practices of Continuous Integration (CI)
There are several best practices that are commonly used in Continuous Integration (CI), including:
1. Using a single source version control system: Version control systems, such as Git or SVN, is essential for implementing CI, as it allows developers to easily manage code changes and track versions.
2. Automated builds: CI systems automatically build the code and test it for errors, allowing developers to focus on writing code instead of manually building and testing it.
3. Automated testing: Automated testing ensures that the code is functioning correctly and meets the required specifications.
4. Continuous feedback: CI systems provide continuous feedback to developers on the status of their code changes, helping them identify errors and resolve issues quickly.
5. Automated deployment: CI systems can be integrated with continuous deployment (CD) systems, allowing developers to deploy code changes to production as soon as they are ready.
6. Clone the production environment in the runner: The build and test process should run on the same environment as in production to simulate the codes' behavior when deployed in production.
7. Process visibility: Enable global access to the status of the CI processes
8. Ensure access to the latest executable: As the source code in the main line is centralized, everyone should have access to the latest executable for various uses, such as testing.
9. Short run time of CI process: The processing time of running CI tasks should usually be shorter than 10 minutes.
10. Contribute frequently to the same main line: Usually, everyone should make a commit at least once per day.

By implementing these best practices, software development teams can improve code quality, accelerate the development process, and deliver better software products to their customers.

## Our Github Action 1: Maven Build and Test
### Configuration
We added the GitHub Action configuration file `.github/workflows/mvn-test.yml` with the content below:

```yml
name: Java CI

on: [push]

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'
          distribution: 'temurin'
      - name: Build with Maven
        run: mvn clean test
```

It means that upon a push to any branches, there will be a runner with ubuntu 22.04 operating system that:
1. download a copy of our source codes
2. configure the Eclipse Temurin Java 17 SDK by Eclipse Adoptium
3. run all the test cases

### Outcome

The Github Action turns out a pass, but with some annotations of error. We checked the log file and confirmed that all test cases were passed.

```text
2023-02-23T21:40:38.5483549Z Results :
2023-02-23T21:40:38.5483723Z 
2023-02-23T21:40:38.5483872Z Tests run: 12126, Failures: 0, Errors: 0, Skipped: 231
```

We think the annotations may come from some test cases designated to fail.

Besides, it takes 27 minutes in total to run. It is usually considered too long for a CI process. However, the QuestDB existing build and test Azure pipelines (explained [below](#existing-azure-pipelines)) also take around 10 - 20 minutes. Considering the large scale of QuestDB and that we didn't pay for higher performance runners to Github, the 27 minutes processing time is reasonable and we cannot really do anything to improve that.

## Our Github Action 2: Markdown To PDF

When we were doing project part 2, we already added a Github Action (the [source codes](https://github.com/BaileyJM02/markdown-to-pdf)) to convert our report written in markdown to pdf automatically on each push to master.

### Configuration

We added the GitHub Action configuration file `.github/workflows/convert-to-pdf.yml` with the content below:

```yml
# .github/workflows/convert-to-pdf.yml

name: Docs to PDF
# This workflow is triggered on pushes to the repository.
on:
  push:
    branches:
     - master
    # Paths can be used to only trigger actions when you have edited certain files, such as a file within the /docs directory
    paths:
      - 'docs/**'
      - '.github/**'

jobs:
  convert-to-pdf:
    name: Build PDF
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: baileyjm02/markdown-to-pdf@v1
        with:
          input_dir: docs
          output_dir: docs/
          images_dir: docs/images
          image_import: ./images
          build_html: false
      - uses: actions/upload-artifact@v1
        with:
          name: theGeneratedPDF
          path: docs
```

It means that upon push to the master branch, if there are changes in the path `docs/**` or `.github/**`, there will be a runner with ubuntu 22.04 operating system that:
1. download a copy of our source codes
2. convert all the markdown files in the path `docs` to pdf files, supposing the images are in `docs/images` folder, and output the pdf files to the directory `docs/`
3. upload the folder `docs` as a zipped file
   
### Outcome
We didn't succeed at the first trial. After adding some logging commands in the GitHub Action configuration file, including the `ls -R`, we find that it is because we put our markdown files in the folder `.github/` but the folder is not downloaded to the runner who runs the action.

But even if we put our markdown files elsewhere, it did not succeed either. We specified the `output_dir` as `docs`, but it outputs the pdf file called `docREADME.md` to the project root. Therefore, since we changed the `output_dir` to `docs/`, it works well now.

They have clearly specified in their documentation that we should declare our path without the `/` suffix. Therefore, we think it is a bug in their logic, in their source code file `src/github_interface.js` around line 64. Hence, we planned to fix that bug and contribute to this GitHub Action source code in the future when we have time.

## Existing Github Action: Danger - Validate PR Title
QuestDB's original repository already defined a Github Action to validate their pull request title. 

They use a CI tool called [Danger](https://danger.systems/js/). Danger is a tool to automate the common code review chores and leave messages inside the pull requests based on the rules defined in scripts.

In this case, they let Danger validate their pull request titles formatting, with Danger configuration file `ci/validate-pr-title/dangerfile.js`, and formatting rules in `ci/validate-pr-title/validate.js`. Basically, it should be in the format `type(subtype):  description`. The allowed types and subtypes can be found in `ci/validate-pr-title/validate.js`. For example, `fix(config) add GitHub action` will be a valid title. If the title is invalid, the GitHub Action will fail.

Besides, they wrote some tests to test the formatting rule itself in `ci/validate-pr-title/validate.test.js`.

The Github Action configuration file is in `.github/workflows/danger.yml`, with the content below:

```yml
name: Danger

on:
  pull_request:
    types: [synchronize, opened, reopened, edited]

jobs:
  build:
    if: ${{ github.event.pull_request.head.repo.full_name == github.repository }} # Only run on non-forked PRs
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@master
      - name: Use Node.js 16.x
        uses: actions/setup-node@master
        with:
          node-version: 16.x
      - name: install danger 
        run: yarn global add danger 
      - name: Validate PR title validation rules
        working-directory: ./ci/validate-pr-title
        run: node validate.test.js
      - name: Danger
        run: danger ci
        working-directory: ./ci/validate-pr-title
        env:
          DANGER_GITHUB_API_TOKEN: ${{ secrets.DANGER_GITHUB_TOKEN }}
```

It means if there is a non-forked pull request in type synchronize, opened, reopened, or edited, there will be a runner with ubuntu 22.04 operating system that:

1. download a copy of our source codes
2. setup Node.js environment
3. install the `danger` tool
4. run the tests in `ci/validate-pr-title/validate.test.js` to test the formatting rule
5. let Danger validate the pull request title

## Existing Azure Pipelines

QuestDB has many existing CI processes hosted in Azure pipelines. All the relevant files are in the directory `ci/`. The most important two are `ci/template/hosted-jobs.yml` and `ct/templates/hosted-cover-jobs.yml`, both are linked from `ci/test-pipeline.yml`.

The `ci/template/hosted-jobs.yml` run the test cases in macos-latest, windows-lates, and ubuntu-latest operating systems respectively. They also split all the test cases into three groups -- griffin, cairo, and others and run them separately.

The `ct/templates/hosted-cover-jobs.yml` utilize the Jacoco tool to measure the test coverage and generate coverage reports automatically. Only ubuntu will run this task and the tests are also separated into griffin, cairo, and others.

As their Azure pipelines are not watching our repository, which is forked from their official repository, no runners really run these checks upon the changes in our repository.

But as we created a new pull request to their official repository to contribute to that backslash escape feature in LIKE statements, their runners ran the CI processes and some failed. As we have no access to the log statements, we would communicate with them further on what we should do next.
