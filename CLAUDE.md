# Specula on Apache Kafka (cloud branch)

This checkout is Specula (TLA+ based bug finding) prepared to run against
Apache Kafka inside a Claude Code cloud session.

- Specula root: this directory. Skills live in `.claude/skills/` and the MCP
  servers (`tracedebugger`, `spec_analyzer`, `inv_checking_tool`) are declared
  in `.mcp.json`. Everything resolves relative to this directory; do not move
  files around.
- Target system: Apache Kafka source at `./kafka` (cloned by `cloud/setup.sh`).
  Build with `./gradlew` from inside `kafka/`. The VM has about 16 GB RAM, so
  pass `--max-workers=2` and prefer compiling only the modules you need, for
  example `./gradlew :group-coordinator:compileJava :coordinator-common:compileJava`.
- Modeling guidance: `guidance/kafka-consumer-group-rebalance.md`. Read it
  before starting any phase. It defines scope, priority questions and the
  files to start from.
- Workflow: run the skills in order and stop after each one so the human can
  review the output in `.specula-output/` before continuing:
  `/code-analysis` -> `/spec-generation` -> `/harness-generation` ->
  `/validation-workflow` -> `/bug-confirmation`.
  See `skills/workflow-overview.md` for what each phase produces.
- TLC memory: keep the JVM heap at or below 8 GB (`-Xmx8g`) and never run
  more than one TLC instance at a time.
- Persisting results: the VM disk disappears when the session ends. When a
  phase finishes, commit `.specula-output/` and any Kafka changes under
  `kafka/` as a patch file (`git -C kafka diff > .specula-output/kafka-harness.patch`)
  to a branch named `results/<date>-<phase>` and push it, so the next session
  can resume.
- If `cloud/setup.sh` did not run (missing `tools/*/.venv` or
  `tools/cfa/target/*.jar`), run `bash cloud/setup.sh` first.
