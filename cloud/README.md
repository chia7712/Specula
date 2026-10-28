# Running Specula on Kafka from a Claude Code cloud session

This branch (`kafka`) adds what a cloud session needs on top of upstream Specula:

| Path | Purpose |
| --- | --- |
| `.claude/skills/` | Relative symlinks to `skills/`, so Claude Code sees `/code-analysis` etc. |
| `.mcp.json` | The three Specula MCP servers, launched through `cloud/mcp.sh` (no absolute paths) |
| `cloud/setup.sh` | Non-interactive setup: venvs, CFA jar, Kafka clone, specula CLI |
| `lib/*.jar` | TLA+ tools committed so no GitHub release download is needed in the VM |
| `guidance/` | Modeling guidance files, one per Kafka target |
| `CLAUDE.md` | Instructions Claude Code reads at session start |

## One-time environment setup (phone or browser)

1. Open claude.ai/code, pick this repository, branch `kafka`.
2. Open the environment selector (cloud icon above the message box) and add an environment:
   - Name: `specula-kafka`
   - Network access: Trusted (default)
   - Setup script:
     ```bash
     bash cloud/setup.sh
     ```
   - Optional variables: `KAFKA_REF=trunk` (or a release tag such as `4.2.0`)
3. Create the environment. The first session runs the setup script and caches the result for about a week.

## Starting a run

Send this as the first message of a session:

```
Read guidance/kafka-consumer-group-rebalance.md. The target is Apache Kafka at ./kafka,
modules group-coordinator and coordinator-common. Run /code-analysis and stop when
modeling-brief.md is written.
```

Then review `.specula-output/modeling-brief.md` and continue with
`/spec-generation`, `/harness-generation`, `/validation-workflow`, `/bug-confirmation`,
one per message, reviewing the output between phases.

## Keeping results

Ask Claude to commit `.specula-output/` to a `results/<date>-<phase>` branch and push
before the session ends. The VM disk is not kept.

## Syncing with upstream Specula

```bash
git fetch https://github.com/specula-org/Specula.git main
git merge FETCH_HEAD
```

If the `skills/` directory gains or renames skills, regenerate the symlinks:

```bash
rm -rf .claude/skills
python3 -I src/specula/skill_install.py --source skills --target .claude/skills
cd .claude/skills && for l in *; do t=$(basename "$(readlink "$l")"); rm "$l"; ln -s "../../skills/$t" "$l"; done
```
