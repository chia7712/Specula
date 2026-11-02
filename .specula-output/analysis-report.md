# Code Analysis Report: KRaft dynamic quorum (audit trail)

- Target: `kafka/` trunk @ `8aa57006ff`, module `raft`
- Guidance: `guidance/kafka-kraft-dynamic-quorum.md`
- Category: A (Distributed / Message-Passing)
- Date: 2026-11-02

## Method
1. **Reconnaissance.** Core files and their sizes: KRC 4185 LOC, LeaderState 1089, QuorumState 933, KafkaRaftLog 838,
   UpdateVoterHandler 567, VoterSet 518, AddVoterHandler 446, KRaftControlRecordStateMachine 333, RemoveVoterHandler 244.
   Concurrency: a single raft IO thread, plus `createSnapshot` and the freeze callback running on the controller thread.
2. **Bug archaeology** (`analysis/archaeology.md`):
   - 359 commits scanned; 83 in-scope bug fixes plus 6 peripheral; 16 analysed in depth.
   - Open-PR heads scanned for #23271..#24070, found by listing PR refs with `git ls-remote`: 9 touch `raft/src/main`, 3 are real fixes.
   - JIRA, the Apache wiki, mailing lists and the GitHub REST API were all blocked by the session proxy, so issue discussions could not be
     read. Commit bodies stood in for them, which leaves the issue-coverage requirement only partly met.
3. **Deep analysis**: four parallel file-group passes.
   - Voter-change handlers + LeaderState → `analysis/deep-voter-change.md`
   - Election, pre-vote and persistence → `analysis/deep-election.md`
   - Snapshots and voter history → `analysis/deep-snapshot.md`
   - Fetch, HW and observers → `analysis/deep-fetch-hw.md`
4. **Verification**:
   - Every finding was re-read in code, and the lead verified the top findings independently: KCRSM:117-129/227/253/245-290,
     TreeMapLogHistory:64-70, KRC:1745-1768/2190-2213/3825-3846/1210-1224, LeaderState:410-426/826-842,
     RemoveVoterHandler:210-243, ChangeVoterHandlerState:245-260.
   - Probe tests were run against RaftClientTestContext and saved under `analysis/` and `analysis/probes/`. They were removed from the
     kafka tree, which is unmodified.
   - Some probe runs used `javac` plus a reflective runner, because Gradle hit Maven Central rate limits (HTTP 429).

## Coverage of guidance priority questions
| Guidance Q | Outcome | Where |
|---|---|---|
| Q1: uncommitted change + leader failure, single-change enforcement | The single-change rule is enforced on every leader path, and a new leader must commit in its own epoch first. The open risk is the follower-side voter history (S1) | brief S1, S2; MC-1/2/3 |
| Q2: remove, then re-add with a new directory id | No defect on kraft.version 1; directory-id wildcard semantics noted | brief S4; MC-7 |
| Q3: snapshot fetch during concurrent append, commit and delete | The log prefix is safe; the voter-set history is not (S1 b/c); figure-8 reachability is open | brief S1, S5; MC-2/3/4 |
| Q4: pre-vote disruption | A pre-vote never raises the sender's own epoch, but raises the *receiver's* epoch; the UNSUPPORTED_VERSION shortcut skips pre-vote | brief S3; MC-6 |
| Q5: quorum-state write ordering | Safe: fsync plus atomic rename happen before the response is built; edge cases are a v0 file and state-file loss | brief S4; MC-8 |

## Findings summary
- **Confirmed by probe**:
  - Leader crash on a late BeginQuorumEpoch ack from a removed voter.
  - Crash when Unattached-with-leader receives a Fetch reply from that leader.
  - Removed leader never resigns after the RemoveVoter op times out.
- **Unit-level repro, end-to-end consequence pending MC**: voter history not rewound on truncation; snapshot install skips the reload; trim off-by-one.
- **Code-read only**: a pre-vote with a higher epoch bumps the receiver; UNSUPPORTED_VERSION skips pre-vote; `isReplicaCaughtUp` ignores
  offsets; a leader's own id in FetchSnapshot or UpdateVoter crashes the leader; ackWhenCommitted=false can lose the record; snapshot epoch
  below the local epoch leaves the follower stuck.
- **Excluded or verified safe**: listed per file in each `analysis/deep-*.md` under "checked and safe".

## Persistent findings
`specula findings init` was run (`persistent-findings-context.json`). No prior registry exists, so there was nothing to reuse.
