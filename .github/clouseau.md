# Colombo review contract

Colombo is a single-process Rust gateway for camera FTP/FTPS and HTTP uploads.
It validates tenant credentials with the tenant CMS, durably accepts files,
delivers them to S3, and independently confirms delivery with the CMS.

Preserve its small operational footprint, explicit interfaces, tenant and
assignment isolation, durability before acknowledgement, bounded retries,
recoverable spool, and private credential handling. An accepted upload is durable
locally; it is not a promise that S3 delivery and CMS confirmation have completed.
Naming policies and receipt retention must keep their documented semantics.

Assess purpose and evidence, rather than counting changed files or lines.
Meaningful tests that raise coverage, implementation repairs, dependency updates,
and operational maintenance can fit the existing service without a new product
decision. Review their actual behavior, risks, assertions, and repository precedent.
A new feature can also fit when trusted documentation or linked acceptance
criteria already support it. Tests, infrastructure, and integration files are not
automatically a reason to require an owner decision.

Ask gaulatti for a second opinion when a change redirects the product, changes
its delivery or security guarantees, or lacks evidence for an ambiguous product
decision. Turning this upload gateway into a music tool or a regulated health-data
product would require that decision. Changes to the review controls or this
purpose contract require explicit owner confirmation.

Tests must exercise the production behavior and assert meaningful outcomes and
failure cases. Detect fabricated reports, always-true assertions, disabled tests,
swallowed failures, test-only success paths, coverage exclusions, and mocks that
replace the behavior being claimed. Ordinary mocks at external CMS/S3 boundaries
are legitimate when request contracts and failure handling remain exercised.
Detected test fakery blocks approval and cannot be waived by intent confirmation.
Missing verification or unresolved integrity holds the PR for bounded automated
investigation; it does not invent fakery or delegate ordinary code review to the
owner. Human input is for purpose, authority, or a real outcome tradeoff.

Every material capability and mechanism must earn fit, need, and proportionality
of at least 80 under Clouseau's shared protocol. Supporting tests inherit the
accepted production obligation when their assertions establish that behavior.
Each causal technical finding is scored separately; a score of at least 70
blocks. A good feature cannot average away an unrelated capability or a defect.
Required integrity and review-completeness gates remain independent of scores.

Clouseau owns the shared assessment, technical gate, owner handoff,
revision-bound confirmation, approval, and auto-merge rules. The pinned action
and .github/clouseau.json define those operations. All tests and exactly 100%
line and branch coverage remain mandatory for every contribution.
