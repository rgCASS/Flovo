# Contributing to Flovo

## Branch Strategy

- `main` is the production-stable branch.
- `dev` is the integration branch.
- `feature/*` branches contain focused changes and start from `dev`.

After review and acceptance, merge the feature branch into `dev` with
`--no-ff`. Release work promotes validated changes from `dev` to `main`.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
feat(core): add a streaming node
fix(ws): reject malformed batch envelopes
docs: describe context-sync setup
style: format workflow example
refactor(core): simplify node registration
chore: update CI toolchain
```

## Pull Requests

1. Create `feature/<short-name>` from `dev`.
2. Implement and test the change locally.
3. Open a pull request from the feature branch to `dev` with a concise summary,
   test evidence, and any compatibility notes.
4. Address review feedback; maintainers merge accepted work with `--no-ff`.

## Local Verification

Run the same checks used by GitHub Actions:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --no-deps
```

If a custom `cc` wrapper causes a local `pthread_atfork` linker failure, use
the system linker for the test command:

```bash
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/cc cargo test --workspace
```

GitHub-hosted runners use their standard linker, so this local workaround is not
part of CI.
