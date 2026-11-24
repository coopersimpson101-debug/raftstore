# Contributing to raftstore

Thanks for your interest! raftstore is a small codebase written from scratch. Contributions that keep it
small, stdlib-only and well-tested are very welcome.

## Ground rules

- **No runtime dependencies.** The package must run on the Python 3.11+ standard library alone.
  `pytest`, `hypothesis` and `pyflakes` are for testing and development only.
- **Safety first.** Any change to `raftstore/raft/` or `raftstore/storage/` must keep the simulator,
  the invariant checker and the linearizability checker green. A new fault or feature should come with a
  test that would have failed without it.
- **Be honest about evidence.** Numbers in the README and docs must come from a run you can reproduce.
  Say where it ran (machine, OS, Python). Hardware results go in the tables in `docs/operations.md`.

## Development setup

```sh
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install pytest hypothesis pyflakes
python -m pyflakes raftstore tests scripts
python -m pytest -q -m "not cluster"               # fast part (simulator, storage, linearizability)
python -m pytest -q -s -m cluster                  # real 3-process kill -9 test
python scripts/chaos_demo.py                       # narrated end-to-end run
```

For a deeper simulation sweep, run `RAFTSTORE_SIM_SEEDS=300 python -m pytest -q tests/test_simulation.py`.
If a schedule fails, the assertion message names the seed. Runs are fully deterministic, so
`SimCluster(seed, cfg, trace=True)` replays the failure step by step.

The README figures are generated from real runs: `python scripts/make_figures.py`.

## Pull requests

1. Open an issue first for anything non-trivial, especially changes to the protocol or the on-disk format.
2. Keep PRs focused. Describe which invariant or behaviour the change preserves or adds.
3. CI must pass: lint, tests on 3.11–3.13, the cluster test and the chaos demo.

By contributing you agree that your contributions are licensed under the MIT License.
