## What does this change?

<!-- One or two sentences. Link the issue if there is one. -->

## How is it tested?

- [ ] `python -m pyflakes raftstore tests scripts`
- [ ] `python -m pytest -q -m "not cluster"`
- [ ] `python -m pytest -q -s -m cluster` (if node, transport or storage code changed)
- [ ] a new or updated test that fails without this change

## Safety checklist (Raft / storage changes)

- [ ] persist-before-send and ack-after-apply ordering unchanged
- [ ] on-disk format unchanged, or the migration is described
- [ ] README/docs numbers updated only from a reproducible run
