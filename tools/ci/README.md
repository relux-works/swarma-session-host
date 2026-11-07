The module workflow uses `module_checks.py` to run vet and the full race suite.
Each check retains a separate load admission: one-minute load below 20, up to
120 seconds of waiting, with polls at most five seconds apart. Both checks are
attempted even when the first fails or cannot start. The race command retains
its five-minute Go timeout and the job retains its fifteen-minute timeout.

Each JSON check record distinguishes an observed command exit from an
unavailable check whose `exit_code` is null. An admission refusal has
`reason: load_admission_refused` and `admission_exit_code: 125`. A command that
actually returns 125 is a product-check failure, not an admission refusal.

The overall exit is 0 only when both checks actually pass, 1 when any check
actually fails, and 125 when checks are unavailable without an actual failure.
GitHub Actions still reports a nonzero exit as a failed job. The classification
allows an operator to identify infrastructure refusal without claiming a test
ran or passed. It neither fixes hosted capacity nor waives the required check;
an unavailable run still needs an admitted run before delivery.

Run the deterministic classification controls with:

```sh
python3 -m unittest discover -s tools/ci -p 'test_module_checks.py' -v
```
