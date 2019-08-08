# Contessa

Data-quality framework

# How to run tests

```bash
$ make test-up  # run postgres + app
$ make test args="/app/test -s"  # args for pytest
$ make test-down  # delete containers + volumes
```

In case of unit tests (you do not need db):
```
$ pytest test/unit/test_operator.py
```