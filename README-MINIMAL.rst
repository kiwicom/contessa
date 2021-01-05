Contessa
============================

Hello, welcome to Contessa!

Contessa is a **Data Quality** library that provides you an easy way to define, execute and
store quality rules for your data.

Instead of writing a lot of sql queries that look almost exactly the same, we're aiming for more
pragmatic approach - define rules programatically. This enables much more flexibility for the user and also for us as the creators of the lib.

We implement new Rules (incrementally) that should reflect Data Quality domain. From the start these are simple
rules like - NOT_NULL, GT (greater than) etc. We want to build on these simple rules and provide more complex Data Quality checkers out-of-the-box.

**Goals:**

- be database agnostic (to a reasonable degree), so you will define checks against any database (e.g. mysql vs. postgres) in the same way
- automatize data quality results e.g. from postgres table to Datadog dashboard
- programmatic approach to data-quality definition, which leads to:

  - dynamic composition of rules in a simple script using db or any 3rd party tool - e.g. take all tables, create NOT_NULl rule for all of them for each integer column

  - users can use special rules for data if needed, if not, they can go with generic solutions

  - automatizable testable parts of definitions when needed

- easier maintenance when number of checks scales too fast :)

Full docs here https://contessa.readthedocs.io/en/latest
