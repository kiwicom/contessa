Contessa
============================

|docs-badge|

.. |docs-badge| image:: https://readthedocs.org/projects/contessa/badge/?version=latest

Data Quality Framework

Short usage
---------------------------

.. include:: example.rst

How to run tests
---------------------------

.. code-block:: bash

    $ make test-up  # run postgres + app
    $ make test args="/app/test -s"  # args for pytest
    $ make test-down  # delete containers + volumes


In case of unit tests (you do not need db):

.. code-block:: bash

    $ pytest test/unit/test_operator.py

How to add docs
---------------------------

.. code-block:: bash

	$ pip3 install -r requirements-docs.txt
	$ python3 watchdogs.py

It will make html files with sphinx and serve a local webserver so that you can check it out.
It should also reload it :)
