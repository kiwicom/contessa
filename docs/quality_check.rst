..  _quality_check:

QualityCheck
==============================

.. code-block:: python

    class QualityCheck:
        id = Column(BIGINT, primary_key=True)
        attribute = Column(TEXT)
        rule_name = Column(TEXT)
        rule_description = Column(TEXT)
        total_records = Column(INTEGER)

        failed = Column(INTEGER)
        median_30_day_failed = Column(DOUBLE_PRECISION)
        failed_percentage = Column(DOUBLE_PRECISION)

        passed = Column(INTEGER)
        median_30_day_passed = Column(DOUBLE_PRECISION)
        passed_percentage = Column(DOUBLE_PRECISION)

        status = Column(TEXT)
        time_filter = Column(TEXT)
        task_ts = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
        created_at = Column(
            DateTime(timezone=True),
            server_default=text("NOW()"),
            nullable=False,
            index=True,
        )
