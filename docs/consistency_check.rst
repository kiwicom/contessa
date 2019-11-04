..  _consistency_check:

ConsistencyCheck
==============================

.. code-block:: python

    class QualityCheck:
        id = Column(BIGINT, primary_key=True)
        type = Column(TEXT)
        name = Column(TEXT)
        description = Column(TEXT)
        left_table = Column(TEXT)
        right_table = Column(TEXT)

        status = Column(TEXT)
        time_filter = Column(TEXT)
        task_ts = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
        created_at = Column(
            DateTime(timezone=True),
            server_default=text("NOW()"),
            nullable=False,
            index=True,
        )