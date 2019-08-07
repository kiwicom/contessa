from datetime import datetime, timedelta
from statistics import median

import pandas as pd
from sqlalchemy import and_, Column, DateTime, MetaData, text, UniqueConstraint
from sqlalchemy.dialects.postgresql import (
    BIGINT,
    DOUBLE_PRECISION,
    INTEGER,
    TEXT,
    TIMESTAMP,
)
from sqlalchemy.ext.declarative import (
    AbstractConcreteBase,
    declarative_base,
    declared_attr,
)

from contessa.base_rules import Rule
from contessa.session import make_session

DQBase = declarative_base(metadata=MetaData(schema="data_quality"))


class QualityCheck(AbstractConcreteBase, DQBase):
    """
    Representation of abstract quality check table.
    """

    __abstract__ = True

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

    @declared_attr
    def __table_args__(cls):
        """
        Concrete classes derived from this abstract one should have unique check among the columns
        that below. But the constraint needs to have unique name, therefore we are using
        @declared_attr here to construct name of the constraint using its table name.
        :return:
        """
        return (
            UniqueConstraint(
                "attribute",
                "rule_name",
                "task_ts",
                "time_filter",
                name=f"{cls.__tablename__}_unique_quality_check",
            ),
        )

    def init_row(self, rule: Rule, results: pd.Series, task_time: datetime = None):
        """
        Count metrics we want to measure using pd.Series api and set them to quality check object.
        """
        if results.isnull().any():
            raise ValueError("In results of rule.apply can't be any Null values.")

        self.task_ts = task_time or datetime.now()
        self.attribute = rule.attribute
        self.rule_name = rule.name
        self.rule_description = rule.description

        self.total_records = results.shape[0]
        self.failed = results[results == False].shape[0]
        self.passed = results[results == True].shape[0]

        self.set_medians()

        self.time_filter = rule.time_filter
        self.failed_percentage = self._perc(self.failed, self.total_records)
        self.passed_percentage = self._perc(self.passed, self.total_records)
        self.status = "invalid" if self.failed > 0 else "valid"

    def _perc(self, a, b):
        res = 0
        try:
            res = (a / b) * 100
        except ZeroDivisionError:
            pass
        return res

    def set_medians(self, days=30):
        """
        Calculate median of passed/failed quality checks from last 30 days.
        :param days: int
        """
        now = datetime.today().date()
        past = now - timedelta(days=days)
        cls = self.__class__

        session = make_session()
        checks = (
            session.query(cls.failed, cls.passed)
            .filter(and_(cls.task_ts <= str(now), cls.task_ts >= str(past)))
            .all()
        )
        session.expunge_all()
        session.commit()
        session.close()

        failed = [ch.failed for ch in checks]
        self.median_30_day_failed = median(failed) if failed else None

        passed = [ch.passed for ch in checks]
        self.median_30_day_passed = median(passed) if passed else None

    def __repr__(self):
        return f"Rule ({self.attribute} - {self.rule_name} - {self.task_ts})"


def get_default_qc_class(table):
    """
    This will construct type/class (not object) that will have special name that its prefixed
    with its table. It's better because sqlalchemy can yell somethings if we would use same class
    for generic quality checks.
    Basically, its somethings as:

        class MyTableQualityCheck(QualityCheck):
            id = ...
            ...

    But it has dynamic name - MyTable is replaced for the table we are doing quality check for.
    :param table: str, name of table we are doing quality check for
    :return: class with dynamically created name
    """
    attributedict = {
        "__tablename__": f"quality_check_{table}",
        "id": Column(BIGINT, primary_key=True),
        "__mapper_args__": {"polymorphic_identity": f"{table}", "concrete": True},
    }
    cls = type(f"{table.capitalize()}QualityCheck", (QualityCheck,), attributedict)
    return cls
