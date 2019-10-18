from datetime import datetime, timedelta
from statistics import median
from typing import Dict

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
from contessa.db import Connector

# default schema for results is `data_quality`, but it can be overridden by passing
# ResultTable to runner. constructing concrete model for QualityCheck (that's abstract) will
# swap the schemas before creation - see `create_default_quality_check_class`
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

    def init_row(
        self, rule: Rule, results: pd.Series, conn: Connector, context: Dict = None
    ):
        """
        Count metrics we want to measure using pd.Series api and set them to quality check object.
        """
        if results.isnull().any():
            raise ValueError("In results of rule.apply can't be any Null values.")

        # todo - add to doc
        self.task_ts = context["task_ts"]
        self.attribute = rule.attribute
        self.rule_name = rule.name
        self.rule_description = rule.description

        self.total_records = results.shape[0]
        self.failed = results[results == False].shape[0]
        self.passed = results[results == True].shape[0]

        self.set_medians(conn)

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

    def set_medians(self, conn: Connector, days=30):
        """
        Calculate median of passed/failed quality checks from last 30 days.
        """
        now = datetime.today().date()
        past = now - timedelta(days=days)
        cls = self.__class__

        session = conn.make_session()
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


class ConsistencyCheck(AbstractConcreteBase, DQBase):
    """
    Representation of abstract consistency check table.
    """

    __abstract__ = True

    id = Column(BIGINT, primary_key=True)
    type = Column(TEXT)
    check_name = Column(TEXT)
    check_description = Column(TEXT)
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
                "type",
                "check_name",
                "task_ts",
                "time_filter",
                name=f"{cls.__tablename__}_unique_consistency_check",
            ),
        )

    def init_row(
        self,
        check: Dict,
        status: str,
        left_table_name: str,
        right_table_name: str,
        context: Dict = None,
    ):
        """
        Set result to consistency check object.
        """
        # todo - add to doc
        self.task_ts = context["task_ts"]
        self.check_name = check["name"]
        self.check_description = check["description"]
        self.left_table = left_table_name
        self.right_table = right_table_name
        self.status = status

    def __repr__(self):
        return f"Rule ({self.type} - {self.check_name} - {self.task_ts})"


# todo - maybe create also CheckTable
class Table:
    def __init__(self, schema_name, table_name):
        self.schema_name = schema_name
        self.table_name = table_name

    @property
    def fullname(self):
        # todo - solve sanitization. problem - can be already sanitized
        return f"{self.schema_name}.{self.table_name}"


class ResultTable(Table):
    """
    Name will be constructed as "quality_check_{load_table_name}", e.g. "quality_check_my_table".
    Can be overridden by `table_name`.
    """

    def __init__(self, schema_name, table_name, use_prefix=True):
        table_name = f"quality_check_{table_name}" if use_prefix else table_name
        super().__init__(schema_name, table_name)

    def to_camel_case(self, snake_str):
        components = snake_str.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    @property
    def clsname(self):
        """
        Construct a name for dynamic creation of cls for quality table.
        Should be unique in runtime.
        """
        name = super().fullname.replace(".", "_")
        camel_case = self.to_camel_case(name)
        return camel_case[0].title() + camel_case[1:]


def create_default_check_class(result_table: ResultTable, check_type: str = "quality"):
    """
    This will construct type/class (not object) that will have special name that its prefixed
    with its table. It's better because sqlalchemy can yell somethings if we would use same class
    for generic quality checks.
    Basically, its somethings as:

        class MyTableQualityCheck(QualityCheck):
            id = ...
            ...

    But it has dynamic name - MyTable is replaced for the table we are doing quality check for.
    :return: class with dynamically created name
    """
    attributedict = {
        "__tablename__": result_table.table_name,
        "id": Column(BIGINT, primary_key=True),
        "__mapper_args__": {
            "polymorphic_identity": f"{result_table.table_name}",
            "concrete": True,
        },
    }
    if check_type == "quality":
        check_class = QualityCheck
    else:
        check_class = ConsistencyCheck
    cls = type(result_table.clsname, (check_class,), attributedict)
    cls.metadata.schema = result_table.schema_name
    cls.__table__.schema = result_table.schema_name
    return cls
