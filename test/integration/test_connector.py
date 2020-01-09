from contessa.db import Connector
from contessa.models import DQBase


def test_upsert_qualitycheck(conn: Connector):
    from sqlalchemy import Column, DateTime, text, UniqueConstraint
    from sqlalchemy.dialects.postgresql import TEXT, INTEGER, BIGINT

    class A(DQBase):
        id = Column(BIGINT, primary_key=True)
        name = Column(TEXT, nullable=False)
        price = Column(INTEGER)
        created_at = Column(
            DateTime(timezone=True),
            server_default=text("NOW()"),
            nullable=False,
            index=True,
        )

        __tablename__ = "my_table"
        __table_args__ = (UniqueConstraint("name", name=f"unique_constraint_test",),)

    conn.ensure_table(A.__table__)
    instance = A(name="hello", price=13)

    conn.upsert(
        objs=[instance,]
    )

    # check if inserted
    s = conn.make_session()
    row = s.query(A.__table__).all()
    s.expunge_all()
    s.commit()
    assert len(row) == 1
    assert row[0].price == 13

    # change data and insert again - should upsert
    instance.price = 42
    conn.upsert(
        objs=[instance,]
    )

    row = s.query(A.__table__).all()
    s.expunge_all()
    s.commit()
    assert len(row) == 1
    assert row[0].price == 42

    s.close()
