from sqlalchemy.orm import sessionmaker

Session = None


def init_session(engine):
    """
    Use to init session class.
    """
    global Session
    Session = sessionmaker(bind=engine)


def make_session():
    """
    Make session instance that can be used to execute queries.
    `init_session` needs to be called first.
    :return: Session
    """
    global Session
    if not Session:
        raise ValueError("Cant make session, call `init_session` first.")
    return Session()
