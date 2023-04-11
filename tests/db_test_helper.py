import sqlalchemy
from mara_db import dbs


def db_is_responsive(db: dbs.DB) -> bool:
    """Returns True when the DB is available on the given port, otherwise False"""
    engine = sqlalchemy.create_engine(db.sqlalchemy_url, pool_pre_ping=True)

    try:
        with engine.connect() as conn:
            return True
    except:
        return False


def db_replace_placeholders(db: dbs.DB, docker_ip: str, docker_port: int, database: str = None) -> dbs.DB:
    """Replaces the internal placeholders with the docker ip and docker port"""
    if db.host == 'DOCKER_IP':
        db.host = docker_ip
    if db.port == -1:
        db.port = docker_port
    if database:
        db.database = database
    return db
