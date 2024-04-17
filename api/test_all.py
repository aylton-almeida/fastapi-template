import os
import time

import alembic.config
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

from api.main import app
from api.repository import SQL_BASE, SQLTodoRepository


@pytest.fixture
def todo_repository():
    time.sleep(1)
    alembicArgs = ["--raiseerr", "upgrade", "head"]
    alembic.config.main(argv=alembicArgs)

    engine = create_engine(os.getenv("DB_STRING", "").replace("+asyncpg", ""), pool_pre_ping=True)
    session = sessionmaker(bind=engine)()

    yield SQLTodoRepository(session)

    session.close()

    sessionmaker(bind=engine)().execute(
        text(";".join([f"TRUNCATE TABLE {t} CASCADE" for t in SQL_BASE.metadata.tables.keys()]))
    )


@pytest.mark.integration
@pytest.mark.usefixtures("todo_repository")
def test_api_create():
    time.sleep(1)
    client = TestClient(app)
    response = client.post("/create/testkey?value=testvalue")

    assert response.status_code == 201


@pytest.mark.integration
@pytest.mark.usefixtures("todo_repository")
def test_api_get_right():
    time.sleep(1)
    client = TestClient(app)
    response = client.post("/create/testkey?value=testvalue")

    response = client.get("/get/testkey")

    assert response.status_code == 200
    assert response.json() == {"key": "testkey", "value": "testvalue", "done": False}


@pytest.mark.integration
@pytest.mark.usefixtures("todo_repository")
def test_api_wrong():
    time.sleep(1)
    client = TestClient(app)
    response = client.post("/create/testkey?value=testvalue")

    response = client.get("/get/wrong")
    assert response.status_code == 404
