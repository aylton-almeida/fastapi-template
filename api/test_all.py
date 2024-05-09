import os
import time
from typing import List, Optional

import alembic.config
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker, selectinload
from starlette.testclient import TestClient

from api.main import app
from api.repository import SQL_BASE, SQLTodoRepository, Todo, TodoFilter, TodoRepository


class InMemoryTodoRepository:  # In-memory implementation of interface
    def __init__(self):
        self.data = {}

    async def save(self, todo: Todo) -> None:
        self.data[todo.key] = todo

    async def get_by_key(self, key: str) -> Optional[Todo]:
        return self.data.get(key)

    async def get(self, todo_filter: TodoFilter) -> List[Todo]:
        all_matching_todos = filter(
            lambda todo: (not todo_filter.key_contains or todo_filter.key_contains in todo.key)
            and (not todo_filter.value_contains or todo_filter.value_contains in todo.value)
            and (not todo_filter.done or todo_filter.done == todo.done),
            self.data.values(),
        )

        return list(all_matching_todos)[: todo_filter.limit]


@pytest.fixture
def fake_todo_repository():
    return InMemoryTodoRepository()


@pytest.fixture(scope="function", autouse=True)
async def todo_repository():
    time.sleep(1)
    alembicArgs = ["--raiseerr", "upgrade", "head"]
    alembic.config.main(argv=alembicArgs)

    engine = create_async_engine(os.getenv("DB_STRING", ""))

    async with engine.begin() as conn:
        yield SQLTodoRepository(conn)

        await conn.execute(text(";".join([f"TRUNCATE TABLE {t} CASCADE" for t in SQL_BASE.metadata.tables.keys()])))
        await conn.commit()


@pytest.mark.unit
def test_example_unit_test():
    assert 1 != 0


@pytest.mark.asyncio
@pytest.mark.integration
async def test_contract_test(fake_todo_repository: TodoRepository, todo_repository: TodoRepository):
    """See https://martinfowler.com/bliki/ContractTest.html"""

    todo = Todo(key="testkey", value="testvalue")

    print(f"fake_todo_repository {fake_todo_repository}")
    print(f"todo_repository {todo_repository}")

    for repo in [fake_todo_repository, todo_repository]:
        await repo.save(todo)

        new_todo = await repo.get_by_key("testkey")
        assert new_todo and new_todo.value == "testvalue"

        assert len(await repo.get(TodoFilter(key_contains="e"))) == 1
        assert len(await repo.get(TodoFilter(key_contains="e", limit=0))) == 0
        assert len(await repo.get(TodoFilter(key_contains="v"))) == 0

        assert len(await repo.get(TodoFilter(value_contains="v"))) == 1
        assert len(await repo.get(TodoFilter(value_contains="e", limit=0))) == 0
        assert len(await repo.get(TodoFilter(value_contains="k"))) == 0


@pytest.mark.asyncio
@pytest.mark.integration
async def test_repository(todo_repository: SQLTodoRepository):
    async with todo_repository as r:
        await r.save(Todo(key="testkey", value="testvalue"))

    todo = await r.get_by_key("testkey")
    assert todo is not None
    assert todo.value == "testvalue"

    with pytest.raises(IntegrityError):
        async with todo_repository as r:
            await r.save(Todo(key="testkey", value="not allowed: unique todo keys!"))

    with pytest.raises(DBAPIError):
        async with todo_repository as r:
            await r.save(Todo(key="too long", value=129 * "x"))


@pytest.mark.asyncio
@pytest.mark.integration
async def test_repository_filter(todo_repository: SQLTodoRepository):
    async with todo_repository as repo:
        await repo.save(Todo(key="testkey", value="testvalue"))
        await repo.save(Todo(key="abcde", value="v"))

    todos = await repo.get(TodoFilter(key_contains="test"))
    assert len(todos) == 1
    assert todos[0].value == "testvalue"

    todos = await repo.get(TodoFilter(key_contains="abcde"))
    assert len(todos) == 1
    assert todos[0].value == "v"

    assert len(await repo.get(TodoFilter(key_contains="e"))) == 2
    assert len(await repo.get(TodoFilter(key_contains="e", limit=1))) == 1
    assert len(await repo.get(TodoFilter(value_contains="v"))) == 2
    assert len(await repo.get(TodoFilter(done=True))) == 0


@pytest.mark.integration
def test_api():
    time.sleep(1)
    client = TestClient(app)
    response = client.post("/create/testkey?value=testvalue")

    assert response.status_code == 201
    response = client.get("/get/testkey")

    assert response.status_code == 200
    assert response.json() == {"key": "testkey", "value": "testvalue", "done": False}

    response = client.get("/get/wrong")
    assert response.status_code == 404
