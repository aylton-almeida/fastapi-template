import os
import time
import asyncio
from typing import List, Optional

import alembic.config
import pytest
from sqlalchemy import create_engine, Column, String, Boolean, ForeignKey, func
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Mapped, mapped_column
from starlette.testclient import TestClient

from api.main import app
from api.repository import (
    SQL_BASE,
    InMemoryTodoRepository,
    TodoFilter,
    get_engine,
)


Base = declarative_base()


class Todo(Base):
    __tablename__ = "todos"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(String)
    done: Mapped[bool] = mapped_column(Boolean, default=False)


class TodoRepository:
    async def save(self, todo: Todo) -> None:
        raise NotImplementedError

    async def get_by_key(self, key: str) -> Optional[Todo]:
        raise NotImplementedError

    async def get(self, filter: TodoFilter) -> List[Todo]:
        raise NotImplementedError


class SQLTodoRepository(TodoRepository):
    def __init__(self, session: AsyncSession):
        self.session = session

    async def save(self, todo: Todo) -> None:
        self.session.add(todo)
        await self.session.commit()

    async def get_by_key(self, key: str) -> Optional[Todo]:
        result = await self.session.execute(select(Todo).filter_by(key=key))
        return result.scalars().first()

    async def get(self, filter: TodoFilter) -> List[Todo]:
        query = select(Todo)
        if filter.key_contains:
            query = query.filter(Todo.key.contains(filter.key_contains))
        if filter.value_contains:
            query = query.filter(Todo.value.contains(filter.value_contains))
        if filter.done is not None:
            query = query.filter(Todo.done == filter.done)
        if filter.limit:
            query = query.limit(filter.limit)
        result = await self.session.execute(query)
        return result.scalars().all()


@pytest.fixture
async def fake_todo_repository():
    return InMemoryTodoRepository()


@pytest.fixture
async def todo_repository():
    time.sleep(1)
    alembicArgs = ["--raiseerr", "upgrade", "head"]
    alembic.config.main(argv=alembicArgs)

    engine = create_async_engine(os.getenv("DB_STRING"))
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        yield SQLTodoRepository(session)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


@pytest.mark.unit
def test_example_unit_test():
    assert 1 != 0


@pytest.mark.integration
async def test_contract_test(fake_todo_repository: TodoRepository, todo_repository: TodoRepository):
    """See https://martinfowler.com/bliki/ContractTest.html"""

    todo = Todo(key="testkey", value="testvalue")

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


@pytest.mark.integration
async def test_repository(todo_repository: SQLTodoRepository):
    await todo_repository.save(Todo(key="testkey", value="testvalue"))

    todo = await todo_repository.get_by_key("testkey")
    assert todo.value == "testvalue"

    with pytest.raises(IntegrityError):
        await todo_repository.save(Todo(key="testkey", value="not allowed: unique todo keys!"))

    with pytest.raises(DataError):
        await todo_repository.save(Todo(key="too long", value=129 * "x"))


@pytest.mark.integration
async def test_repository_filter(todo_repository: SQLTodoRepository):
    await todo_repository.save(Todo(key="testkey", value="testvalue"))
    await todo_repository.save(Todo(key="abcde", value="v"))

    todos = await todo_repository.get(TodoFilter(key_contains="test"))
    assert len(todos) == 1
    assert todos[0].value == "testvalue"

    todos = await todo_repository.get(TodoFilter(key_contains="abcde"))
    assert len(todos) == 1
    assert todos[0].value == "v"

    assert len(await todo_repository.get(TodoFilter(key_contains="e"))) == 2
    assert len(await todo_repository.get(TodoFilter(key_contains="e", limit=1))) == 1
    assert len(await todo_repository.get(TodoFilter(value_contains="v"))) == 2
    assert len(await todo_repository.get(TodoFilter(done=True))) == 0


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

