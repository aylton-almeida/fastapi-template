import os
from functools import lru_cache
from typing import Iterator, List, Optional
from contextlib import asynccontextmanager

from pydantic import BaseModel
from sqlalchemy import Boolean, Column, Integer, String, create_async_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_session
from sqlalchemy.orm import declarative_base, sessionmaker

SQL_BASE = declarative_base()


@lru_cache(maxsize=None)
def get_engine(db_string: str):
    return create_async_engine(db_string, pool_pre_ping=True)


class TodoInDB(SQL_BASE):  # type: ignore
    __tablename__ = "todo"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(length=128), nullable=False, unique=True)
    value = Column(String(length=128), nullable=False)
    done = Column(Boolean, default=False)


class Todo(BaseModel):
    key: str
    value: str
    done: bool = False


class TodoFilter(BaseModel):
    limit: Optional[int] = None
    key_contains: Optional[str] = None
    value_contains: Optional[str] = None
    done: Optional[bool] = None


class TodoRepository:  # Interface
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def save(self, todo: Todo) -> None:
        raise NotImplementedError()

    async def get_by_key(self, key: str) -> Optional[Todo]:
        raise NotImplementedError()

    async def get(self, todo_filter: TodoFilter) -> List[Todo]:
        raise NotImplementedError()


class InMemoryTodoRepository(TodoRepository):  # In-memory implementation of interface
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


class SQLTodoRepository(TodoRepository):  # SQL Implementation of interface
    def __init__(self, session):
        self._session: AsyncSession = session

    async def __aexit__(self, exc_type, exc_value: str, exc_traceback: str) -> None:
        if any([exc_type, exc_value, exc_traceback]):
            await self._session.rollback()
            return

        try:
            await self._session.commit()
        except DatabaseError as e:
            await self._session.rollback()
            raise e

    async def save(self, todo: Todo) -> None:
        self._session.add(TodoInDB(key=todo.key, value=todo.value))

    async def get_by_key(self, key: str) -> Optional[Todo]:
        instance = await self._session.query(TodoInDB).filter(TodoInDB.key == key).first()

        if instance:
            return Todo(key=instance.key, value=instance.value, done=instance.done)

        return None

    async def get(self, todo_filter: TodoFilter) -> List[Todo]:
        query = self._session.query(TodoInDB)

        if todo_filter.key_contains is not None:
            query = query.filter(TodoInDB.key.contains(todo_filter.key_contains))

        if todo_filter.value_contains is not None:
            query = query.filter(TodoInDB.value.contains(todo_filter.value_contains))

        if todo_filter.done is not None:
            query = query.filter(TodoInDB.done == todo_filter.done)

        if todo_filter.limit is not None:
            query = query.limit(todo_filter.limit)

        return [Todo(key=todo.key, value=todo.value, done=todo.done) for todo in query]


@asynccontextmanager
async def create_todo_repository() -> Iterator[TodoRepository]:
    async with create_async_session(get_engine(os.getenv("DB_STRING"))) as session:
        todo_repository = SQLTodoRepository(session)

        try:
            yield todo_repository
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

