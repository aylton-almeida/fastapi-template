from __future__ import annotations

import os
from functools import lru_cache
from typing import Iterator, List, Optional

from pydantic import BaseModel
from sqlalchemy import Boolean, Column, Integer, String, select
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import Mapped, mapped_column, Session, declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import NullPool

SQL_BASE = declarative_base()


@lru_cache(maxsize=None)
def get_engine(db_string: str):
    return create_async_engine(db_string, pool_pre_ping=True, poolclass=NullPool)


class TodoInDB(SQL_BASE):  # type: ignore
    __tablename__ = "todo"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(length=128), nullable=False, unique=True)
    value: Mapped[str] = mapped_column(String(length=128), nullable=False)
    done: Mapped[bool] = mapped_column(Boolean, default=False)


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


class SQLTodoRepository(TodoRepository):  # SQL Implementation of interface
    def __init__(self, session):
        self._session: Session = session

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
        result = await self._session.execute(
            select(TodoInDB).where(TodoInDB.key == key)
        )
        instance = result.scalars().first()

        if instance:
            return Todo(key=instance.key, value=instance.value, done=instance.done)

        return None

    async def get(self, todo_filter: TodoFilter) -> List[Todo]:
        stmt = select(TodoInDB)

        if todo_filter.key_contains is not None:
            stmt = stmt.where(TodoInDB.key.contains(todo_filter.key_contains))

        if todo_filter.value_contains is not None:
            stmt = stmt.where(TodoInDB.value.contains(todo_filter.value_contains))

        if todo_filter.done is not None:
            stmt = stmt.where(TodoInDB.done == todo_filter.done)

        if todo_filter.limit is not None:
            stmt = stmt.limit(todo_filter.limit)

        result = await self._session.execute(stmt)

        return [Todo(key=todo.key, value=todo.value, done=todo.done) for todo in result.scalars()]


async def create_todo_repository() -> Iterator[TodoRepository]:
    async with AsyncSession(get_engine(os.getenv("DB_STRING"))) as session:
        todo_repository = SQLTodoRepository(session)

        try:
            yield todo_repository
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

