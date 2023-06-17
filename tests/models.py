import typing as t
from datetime import date, datetime, time
from decimal import Decimal
from os import environ

from sqlalchemy import (
    CHAR,
    DECIMAL,
    JSON,
    NCHAR,
    REAL,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
    BigInteger,
    Column,
    ForeignKey,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    Unicode,
)
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, MEDIUMINT, SMALLINT, TINYINT
from sqlalchemy.orm import DeclarativeBase, Mapped, backref, mapped_column, relationship
from sqlalchemy.sql.functions import current_timestamp


class Base(DeclarativeBase):
    pass


class Author(Base):
    __tablename__ = "authors"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    dupe: Mapped[bool] = mapped_column(index=True, default=False)

    def __repr__(self):
        return f"<Author(id='{self.id}', name='{self.name}')>"


article_authors = Table(
    "article_authors",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("author_id", Integer, ForeignKey("authors.id"), primary_key=True),
)


class Image(Base):
    __tablename__ = "images"
    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[str] = mapped_column(String(255), nullable=True)
    dupe: Mapped[bool] = mapped_column(index=True, default=False)

    def __repr__(self):
        return f"<Image(id='{self.id}', path='{self.path}')>"


article_images = Table(
    "article_images",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("image_id", Integer, ForeignKey("images.id"), primary_key=True),
)


class Tag(Base):
    __tablename__ = "tags"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    dupe: Mapped[bool] = mapped_column(index=True, default=False)

    def __repr__(self):
        return f"<Tag(id='{self.id}', name='{self.name}')>"


article_tags = Table(
    "article_tags",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id"), primary_key=True),
)


class Misc(Base):
    """This model contains all possible MySQL types"""

    __tablename__ = "misc"
    id: Mapped[int] = mapped_column(primary_key=True)
    big_integer_field: Mapped[int] = mapped_column(BigInteger, default=0)
    big_integer_unsigned_field: Mapped[int] = mapped_column(BIGINT(unsigned=True), default=0)
    if environ.get("LEGACY_DB", "0") == "0":
        large_binary_field: Mapped[bytes] = mapped_column(LargeBinary, nullable=True, default=b"Lorem ipsum dolor")
    else:
        large_binary_field = mapped_column(LargeBinary, nullable=True)
    boolean_field: Mapped[bool] = mapped_column(default=False)
    char_field: Mapped[str] = mapped_column(CHAR(255), nullable=True)
    date_field: Mapped[date] = mapped_column(nullable=True)
    date_time_field: Mapped[datetime] = mapped_column(nullable=True)
    decimal_field: Mapped[Decimal] = mapped_column(DECIMAL(10, 2), nullable=True)
    float_field: Mapped[Decimal] = mapped_column(DECIMAL(12, 4), default=0)
    integer_field: Mapped[int] = mapped_column(default=0)
    integer_unsigned_field: Mapped[int] = mapped_column(INTEGER(unsigned=True), default=0)
    tinyint_field: Mapped[int] = mapped_column(TINYINT, default=0)
    tinyint_unsigned_field: Mapped[int] = mapped_column(TINYINT(unsigned=True), default=0)
    mediumint_field: Mapped[int] = mapped_column(MEDIUMINT, default=0)
    mediumint_unsigned_field: Mapped[int] = mapped_column(MEDIUMINT(unsigned=True), default=0)
    if environ.get("LEGACY_DB", "0") == "0":
        json_field: Mapped[t.Mapping[str, t.Any]] = mapped_column(JSON, nullable=True)
    nchar_field: Mapped[str] = mapped_column(NCHAR(255), nullable=True)
    numeric_field: Mapped[float] = mapped_column(Numeric(12, 4), default=0)
    unicode_field: Mapped[str] = mapped_column(Unicode(255), nullable=True)
    real_field: Mapped[float] = mapped_column(REAL(12), default=0)
    small_integer_field: Mapped[int] = mapped_column(SmallInteger, default=0)
    small_integer_unsigned_field: Mapped[int] = mapped_column(SMALLINT(unsigned=True), default=0)
    string_field: Mapped[str] = mapped_column(String(255), nullable=True)
    text_field: Mapped[str] = mapped_column(Text, nullable=True)
    time_field: Mapped[time] = mapped_column(Time, nullable=True)
    varbinary_field: Mapped[bytes] = mapped_column(VARBINARY(255), nullable=True)
    varchar_field: Mapped[str] = mapped_column(VARCHAR(255), nullable=True)
    timestamp_field: Mapped[datetime] = mapped_column(TIMESTAMP, default=current_timestamp())
    dupe: Mapped[bool] = mapped_column(index=True, default=False)


article_misc = Table(
    "article_misc",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("misc_id", Integer, ForeignKey("misc.id"), primary_key=True),
)


class Article(Base):
    __tablename__ = "articles"
    id: Mapped[int] = mapped_column(primary_key=True)
    hash: Mapped[str] = mapped_column(String(32), unique=True)
    slug: Mapped[str] = mapped_column(String(255), index=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    content: Mapped[str] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(CHAR(1), index=True)
    published: Mapped[datetime] = mapped_column(nullable=True)
    dupe: Mapped[bool] = mapped_column(index=True, default=False)
    # relationships
    authors: Mapped[t.List[Author]] = relationship(
        "Author",
        secondary=article_authors,
        backref=backref("authors", lazy="dynamic"),
        lazy="dynamic",
    )
    tags: Mapped[t.List[Tag]] = relationship(
        "Tag",
        secondary=article_tags,
        backref=backref("tags", lazy="dynamic"),
        lazy="dynamic",
    )
    images: Mapped[t.List[Image]] = relationship(
        "Image",
        secondary=article_images,
        backref=backref("images", lazy="dynamic"),
        lazy="dynamic",
    )
    misc: Mapped[t.List[Misc]] = relationship(
        "Misc",
        secondary=article_misc,
        backref=backref("misc", lazy="dynamic"),
        lazy="dynamic",
    )

    def __repr__(self):
        return f"<Article(id='{self.id}', title='{self.title}')>"


class CrazyName(Base):
    __tablename__ = "crazy_name."
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    dupe: Mapped[bool] = mapped_column(index=True, default=False)

    def __repr__(self):
        return f"<CrazyName(id='{self.id}', name='{self.name}')>"
