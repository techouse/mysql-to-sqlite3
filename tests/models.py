from os import environ

from sqlalchemy import (
    Table,
    Column,
    ForeignKey,
    BigInteger,
    LargeBinary,
    Boolean,
    CHAR,
    Date,
    DateTime,
    DECIMAL,
    Float,
    Integer,
    JSON,
    NCHAR,
    Numeric,
    Unicode,
    REAL,
    SmallInteger,
    String,
    Text,
    Time,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
)
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, MEDIUMINT, SMALLINT, TINYINT
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class Author(Base):
    __tablename__ = "authors"
    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False, index=True)
    dupe = Column(Boolean, index=True, default=False)

    def __repr__(self):
        return "<Author(id='{id}', name='{name}')>".format(id=self.id, name=self.name)


article_authors = Table(
    "article_authors",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("author_id", Integer, ForeignKey("authors.id"), primary_key=True),
)


class Image(Base):
    __tablename__ = "images"
    id = Column(Integer, primary_key=True)
    path = Column(String(255), index=True)
    description = Column(String(255), nullable=True)
    dupe = Column(Boolean, index=True, default=False)

    def __repr__(self):
        return "<Image(id='{id}', path='{path}')>".format(id=self.id, path=self.path)


article_images = Table(
    "article_images",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("image_id", Integer, ForeignKey("images.id"), primary_key=True),
)


class Tag(Base):
    __tablename__ = "tags"
    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False, index=True)
    dupe = Column(Boolean, index=True, default=False)

    def __repr__(self):
        return "<Tag(id='{id}', name='{name}')>".format(id=self.id, name=self.name)


article_tags = Table(
    "article_tags",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tags.id"), primary_key=True),
)


class Misc(Base):
    """This model contains all possible MySQL types"""

    __tablename__ = "misc"
    id = Column(Integer, primary_key=True)
    big_integer_field = Column(BigInteger, default=0)
    big_integer_unsigned_field = Column(BIGINT(unsigned=True), default=0)
    large_binary_field = Column(LargeBinary, nullable=True)
    boolean_field = Column(Boolean, default=False)
    char_field = Column(CHAR(255), nullable=True)
    date_field = Column(Date, nullable=True)
    date_time_field = Column(DateTime, nullable=True)
    decimal_field = Column(DECIMAL(10, 2), nullable=True)
    float_field = Column(Float(12, 4), default=0)
    integer_field = Column(Integer, default=0)
    integer_unsigned_field = Column(INTEGER(unsigned=True), default=0)
    tinyint_field = Column(TINYINT, default=0)
    tinyint_unsigned_field = Column(TINYINT(unsigned=True), default=0)
    mediumint_field = Column(MEDIUMINT, default=0)
    mediumint_unsigned_field = Column(MEDIUMINT(unsigned=True), default=0)
    if environ.get("LEGACY_DB", "0") == "0":
        json_field = Column(JSON, nullable=True)
    nchar_field = Column(NCHAR(255), nullable=True)
    numeric_field = Column(Numeric(12, 4), default=0)
    unicode_field = Column(Unicode(255), nullable=True)
    real_field = Column(REAL(12, 4), default=0)
    small_integer_field = Column(SmallInteger, default=0)
    small_integer_unsigned_field = Column(SMALLINT(unsigned=True), default=0)
    string_field = Column(String(255), nullable=True)
    text_field = Column(Text, nullable=True)
    time_field = Column(Time, nullable=True)
    varbinary_field = Column(VARBINARY(255), nullable=True)
    varchar_field = Column(VARCHAR(255), nullable=True)
    timestamp_field = Column(TIMESTAMP, default=current_timestamp())
    dupe = Column(Boolean, index=True, default=False)


article_misc = Table(
    "article_misc",
    Base.metadata,
    Column("article_id", Integer, ForeignKey("articles.id"), primary_key=True),
    Column("misc_id", Integer, ForeignKey("misc.id"), primary_key=True),
)


class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer, primary_key=True)
    hash = Column(String(32), unique=True)
    slug = Column(String(255), index=True)
    title = Column(String(255), index=True)
    content = Column(Text, nullable=True)
    status = Column(CHAR(1), index=True)
    published = Column(DateTime, nullable=True)
    dupe = Column(Boolean, index=True, default=False)
    # relationships
    authors = relationship(
        "Author",
        secondary=article_authors,
        backref=backref("authors", lazy="dynamic"),
        lazy="dynamic",
    )
    tags = relationship(
        "Tag",
        secondary=article_tags,
        backref=backref("tags", lazy="dynamic"),
        lazy="dynamic",
    )
    images = relationship(
        "Image",
        secondary=article_images,
        backref=backref("images", lazy="dynamic"),
        lazy="dynamic",
    )
    misc = relationship(
        "Misc",
        secondary=article_misc,
        backref=backref("misc", lazy="dynamic"),
        lazy="dynamic",
    )

    def __repr__(self):
        return "<Article(id='{id}', title='{title}')>".format(
            id=self.id, title=self.title
        )


class CrazyName(Base):
    __tablename__ = "crazy_name."
    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False, index=True)
    dupe = Column(Boolean, index=True, default=False)

    def __repr__(self):
        return "<CrazyName(id='{id}', name='{name}')>".format(
            id=self.id, name=self.name
        )
