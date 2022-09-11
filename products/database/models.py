import datetime
import enum
import logging
import uuid
from typing import List, Optional, Tuple

from sqlalchemy import Column, String, DateTime, ForeignKey, Integer, event, \
    select, update
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Connection
from sqlalchemy.orm import relationship, backref
from sqlalchemy_utils import ChoiceType

from database.base import Base
from database.engine import Session

logger = logging.getLogger('uvicorn')


class UnitType(str, enum.Enum):
    FOLDER = 'FOLDER'
    FILE = 'FILE'


class Unit(Base):
    __tablename__ = "unit"
    id = Column(String, primary_key = True, nullable= False)
    url = Column(String, nullable=True)
    date = Column(DateTime(timezone=datetime.timezone.utc), nullable=False)
    type = Column(ChoiceType(UnitType, impl=String()), nullable=False)
    parent_id = Column(String, ForeignKey('unit.id'),
                       index=True, default=None,
                       nullable=True)
    size = Column(Integer, nullable=True)

    children: List["Unit"] = relationship(
        "Unit",
        backref=backref('parent', remote_side='Unit.id'),
        uselist=True, cascade="all, delete"
    )

    def get_child(self, index: int = 0) -> Optional["Unit"]:
        if len(self.children) > index:
            return self.children[index]
        return None

    def __str__(self):
        return f'{self.name} {self.type}'

    def __repr__(self):
        return f'<Unit {self.name}>'

# для таблицы истории по элементу

class HistoryUnit(Base):
    __tablename__ = "history_unit"
    self_id = Column(Integer, primary_key = True, autoincrement = True)
    id = Column(String, ForeignKey('unit.id', ondelete='CASCADE'), nullable = False)
    url = Column(String, nullable=True)
    date = Column(DateTime(timezone=datetime.timezone.utc), primary_key= True, nullable=False)
    type = Column(ChoiceType(UnitType, impl=String()), nullable=False)
    parent_id = Column(String, default=None, nullable=True)
    size = Column(Integer, nullable=True)


    def __str__(self):
        return f'{self.name} {self.type}'

    def __repr__(self):
        return f'<HistoryUnit {self.name}>'


@event.listens_for(Unit, 'after_insert')
def do_something(mapper, connection: Connection, target):
    if target.parent_id is not None:
        session = Session()
        parent = session.query(Unit).filter_by(id=target.parent_id).one()
        parent.date = target.date
        session.add(parent)
        session.commit()


@event.listens_for(Unit, 'after_update')
def do_something(mapper, connection: Connection, target: Unit):
    if target.parent_id is not None:
        session = Session()
        parent = session.query(Unit).filter_by(id=target.parent_id).one()
        parent.date = target.date
        session.add(parent)
        session.commit()



