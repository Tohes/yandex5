from datetime import datetime, timezone
from typing import Optional, List
from uuid import UUID
from datetime import datetime, timezone

from pydantic import BaseModel, Field, validator, root_validator
from loguru import logger
from database.models import UnitType


def convert_datetime_to_iso_8601_with_z_suffix(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec='seconds', ).replace('+00:00', 'Z')


class HistoryRequest(BaseModel):
    dateStart: Optional[datetime]
    dateEnd: Optional[datetime]


class UnitBaseSchema(BaseModel):
    id: str
    url: Optional[str] = None
    parent_id: Optional[str] = Field(alias='parentId')
    size: Optional[int]
    type: UnitType
    date: Optional[datetime]

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
        orm_mode = True
        allow_population_by_field_name = True

        json_encoders = {
            datetime: convert_datetime_to_iso_8601_with_z_suffix
        }

class HistoryBaseSchema(BaseModel):
    id: str
    url: Optional[str]
    parent_id: Optional[str] = Field(alias='parentId')
    size: Optional[int]
    type: UnitType
    date: Optional[datetime]

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
        orm_mode = True
        allow_population_by_field_name = True

    @validator("date")
    def date_conversion(cls, v):
        return convert_datetime_to_iso_8601_with_z_suffix(v)





class HistoryResponseSchema(BaseModel):
    items: List[HistoryBaseSchema]

class TestSchema(BaseModel):
    DateStart: datetime
    DateEnd: datetime

class UnitImport(UnitBaseSchema):

    @root_validator
    def check_size_type(cls, values):
        size = values.get('size')
        type = values.get('type')
        url = values.get('url')
        assert ((UnitType(type) == UnitType.FOLDER and size is None and url is None) or (
                UnitType(type) == UnitType.FILE and size > 0 and len(url)<=255))


        return values


class UnitImportRequest(BaseModel):
    items: List[UnitImport]
    update_date: datetime = Field(alias='updateDate')


class UnitSchema(UnitBaseSchema):
    children: List["UnitSchema"] = None

    @validator("children")
    def replace_empty_list(cls, v):
        return v or None

    def get_child(self, index):
        if len(self.children) > index:
            return self.children[index]
        return None

class UnitResponseSchema(UnitBaseSchema):
    children: List["UnitResponseSchema"] = None

    @validator("date")
    def date_conversion(cls, v):
        return convert_datetime_to_iso_8601_with_z_suffix(v)

    @validator("children")
    def replace_empty_list(cls, v, values):
        type = values.get("type")
        if type == UnitType.FILE:
            return v or None
        else:
            return v or list()

    def get_child(self, index):
        try:
            if len(self.children) > index:
                return self.children[index]
        except:
            pass
        return None




class UnitStatisticResponse(BaseModel):
    items: List[HistoryBaseSchema]

    class Config:
        orm_mode = True


UnitSchema.update_forward_refs()
