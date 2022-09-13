import datetime
from math import floor
from typing import Dict, Union
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger
from sqlalchemy.orm import Session
from starlette.responses import Response
from starlette.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_404_NOT_FOUND

from api.schemas.responses import HTTP_400_RESPONSE, HTTP_404_RESPONSE
from api.schemas.file_unit import UnitImportRequest, UnitSchema, \
    UnitStatisticResponse, UnitResponseSchema, HistoryRequest, HistoryResponseSchema, \
    TestSchema, UnitBaseSchema
from database.engine import get_session
from database.models import Unit, UnitType, HistoryUnit

router = APIRouter()

def unit_calc(unit: Unit):
    if unit.type == UnitType.FILE:
        return unit.size
    elif unit.type == UnitType.FOLDER:
        size = 0
        for child in unit.children:
            size += unit_calc(child)

        return size



@router.get('/', name='Заяц - волк!',tags=['Проверка связи'])
def get_test() -> Dict[str, str]:
    return {'Привет': 'лунатикам!'}


@router.post('/imports', name='Добавляет новые папки и файлы',
             status_code=200, tags=['Базовые задачи'])
def import_units(items: UnitImportRequest,
                 session: Session = Depends(get_session)) -> \
        Response:
    id_set = set()
    parent_set = set()
    file_set = set()
    for fileunit in items.items:

        # если элемент уже есть в загрузке, возвращаем ошибку
        if str(fileunit.id) in id_set:
            raise HTTPException(status_code=400, detail='id already exists in batch!')
        id_set.add(str(fileunit.id))

        # нужно проверить не ссылается ли parentid на FILE
        file_parent = session.query(Unit).filter(
            Unit.id == fileunit.parent_id).one_or_none()
        # если элемент есть, то проверяем, не является ли parent FILE
        if fileunit.parent_id is not None:
            if file_parent is not None:
                if file_parent.type == UnitType.FILE:
                    raise HTTPException(status_code=400, detail='File can not be a parent!')
            # если в базе его нет, добавим id в множество родителей,
            # и он гарантированно должен быть в загрузке
            else:
                parent_set.add(str(fileunit.parent_id))

        # если тип элемента файл - запишем его в множество, чтобы потом проверить
        # пересечение множеств. Если множества пересекаются, то есть ссылка на файл!
        if str(fileunit.type) == 'FILE':
            file_set.add(str(fileunit.type))

        # считываем дату из запроса
        fileunit.date = items.update_date
        # проверяем, есть ли элемент в базе
        file_unit_model = session.query(Unit).filter(
            Unit.id == fileunit.id).one_or_none()
        # если элемент есть:
        # проверяем, не изменился ли его тип
        if file_unit_model is not None:
            if file_unit_model.type != fileunit.type:
                # менять тип элемента не допускается
                raise HTTPException(status_code=400, detail='Attempt to change unit type!')
            # добавим в session
            for var, value in vars(fileunit).items():
                setattr(file_unit_model, var, value) #if value else None
            session.add(file_unit_model)
        else:
            session.add(Unit(**fileunit.dict()))
        #     перед коммитом проверим ссылки parent на FILE
        if file_set.intersection(parent_set) != set():
            raise HTTPException(status_code=400, detail='File can not be a parent!')
        if not (id_set >= parent_set):
            raise HTTPException(status_code=400, detail='Unresolved parentId link!')


        session.commit()
    #     вот здесь нужно запросить данные из базы
    #     с датой равной дате загрузки
    date = items.update_date
    updated_units = session.query(Unit).filter(
        Unit.date == date).all()
    if updated_units:
        for unit in updated_units:
            dct = {}
            dct["id"] = str(unit.id)
            dct["type"] = str(unit.type).split('.')[1]
            dct["url"] = unit.url
            if not str(unit.parent_id) == "None":
                dct["parent_id"] = str(unit.parent_id)

            dct["size"] = unit_calc(unit)
            dct["date"] = str(unit.date.astimezone(datetime.timezone.utc))
            session.add(HistoryUnit(**dct))

        session.commit()

    return Response(status_code=200)


@router.get('/nodes/{id}',
            name='Получает информацию об элементе по идентификатору',
            response_model=UnitResponseSchema, response_model_by_alias=True,
            tags=['Базовые задачи'])

def get_unit(id:  str, session: Session = Depends(get_session)):
    """
    Получить информацию об элементе по идентификатору. При получении информации о папке
    также предоставляется информация о её дочерних элементах.

        - для пустой папки поле children равно пустому массиву, а для файла равно null
        - размер папки - это суммарный размер всех её элементов. Если папка не содержит элементов,
        то размер равен 0. При обновлении размера элемента,
         суммарный размер папки, которая содержит этот элемент, тоже обновляется.

    """
    unit = session.query(Unit).filter_by(id=id).one_or_none()
    if unit is None:
        raise HTTPException(status_code=404, detail='Item not found')
    element: UnitResponseSchema = UnitResponseSchema.from_orm(unit)
    if element.type == UnitType.FOLDER:
        stc = [[element, 0, 0]]
        while len(stc):
            last, index = stc[-1][0], stc[-1][1]
            child = last.get_child(index)
            if child is None:
                last.size = stc[-1][2]
                if len(stc) > 1:
                    stc[-2][2] += stc[-1][2]
                stc.pop()
            else:
                stc[-1][1] += 1
                if child.type == UnitType.FILE:
                    stc[-1][2] += child.size
                else:
                    stc.append([child, 0, 0])
    return element


@router.delete(
    '/delete/{id}',
    name='Удаляет элемент по идентификатору',
    status_code=200,
    responses={
        200: {
            'description': 'Удаление прошло успешно',
            'model': None,
        },
        HTTP_400_BAD_REQUEST: HTTP_400_RESPONSE,
        HTTP_404_NOT_FOUND: HTTP_404_RESPONSE,
    },
    tags=['Базовые задачи']
)
def delete_unit(id: str,
                session: Session = Depends(get_session)) -> Response:
    """
    При удалении папки удаляются все вложенные элементы.
    Доступ к статистике (истории обновлений) удаленного элемента невозможен.
    """
    shopunit = session.query(Unit).filter_by(id=id).one_or_none()
    if shopunit is None:
        raise HTTPException(status_code=404, detail='Item not found')
    try:
        session.delete(shopunit)
        session.commit()
        return Response(status_code=HTTP_200_OK)
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=400, detail='Validation Failed')


@router.get('/updates', status_code=200, tags=['Дополнительные задачи'],
            response_model=UnitStatisticResponse)
def get_files(date: datetime.datetime, session: Session = Depends(get_session)) -> UnitStatisticResponse:
    """
    Получение списка **файлов**, которые были обновлены за последние 24 часа
    включительно [date - 24h, date] от времени переданном в запросе.
    """
    logger.info(date)
    items = session.query(Unit).filter(
        Unit.type == UnitType.FILE,
        Unit.date <= date,
        Unit.date >= date - datetime.timedelta(days=1),
    ).all()
    return UnitStatisticResponse(items=items)

@router.get('/node/{id}/history',
            name='истории обновлений по элементу за заданный полуинтервал [from, to)',
            response_model=HistoryResponseSchema, response_model_by_alias=True,
            tags=['Дополнительные задачи'])

def get_history(id: str, dateStart: datetime.datetime = None, dateEnd: datetime.datetime = None, session: Session = Depends(get_session)):
    """
    Получить информацию об элементе по идентификатору.
    При получении информации о категории также предоставляется информация о её
     дочерних элементах
    """
    if dateStart == None:
        dateStart = datetime.datetime.min
    if dateEnd == None:
        dateEnd = datetime.datetime.max

    items = session.query(HistoryUnit).filter(
        HistoryUnit.id == id,
        HistoryUnit.date >= dateStart,
        HistoryUnit.date < dateEnd ).all()

    return {"items" : items}