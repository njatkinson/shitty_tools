from sqlalchemy.exc import IntegrityError


# TODO: Add retries with exponential back-off on OperationalError
# On MariaDB/Percona platform with galera replication, writes to a single table with a unique constraint
# on multiple nodes can result in deadlocks & timeouts when unique values written on multiple nodes
# collide. The solution is to exponential back-off with a random jitter to allow one of the writes to
# succeed.


def get_or_create(object_class, object_dict, db_session):
    orm_object = object_class(**object_dict)
    db_session.add(orm_object)
    try:
        db_session.flush()
    except IntegrityError as e:
        db_session.rollback()
        orm_object_query = db_session.query(object_class).filter_by(**object_dict)
        orm_object = orm_object_query.scalar()
    return orm_object


def get_or_create_list(object_class, object_dict_list, db_session):
    if len(object_dict_list) < 4:
        return _pessimistic_get_or_create_list(object_class, object_dict_list, db_session)
    try:
        return _optimistic_get_or_create_list(object_class, object_dict_list, db_session)
    except IntegrityError as e:
        db_session.rollback()
        return get_or_create_list(object_class, object_dict_list[::2], db_session) + \
               get_or_create_list(object_class, object_dict_list[1::2], db_session)


def _optimistic_get_or_create_list(object_class, object_dict_list, db_session):
    orm_objects = [object_class(**object_dict) for object_dict in object_dict_list]
    db_session.add_all(orm_objects)
    db_session.flush()
    return orm_objects


def _pessimistic_get_or_create_list(object_class, object_dict_list, db_session):
    return [get_or_create(object_class, object_dict, db_session) for object_dict in object_dict_list]

