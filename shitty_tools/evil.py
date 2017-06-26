from sqlalchemy.orm import relationship
from sqlalchemy import and_


def create_attribute_associator(entity_id_col, eav_cls, eav_entity_id_col, eav_attr_col, eav_value_col):
    '''
    Returns a class method that allows one to associate attributes in an Entity-Attribute-Value table
    with a sqlalchemy class and then access those attributes as properties of the entity class.

    Example usage:

    >>> from sqlalchemy import Column, ForeignKey, Index, Integer, String
    >>> from sqlalchemy.orm import relationship
    >>> from sqlalchemy.ext.declarative import declarative_base
    >>> Base = declarative_base()
    >>> metadata = Base.metadata
    >>>
    >>> class Eav(Base):
    ...     __tablename__ = 'eav'
    ...     __table_args__ = (
    ...         Index('e_a_uq', 'entity_id', 'attribute', unique=True),
    ...     )
    ...     id = Column(Integer, primary_key=True)
    ...     entity_id = Column(ForeignKey('entity.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    ...     attribute = Column(String(255), nullable=False)
    ...     value = Column(String(255))
    ...
    >>>
    >>> class Entity(Base):
    ...     __tablename__ = 'entity'
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String(255), nullable=False)
    ...     _add_attribute = create_attribute_associator(id, Eav, Eav.entity_id, Eav.attribute, Eav.value)
    ...
    >>> Entity._add_attribute('foo')
    >>> Entity._add_attribute('bar')
    >>>
    >>> dir(Entity)
    ['__class__', '__delattr__', '__dict__', '__doc__', '__format__', '__getattribute__', '__hash__', '__init__',
    '__mapper__', '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__',
    '__str__', '__subclasshook__', '__table__', '__tablename__', '__weakref__', '_add_attribute', '_bar_get',
    '_bar_obj', '_bar_set', '_decl_class_registry', '_foo_get', '_foo_obj', '_foo_set', '_sa_class_manager',
    'bar', 'foo', 'id', 'metadata', 'name']


    :param entity_id_col: The id column of your entity
    :param eav_cls: The sqlalchemy class of the entity attribute value (EAV) table
    :param eav_entity_id_col: The foreign key column from the EAV table to the entity table
    :param eav_attr_col: The EAV table column that stores the attribute name
    :param eav_value_col: The EAV table column that stores the attribute value
    :return: class method to with signature like add_attribute(cls, attr_name, lazy='joined')
    '''
    attr_col_name = eav_attr_col.key
    value_col_name = eav_value_col.key
    @classmethod
    def add_attribute(cls, attr_name, lazy='joined'):
        obj_name = '_%s_obj' % attr_name
        getter_name = '_%s_get' % attr_name
        setter_name = '_%s_set' % attr_name
        rel = relationship(eav_cls,
                           primaryjoin=and_(entity_id_col == eav_entity_id_col,
                                            eav_attr_col == attr_name),
                           uselist=False, lazy=lazy)
        def getter(self):
            return getattr(self, obj_name).value
        def setter(self, value):
            obj = getattr(self, obj_name)
            if obj is None:
                obj = eav_cls(**{attr_col_name: attr_name, value_col_name: value})
                setattr(self, obj_name, obj)
            else:
                setattr(obj, value_col_name, value)
        prop = property(getter, setter)
        setattr(cls, obj_name, rel)
        setattr(cls, getter_name, getter)
        setattr(cls, setter_name, setter)
        setattr(cls, attr_name, prop)
    return add_attribute