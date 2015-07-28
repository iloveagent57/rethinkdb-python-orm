# encoding=utf-8
from __future__ import unicode_literals

from collections import defaultdict
from contextlib import contextmanager

import rethinkdb


class Attribute(object):
    def __init__(self, default=None):
        self.default = default
        
    def from_document(self, value):
        return value

    def to_document(self, value):
        return value


class Reference(Attribute):
    def __init__(self, referenced_mappable):
        """
        Creates a new Reference attribute.
        :param referenced_mappable A Mappable class, an instance of which is referred to by this Reference.
        """
        self.referenced_mappable = referenced_mappable

    def from_document(self, value):
        return self.referenced_mappable.get(value)

    def to_document(self, value):
        return value.id


class HasAttributes(type):
    def __new__(cls, name, bases, attrs):
        mappable_cls = super(HasAttributes, cls).__new__(cls, name, bases, attrs)
        if not getattr(mappable_cls, '_attributes', None):
            setattr(mappable_cls, '_attributes', {})
        for attr_name, attr_value in attrs.iteritems():
            if isinstance(attr_value, Attribute):
                print attr_name, attr_value
                mappable_cls._attributes[attr_name] = attr_value
        return mappable_cls


class Mappable(object):
    __metaclass__ = HasAttributes

    @classmethod
    def plural(cls):
        return getattr(cls, '__plural__', cls.__name__.lower() + 's')

    @property
    def attributes(self):
        return getattr(self, '_attributes', {})

    def __init__(self, doc=None, **kwargs):
        self.from_document(doc or kwargs)

    def from_document(self, document):
        for attr_name, attribute in self.attributes.iteritems():
            setattr(self, attr_name, attribute.default)
        for key, value in document.iteritems():
            attribute = self.attributes.get(key)
            if not attribute:
                continue
            setattr(self, key, attribute.from_document(value))

    def to_document(self):
        return {attr_name: attribute.to_document(getattr(self, attr_name, None))
                for attr_name, attribute in self.attributes.iteritems()}

    def __str__(self):
        attrs_and_values = ', '.join(('%s=%s' % (attr_name, repr(getattr(self, attr_name, None)))
                                                for attr_name in self.attributes.keys()))
        return '<%s: %s>' % (self.__class__.__name__, attrs_and_values)

    def __repr__(self):
        return self.__str__()


class Model(Mappable):
    id = Attribute()
    _scope = []

    @classmethod
    def _current_scope(cls):
        if cls._scope:
            return cls._scope[-1]
        else:
            raise Exception("Ain't no scope!")

    @classmethod
    def _connection(cls):
        return cls._current_scope()[0]

    @classmethod
    def _identity_map(cls):
        return cls._current_scope()[1]

    def _by_primary_key(cls, first, *args):
        connection = cls._connection()
        if not connection:
            raise Exception("No RethinkDB Connection.")

        if not args:
            return rethinkdb.table(cls.plural()).get(first).run(connection)
        else:
            return rethinkdb.table(cls.plural()).get([first] + args).run(connection)

    def save(self):
        connection = self.__class__._connection()
        table_name = self.__class__.plural()
        document = self.to_document()
        if not document.get('id'):
            document.pop('id', None)
            result = rethinkdb.table(table_name).insert(document).run(connection)
            setattr(self, 'id', result['generated_keys'][0])
            return result
        else:
            return rethinkdb.table(table_name).insert(document, conflict='update').run(connection)

    def get(cls, primary_key):
        model = cls._identity_map().get(key)
        if not model:
            model = cls.from_document(cls._by_primary_key(primary_key))
            cls._identify_map()[model.__class__][primary_key] = model
        return model

    def get_many(cls, primary_keys):
        identity_map = cls._identity_map().get(cls)
        models = {key: identity_map.get(key) for key in primary_keys}
        to_fetch = [key for key in primary_keys if not models.get(key)]

        for document in cls._by_primary_key(*to_fetch):
            model = cls.from_document(document)
            cls._identity_map()[model.__class__][model.id] = model
            models[model.id] = model

        return (models.get(key) for key in primary_keys)

    @classmethod
    @contextmanager
    def connection(cls, connection):
        cls._scope.append((connection, defaultdict(dict)))
        try:
            yield None
        finally:
            connection.close()
            cls._scope.pop()
