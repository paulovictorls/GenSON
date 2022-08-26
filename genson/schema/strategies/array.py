from msilib import schema
from .base import SchemaStrategy


class BaseArray(SchemaStrategy):
    """
    abstract array schema strategy
    """
    KEYWORDS = ('type', 'items')

    @staticmethod
    def match_object(obj):
        return isinstance(obj, list)

    def json_method(self, schema):
        schema['type'] = 'array'
        if self._items:
            schema['items'] = self.items_to_schema('json')

    def avro_method(self, schema, field_name):
        schema['name'] = field_name
        schema['type'] = 'array'
        if self._items:
            schema['type'] = {
                'type': 'array',
                'elementType': self.items_to_schema('avro'),
                'containsNull': True
            }
        schema['nullable'] = True
        schema['metadata'] = {}

    def spark_method(self, schema, field_name):
        schema['name'] = field_name
        schema['type'] = 'array'
        if self._items:
            schema['type'] = {
                'type': 'array',
                'elementType': self.items_to_schema('spark'),
                'containsNull': True
            }
        schema['nullable'] = True
        schema['metadata'] = {}

    def ddl_method(self, field_name):
        aux = self.items_to_schema('ddl')
        if isinstance(aux, str):
            fields_schema = f"struct<{aux}>"
        else:
            fields_schema = ",".join(aux)
        
        return "%s:array<%s>" % (field_name, fields_schema)

    def to_schema(self, field_name=None):
        schema = super().to_schema()
        if self.schema_type == 'json':
            self.json_method(schema)
        elif self.schema_type == 'avro':
            self.avro_method(schema, field_name)
        elif self.schema_type == 'spark':
            self.spark_method(schema, field_name)
        elif self.schema_type == 'ddl':
            return self.ddl_method(field_name)

        return schema


class List(BaseArray):
    """
    strategy for list-style array schemas. This is the default
    strategy for arrays.
    """
    @staticmethod
    def match_schema(schema):
        return schema.get('type') == 'array' \
            and isinstance(schema.get('items', {}), dict)

    def __init__(self, node_class, schema_type):
        super().__init__(node_class, schema_type)
        self._items = node_class()
        self.schema_type = schema_type

    def add_schema(self, schema):
        super().add_schema(schema)
        if 'items' in schema:
            self._items.add_schema(schema['items'])

    def add_object(self, obj, schema_type=None):
        for item in obj:
            self._items.add_object(item, self.schema_type)

    def items_to_schema(self, schema_type):
        return self._items.to_schema(schema_type)


class Tuple(BaseArray):
    """
    strategy for tuple-style array schemas. These will always have
    an items key to preserve the fact that it's a tuple.
    """
    @staticmethod
    def match_schema(schema):
        return schema.get('type') == 'array' \
            and isinstance(schema.get('items'), list)

    def __init__(self, node_class, schema_type):
        super().__init__(node_class)
        self._items = [node_class()]

    def add_schema(self, schema):
        super().add_schema(schema)
        if 'items' in schema:
            self._add(schema['items'], 'add_schema')

    def add_object(self, obj):
        self._add(obj, 'add_object')

    def _add(self, items, func):
        while len(self._items) < len(items):
            self._items.append(self.node_class())

        for subschema, item in zip(self._items, items):
            getattr(subschema, func)(item)

    def items_to_schema(self, schema_type):
        return [item.to_schema(schema_type) for item in self._items]
