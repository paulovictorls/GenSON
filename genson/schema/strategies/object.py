from collections import defaultdict
from re import search

from py_avro_schema import schema
from .base import SchemaStrategy


class Object(SchemaStrategy):
    """
    object schema strategy
    """
    KEYWORDS = ('type', 'properties', 'patternProperties', 'required')

    @staticmethod
    def match_schema(schema):
        return schema.get('type') == 'object'

    @staticmethod
    def match_object(obj):
        return isinstance(obj, dict)

    def __init__(self, node_class, schema_type):
        super().__init__(node_class, schema_type)

        self._properties = defaultdict(node_class)
        self._pattern_properties = defaultdict(node_class)
        self._required = None
        self._include_empty_required = False
        self.schema_type = schema_type

    def add_schema(self, schema):
        super().add_schema(schema)
        if 'properties' in schema:
            for prop, subschema in schema['properties'].items():
                subnode = self._properties[prop]
                if subschema is not None:
                    subnode.add_schema(subschema)
        if 'patternProperties' in schema:
            for pattern, subschema in schema['patternProperties'].items():
                subnode = self._pattern_properties[pattern]
                if subschema is not None:
                    subnode.add_schema(subschema)
        if 'required' in schema:
            required = set(schema['required'])
            if not required:
                self._include_empty_required = True
            if self._required is None:
                self._required = required
            else:
                self._required &= required

    def add_object(self, obj):
        properties = set()
        for prop, subobj in obj.items():
            pattern = None

            if prop not in self._properties:
                pattern = self._matching_pattern(prop)

            if pattern is not None:
                self._pattern_properties[pattern].add_object(subobj)
            else:
                properties.add(prop)
                self._properties[prop].add_object(subobj, self.schema_type)

        if self._required is None:
            self._required = properties
        else:
            self._required &= properties

    def _matching_pattern(self, prop):
        for pattern in self._pattern_properties.keys():
            if search(pattern, prop):
                return pattern

    def _add(self, items, func):
        while len(self._items) < len(items):
            self._items.append(self._schema_node_class())

        for subschema, item in zip(self._items, items):
            getattr(subschema, func)(item)

    def create_json_schema(self, schema):
        schema['type'] = 'object'
        if self._properties:
            schema['properties'] = self._properties_to_schema(
                self._properties)
        if self._pattern_properties:
            schema['patternProperties'] = self._properties_to_schema(
                self._pattern_properties)
        if self._required or self._include_empty_required:
            schema['required'] = sorted(self._required)
        return schema

    def create_avro_schema(self, schema, field_name):
        if field_name:

            schema['name'] = field_name
        schema['type'] = 'record'

        if self._properties:
            if field_name == None:
                schema['name'] = "DynamicRecord"
                schema['namespace'] = "root"
                schema['fields'] = self._properties_to_schema(
                    self._properties)

            else:
                schema['type'] = [{
                    'type': 'record',
                    'name': field_name,
                    'fields': self._properties_to_schema(
                        self._properties)
                }, "null"]
                schema['nullable'] = True

    def create_spark_schema(self, schema, field_name):
        if field_name:
            schema['name'] = field_name

        schema['type'] = 'struct'
        if self._properties:
            if field_name == None:
                schema['fields'] = self._properties_to_schema(
                    self._properties)

            else:
                schema['type'] = {
                    'type': 'struct',
                    'fields': self._properties_to_schema(
                        self._properties)
                }
                schema['nullable'] = True
                schema['metadata'] = {}

    def create_ddl_schema(self, schema, field_name):
        aux = self._properties_to_schema(self._properties)
        fields_schema = ",".join(aux)
        if field_name:
            return "%s:struct<%s>" % (field_name, fields_schema)
        else:
            return fields_schema

    def to_schema(self, field_name=None):
        schema = super().to_schema()

        if self.schema_type == 'json':
            self.create_json_schema(schema)
        elif self.schema_type == 'avro':
            self.create_avro_schema(schema, field_name)
        elif self.schema_type == 'spark':
            self.create_spark_schema(schema, field_name)
        elif self.schema_type == 'ddl':
            schema = self.create_ddl_schema(schema, field_name)

        return schema

    def _properties_to_schema(self, properties):
        if self.schema_type == 'json':
            schema_properties = {}
        else:
            schema_properties = []

        for prop, schema_node in properties.items():
            if self.schema_type == 'json':
                schema_properties[prop] = schema_node.to_schema(self.schema_type)
            else:
                schema_properties.append(schema_node.to_schema(self.schema_type, prop))

        return schema_properties
