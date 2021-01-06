from pynamodb.attributes import Attribute
from pynamodb.constants import STRING


class StringJoinAttribute(Attribute):

    attr_type = STRING

    def __init__(self, fields, separator='||', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fields = fields
        self._separator = separator

    def serialize(self, value):
        if isinstance(value, str):
            return value

        serialized_values = []
        for v, f in zip(value, self._fields):
            serialized_values.append(f.serialize(v) or '')
        return self._separator.join(serialized_values)

    def deserialize(self, value):
        serialized_values = value.split(self._separator)
        deserialized_values = []
        for v, f in zip(serialized_values, self._fields):
            deserialized_values.append(f.deserialize(v))
        return deserialized_values


class BooleanUnicodeAttribute(Attribute):

    attr_type = STRING

    def serialize(self, value):
        if value is None:
            return None
        return str(value)

    def deserialize(self, value):
        return value == 'True'
