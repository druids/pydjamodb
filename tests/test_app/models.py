from pynamodb.indexes import AllProjection, GlobalSecondaryIndex

from pydjamodb.models import DynamoModel
from pydjamodb.queryset import DynamoDBManager
from pynamodb.attributes import (
    MapAttribute, NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute, BooleanAttribute, NumberAttribute
)


class StringNumberIndex(GlobalSecondaryIndex):

    string = UnicodeAttribute(hash_key=True)
    number = NumberAttribute(range_key=True)

    class Meta:
        projection = AllProjection()


class TestDynamoModel(DynamoModel):

    id = UnicodeAttribute(hash_key=True)
    date = UTCDateTimeAttribute(range_key=True)
    string = UnicodeAttribute(null=True)
    number = NumberAttribute()
    bool = BooleanAttribute()

    string_number_index = StringNumberIndex()
    objects_string_number = DynamoDBManager(index=string_number_index)

    class Meta:
        table_name = 'pydjamodbtest'
