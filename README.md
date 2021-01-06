Prolog
======

`pydjamodb` library is Django connector to the AWS DynamoDB. As a base is used `PynamoDB` library which models is transformed into `Django` model structure with the querysets and managers.

Installation
------------

- Install `pydjamodb` with the `pip` command:

```bash
pip install pydjamodb
```

- Set ``pydjamodb.test_runner.DynamoDBTestDiscoverRunner`` as your Django test runner:

```python
TEST_RUNNER = 'pydjamodb.test_runner.DynamoDBTestDiscoverRunner'
```

- Set configuration of your DynamoDB database:

```python
PYDJAMODB_DATABASE = {
    'HOST': '',
    'AWS_ACCESS_KEY_ID': '',
    'AWS_SECRET_ACCESS_KEY': '',
    'AWS_REGION': '',
    'TABLE_PREFIX': 'some-prefix',
    'BILLING_MODE': 'PAY_PER_REQUEST',
    'TAGS': {
        'tag_name': 'tag',
    }
}
```

Usage
-----

PyDjamoDB is common to Django models:

```python
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
```

Now we can use model manager similar to Django model managers:

```python
# sets hash key for DynamoDB database and returns all elements with this key
TestDynamoModel.objects.set_hash_key('test')
# returns first instance with hash key 'test'
TestDynamoModel.objects.set_hash_key('test').first()
# returns first instance with hash key 'test'
TestDynamoModel.objects.set_hash_key('test').last()
# returns instance with hash key 'test' if there is only one instance, else raises MultipleObjectsReturned or ObjectDoesNotExist exception
TestDynamoModel.objects.set_hash_key('test').get()
# returns instance with hash key 'test' and range key equal to datetime.now() if there is only one instance, else raises MultipleObjectsReturned or ObjectDoesNotExist exception
TestDynamoModel.objects.set_hash_key('test').get(date=datetime.now())
# returns number of instances with hash key 'test'
TestDynamoModel.objects.set_hash_key('test').count()
# Filter elements by range key you can use operators (eq, startswith, gt, lt, gte, lte, between)
TestDynamoModel.objects.set_hash_key('test').filter(date=datetime.now()) 
 # sets paginator limitation to 10 items
TestDynamoModel.objects.set_hash_key('test').set_limit(10)
# returns key of the last item of the queryset result
TestDynamoModel.objects.set_hash_key('test').set_limit(10).last_evaluated_key
# sets index of pagination to specific key
TestDynamoModel.objects.set_hash_key('test').set_limit(10).set_last_evaluated_key(key)
# sets empty queryset
TestDynamoModel.objects.set_hash_key('test').none()
# reverses the order of returned items
TestDynamoModel.objects.set_hash_key('test').set_scan_index_forward(False)
# sets another pynamodb model index
TestDynamoModel.objects.set_hash_key('test').set_index(TestDynamoModel.string_number_index)
```
