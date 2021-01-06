import random

import string

from django.test import TestCase
from django.utils.timezone import now

from germanium.tools import assert_equal, assert_raises, assert_true, assert_false

from uuid import uuid4

from test_app.models import TestDynamoModel

from pydjamodb.queryset import DynamoDBQuerySetError, MultipleObjectsReturned, ObjectDoesNotExist
from pydjamodb.tests import DynamoDBTestMixin


class PyDjamoDBTestCase(DynamoDBTestMixin, TestCase):

    def create_test_dynamo_model(self, **kwargs):
        default_data = dict(
            id=str(uuid4()),
            date=now(),
            string=''.join(random.choice(string.ascii_lowercase) for _ in range(10)),
            number=random.randint(0, 100),
            bool=True
        )
        default_data.update(kwargs)

        instance = TestDynamoModel(**default_data)
        instance.save()
        return instance

    def create_test_dynamo_model_instances(self, count=10, **kwargs):
        instances = []
        for i in range(count):
            default_data = dict(
                string='test {}'.format(i),
                number=i,
                bool=bool(i % 2)
            )
            default_data.update(kwargs)
            instances.append(self.create_test_dynamo_model(**default_data))
        return instances

    def test_queryset_first_should_return_first_element(self):
        instances = self.create_test_dynamo_model_instances(id='test')
        assert_equal(TestDynamoModel.objects.set_hash_key('test').first(), instances[0])

    def test_queryset_last_should_return_first_element(self):
        instances = self.create_test_dynamo_model_instances(id='test')
        assert_equal(TestDynamoModel.objects.set_hash_key('test').last(), instances[-1])

    def test_index_to_queryset_first_should_return_first_element(self):
        instances = self.create_test_dynamo_model_instances(string='test')
        assert_equal(TestDynamoModel.objects_string_number.set_hash_key('test').first(), instances[0])

    def test_index_to_queryset_first_should_return_last_element(self):
        instances = self.create_test_dynamo_model_instances(string='test')
        assert_equal(TestDynamoModel.objects_string_number.set_hash_key('test').last(), instances[-1])

    def test_queryset_set_scan_index_forward_should_change_items_order(self):
        instances = self.create_test_dynamo_model_instances(string='test')
        qs = TestDynamoModel.objects_string_number.set_hash_key('test')
        assert_equal(qs.set_scan_index_forward(False).first(), instances[-1])
        assert_equal(qs.set_scan_index_forward(False).last(), instances[0])

    def test_queryset_count_should_return_elements_count(self):
        self.create_test_dynamo_model_instances(string='test')
        qs = TestDynamoModel.objects_string_number.set_hash_key('test')
        assert_equal(qs.count(), 10)

    def test_queryset_none_should_return_empty_list(self):
        self.create_test_dynamo_model_instances(string='test')
        qs = TestDynamoModel.objects_string_number.set_hash_key('test')
        assert_equal(list(qs.none()), [])
        assert_equal(qs.none().count(), 0)

    def test_queryset_limit_should_return_paged_items(self):
        instances = self.create_test_dynamo_model_instances(string='test')
        qs = TestDynamoModel.objects_string_number.set_hash_key('test')
        assert_equal(list(qs.set_limit(limit=2)), instances[:2])
        assert_equal(qs.set_limit(limit=2).count(), 2)
        assert_equal(
            qs.set_limit(limit=2).next_key['id'],
            {'S': instances[1].id}
        )
        assert_equal(
            list(qs.set_limit(limit=10).set_last_evaluated_key(qs.set_limit(limit=2).next_key)),
            instances[2:]
        )

    def test_not_set_queryset_hash_key_should_raise_exception(self):
        with assert_raises(DynamoDBQuerySetError):
            TestDynamoModel.objects_string_number.count()

    def test_queryset_filter_should_return_filtered_items(self):
        instances = self.create_test_dynamo_model_instances(string='test')
        qs = TestDynamoModel.objects_string_number.set_hash_key('test')
        assert_equal(list(qs.filter(number=5)), [instances[5]])
        assert_equal(list(qs.filter(number__between=(2, 4))), instances[2:5])
        assert_equal(list(qs.filter(number__gt=4)), instances[5:])
        assert_equal(list(qs.filter(number__lte=4)), instances[:5])

    def test_queryset_exists_should_return_if_items_exists(self):
        self.create_test_dynamo_model_instances(string='test')
        qs = TestDynamoModel.objects_string_number.set_hash_key('test')
        assert_true(qs.filter(number=5).exists())
        assert_false(qs.filter(number=10).exists())

    def test_queryset_get_should_return_item_or_error(self):
        instances = self.create_test_dynamo_model_instances(string='test')
        qs = TestDynamoModel.objects_string_number.set_hash_key('test')
        with assert_raises(MultipleObjectsReturned):
            qs.get()

        with assert_raises(ObjectDoesNotExist):
            qs.filter(number=10).get()

        with assert_raises(ObjectDoesNotExist):
            qs.get(number=10)

        assert_equal(qs.get(number=9), instances[-1])
