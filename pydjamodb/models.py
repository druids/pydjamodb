from django.conf import settings

from pynamodb.models import MetaModel, Model

from .connection import TableConnection
from .queryset import DynamoDBManager


dynamodb_model_classes = []


class DynamoMetaModel(MetaModel):

    def __init__(cls, name, bases, attrs):
        if 'objects' not in attrs:
            cls.objects = attrs['objects'] = DynamoDBManager()

        attr_meta = attrs.get('Meta', None)

        abstract = getattr(attr_meta, 'abstract', False)
        isproxy = getattr(attr_meta, 'proxy', False)

        base_meta = getattr(cls, '_meta', None)
        meta = attr_meta or getattr(cls, 'Meta', None)

        # Meta inheritance
        if base_meta:
            for k, v in base_meta.__dict__.items():
                if not (k.startswith('_') or k in {'abstract', 'proxy'} or hasattr(meta, k)):
                    setattr(meta, k, v)

        cls._meta = meta

        super().__init__(name, bases, attrs)

        if meta and not hasattr(cls.Meta, 'billing_mode'):
            meta.billing_mode = settings.PYDJAMODB_DATABASE.get('BILLING_MODE')

        if not abstract and not isproxy:
            dynamodb_model_classes.append(cls)

        for k, v in attrs.items():
            if isinstance(v, DynamoDBManager):
                v.contribute_to_class(cls)


class DynamoModel(Model, metaclass=DynamoMetaModel):

    @classmethod
    def _get_connection(cls):
        if cls._connection is None:
            cls._connection = TableConnection(cls.Meta.table_name)
        return cls._connection

    @classmethod
    def delete_table(cls, wait=False):
        return cls._get_connection().delete_table(wait)

    def __eq__(self, other):
        return repr(self) == repr(other)

    class Meta:
        abstract = True
