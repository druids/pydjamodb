import inspect


KEYS_SEPARATOR = '||'


class DynamoDBQueryException(Exception):
    pass


class FieldDoesNotExists(DynamoDBQueryException):
    pass


class InvalidOperator(DynamoDBQueryException):
    pass


class NoneExecution:

    last_evaluated_key = None


class DynamoDBQuerySetError(Exception):
    pass


class MultipleObjectsReturned(DynamoDBQuerySetError):
    pass


class ObjectDoesNotExist(DynamoDBQuerySetError):
    pass


class DynamoDBQuerySet:

    def __init__(self, model):
        self._model = model
        self._limit = None
        self._last_evaluated_key = None
        self._hash_key = None
        self._index = None
        self._scan_index_forward = True
        self._filter = None
        self._next_key = None
        self._init()

    def _clone(self):
        """
        Return a copy of the current QuerySet. A lightweight alternative
        to deepcopy().
        """
        c = self.__class__(model=self._model)
        c._limit = self._limit
        c._last_evaluated_key = self._last_evaluated_key
        c._hash_key = self._hash_key
        c._index = self._index
        c._scan_index_forward = self._scan_index_forward
        c._filter = self._filter
        if isinstance(self._execution, NoneExecution):
            c._execution = self._execution
            c._results = self._results
        return c

    def __iter__(self):
        self._execute()
        return iter(self._results)

    def _init(self):
        self._execution = None
        self._results = None

    def _process_execution(self):
        query = self._index.query if self._index else self._model.query

        if self._hash_key is None:
            raise DynamoDBQuerySetError('Hash key must be set')

        self._execution = query(
            self._hash_key,
            self._filter,
            limit=self._limit,
            last_evaluated_key=self._last_evaluated_key,
            scan_index_forward=self._scan_index_forward
        )
        self._results = list(self._execution)
        self._next_key = self._execution.last_evaluated_key

    def _execute(self):
        if not self._execution:
            self._process_execution()

    @property
    def next_key(self):
        self._execute()
        return self._next_key

    def set_limit(self, limit):
        obj = self._clone()
        obj._limit = limit
        return obj

    def set_last_evaluated_key(self, last_evaluated_key):
        obj = self._clone()
        obj._last_evaluated_key = last_evaluated_key
        return obj

    def set_index(self, index):
        obj = self._clone()
        obj._index = index
        return obj

    def set_hash_key(self, hash_key):
        obj = self._clone()
        obj._hash_key = hash_key
        return obj

    def none(self):
        obj = self._clone()
        obj._execution = NoneExecution()
        obj._results = []
        return obj

    def set_scan_index_forward(self, value):
        obj = self._clone()
        obj._scan_index_forward = value
        return obj

    def last(self):
        obj = self._clone()
        obj._limit = 1
        obj._scan_index_forward = not obj._scan_index_forward
        obj._execute()
        if obj._results:
            return obj._results[0]
        else:
            return None

    def first(self):
        obj = self._clone()
        obj._limit = 1
        obj._execute()
        if obj._results:
            return obj._results[0]
        else:
            return None

    def exists(self):
        return self.first() is not None

    def _parse_lookup(self, lookup):
        if '__' in lookup:
            return lookup.split('__')
        else:
            return lookup, None

    def _get_field(self, field_name):
        try:
            return getattr(self._model, field_name)
        except AttributeError:
            raise FieldDoesNotExists('Field "{}" does not exist'.format(field_name))

    def _get_filter(self, field, operator, value):
        if operator == 'eq' or operator is None:
            return field == value
        elif operator == 'not':
            return field != value
        elif operator == 'lt':
            return field < value
        elif operator == 'lte':
            return field <= value
        elif operator == 'gt':
            return field > value
        elif operator == 'gte':
            return field >= value
        elif operator == 'between':
            return field.between(*value)
        elif operator == 'in':
            return field.is_in(value)
        elif operator == 'exists' and value:
            return field.exists(value)
        elif operator == 'exists' and not value:
            return field.does_not_exist(value)
        elif operator == 'startswith':
            return field.startswith(value)
        elif operator == 'contains':
            return field.contains(value)
        else:
            raise InvalidOperator('Invalid operator "{}"'.format(operator))

    def _pre_filter(self, field, field_name, operator, value):
        pass

    def filter(self, **kwargs):
        assert len(kwargs) == 1, 'Only one filter value is required'

        obj = self._clone()

        lookup, value = list(kwargs.items())[0]

        field_name, operator = obj._parse_lookup(lookup)

        field = obj._get_field(field_name)

        obj._pre_filter(field, field_name, operator, value)
        obj._filter = obj._get_filter(field, operator, value)
        if operator == 'between' and value[0] > value[1]:
            return self.none()
        return obj

    def get(self, **kwargs):
        obj = self
        if kwargs:
            obj = self.filter(**kwargs)
        obj._execute()
        if len(obj._results) == 1:
            return obj._results[0]
        elif len(obj._results) == 0:
            raise ObjectDoesNotExist
        else:
            raise MultipleObjectsReturned

    def count(self):
        if self._execution is not None:
            return len(self._results)
        if self._last_evaluated_key or self._limit:
            self._execute()
            return len(self._results)
        else:
            query = self._index.count if self._index else self._model.count

            if self._hash_key is None:
                raise DynamoDBQuerySetError('Hash key must be set')
            return query(
                self._hash_key,
                self._filter,
            )

    def delete(self):
        with self._model.batch_write() as batch:
            for obj in self:
                batch.delete(obj)

    def as_manager(cls):
        return DynamoDBManager.from_queryset(cls)()
    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)


class BaseDynamoDBManager:

    def __init__(self, index=None):
        super().__init__()
        self.model = None
        self._index = index

    def contribute_to_class(self, model):
        self.model = model

    def get_queryset(self):
        queryset = self._queryset_class(self.model)
        if self._index:
            return queryset.set_index(self._index)
        else:
            return queryset

    @classmethod
    def _get_queryset_methods(cls, queryset_class):
        def create_method(name, method):
            def manager_method(self, *args, **kwargs):
                return getattr(self.get_queryset(), name)(*args, **kwargs)
            manager_method.__name__ = method.__name__
            manager_method.__doc__ = method.__doc__
            return manager_method

        new_methods = {}
        for name, method in inspect.getmembers(queryset_class, predicate=inspect.isfunction):
            # Only copy missing methods.
            if hasattr(cls, name):
                continue
            # Only copy public methods or methods with the attribute `queryset_only=False`.
            queryset_only = getattr(method, 'queryset_only', None)
            if queryset_only or (queryset_only is None and name.startswith('_')):
                continue
            # Copy the method onto the manager.
            new_methods[name] = create_method(name, method)
        return new_methods

    @classmethod
    def from_queryset(cls, queryset_class, index=None, class_name=None):
        if class_name is None:
            class_name = '%sFrom%s' % (cls.__name__, queryset_class.__name__)
        return type(class_name, (cls,), {
            '_queryset_class': queryset_class,
            '_index': index,
            **cls._get_queryset_methods(queryset_class),
        })


class DynamoDBManager(BaseDynamoDBManager.from_queryset(DynamoDBQuerySet)):
    pass
