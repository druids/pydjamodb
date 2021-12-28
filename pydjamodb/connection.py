import time

from django.conf import settings

from pynamodb.connection.table import TableConnection as BaseTableConnection
from pynamodb.exceptions import TableDoesNotExist

from botocore.client import ClientError


class TableConnection(BaseTableConnection):

    def __init__(self, table_name,
                 region=None,
                 host=None,
                 connect_timeout_seconds=None,
                 read_timeout_seconds=None,
                 max_retry_attempts=None,
                 base_backoff_ms=None,
                 max_pool_connections=None,
                 extra_headers=None,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None):
        table_name = '{}-{}'.format(settings.PYDJAMODB_DATABASE['TABLE_PREFIX'], table_name)
        region = settings.PYDJAMODB_DATABASE.get('AWS_REGION') if region is None else region
        host = settings.PYDJAMODB_DATABASE.get('HOST') if host is None else host
        aws_access_key_id = (
            settings.PYDJAMODB_DATABASE.get('AWS_ACCESS_KEY_ID') if aws_access_key_id is None
            else aws_access_key_id
        )
        aws_secret_access_key = (
            settings.PYDJAMODB_DATABASE.get('AWS_SECRET_ACCESS_KEY') if aws_secret_access_key is None
            else aws_secret_access_key
        )
        aws_session_token = (
            settings.PYDJAMODB_DATABASE.get('AWS_SESSION_TOKEN') if aws_session_token is None
            else aws_session_token
        )

        super().__init__(
            table_name,
            region=region,
            host=host,
            connect_timeout_seconds=connect_timeout_seconds,
            read_timeout_seconds=read_timeout_seconds,
            max_retry_attempts=max_retry_attempts,
            base_backoff_ms=base_backoff_ms,
            max_pool_connections=max_pool_connections,
            extra_headers=extra_headers,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

    def create_table(self,
                     attribute_definitions=None,
                     key_schema=None,
                     read_capacity_units=None,
                     write_capacity_units=None,
                     global_secondary_indexes=None,
                     local_secondary_indexes=None,
                     stream_specification=None,
                     billing_mode=None,
                     tags=None,
                     wait=False,
                     set_point_in_time_recovery=None):

        billing_mode = settings.PYDJAMODB_DATABASE.get('BILLING_MODE') if billing_mode is None else billing_mode
        set_point_in_time_recovery = (
            settings.PYDJAMODB_DATABASE.get('POINT_IN_TIME_RECOVERY', False) if set_point_in_time_recovery is None
            else set_point_in_time_recovery
        )
        if not stream_specification:
            stream_specification = settings.PYDJAMODB_DATABASE.get('STREAM_SPECIFICATION')

        if tags is None and 'TAGS' in settings.PYDJAMODB_DATABASE:
            tags = {
                k: v.format(table_name=self.table_name) for k, v in settings.PYDJAMODB_DATABASE['TAGS'].items()
            }
        result = self.connection.create_table(
            self.table_name,
            attribute_definitions=attribute_definitions,
            key_schema=key_schema,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            global_secondary_indexes=global_secondary_indexes,
            local_secondary_indexes=local_secondary_indexes,
            stream_specification=stream_specification,
            billing_mode=billing_mode,
            tags=tags
        )

        if wait or set_point_in_time_recovery:
            self.connection.client.get_waiter('table_exists').wait(
                TableName=self.table_name,
                WaiterConfig={
                    'Delay': 10,
                    'MaxAttempts': self.connection._max_retry_attempts_exception
                }
            )

        if set_point_in_time_recovery:
            self.set_point_in_time_recovery(enabled=True)

        return result

    def delete_table(self, wait=False):
        """
        Performs the DeleteTable operation and returns the result
        """
        result = self.connection.delete_table(self.table_name)
        if wait:
            self.connection.client.get_waiter('table_not_exists').wait(
                TableName=self.table_name,
                WaiterConfig={
                    'Delay': 10,
                    'MaxAttempts': self.connection._max_retry_attempts_exception
                }
            )
        return result

    def exists_table(self):
        try:
            self.describe_table()
            return True
        except TableDoesNotExist:
            return False

    def set_point_in_time_recovery(self, enabled=True):
        for i in range(0, self.connection._max_retry_attempts_exception + 1):
            try:
                self.connection.client.update_continuous_backups(
                    TableName=self.table_name,
                    PointInTimeRecoverySpecification={
                        'PointInTimeRecoveryEnabled': enabled
                    }
                )
                break
            except ClientError:
                if i == self.connection._max_retry_attempts_exception:
                    raise
                time.sleep(10)


class TestTableConnection:

    def __init__(self, wrapped_connection, prefix=None):
        self._wrapped_connection = wrapped_connection
        self._is_test_clean_required = False
        if prefix:
            self._wrapped_connection.table_name = 'test_{}_{}'.format(prefix, self._wrapped_connection.table_name)
        else:
            self._wrapped_connection.table_name = 'test_{}'.format(self._wrapped_connection.table_name)

    def __getattr__(self, attr):
        return getattr(self._wrapped_connection, attr)

    def update_item(self, *args, **kwargs):
        self._is_test_clean_required = True
        return self._wrapped_connection.update_item(*args, **kwargs)

    def put_item(self, *args, **kwargs):
        self._is_test_clean_required = True
        return self._wrapped_connection.put_item(*args, **kwargs)

    def batch_write_item(self, *args, **kwargs):
        self._is_test_clean_required = True
        return self._wrapped_connection.batch_write_item(*args, **kwargs)

    def post_test_clean(self, model_class):
        if self._is_test_clean_required:
            with model_class.batch_write() as batch:
                for item in model_class.scan():
                    batch.delete(item)
        self._is_test_clean_required = False
