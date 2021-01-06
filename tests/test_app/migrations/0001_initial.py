from django.db import migrations

from pynamodb.constants import TABLE_STATUS, ACTIVE
from pynamodb.exceptions import TableDoesNotExist

from pydjamodb.connection import TableConnection


def clear_table(app, schema_editor):
    connection = TableConnection('pydjamodbtest')
    if connection.exists_table():
        connection.delete_table(wait=True)


def create_event_table(apps, schema_editor):
    connection = TableConnection('pydjamodbtest')
    connection.create_table(
        **{
            'attribute_definitions': [
                {'attribute_name': 'id', 'attribute_type': 'S'},
                {'attribute_name': 'date', 'attribute_type': 'S'},
                {'attribute_name': 'string', 'attribute_type': 'S'},
                {'attribute_name': 'number', 'attribute_type': 'N'},
            ],
            'key_schema': [
                {'key_type': 'RANGE', 'attribute_name': 'date'},
                {'key_type': 'HASH', 'attribute_name': 'id'}
            ],
            'global_secondary_indexes': [
                {
                    'index_name': 'string_number_index',
                    'key_schema': [
                        {'AttributeName': 'number', 'KeyType': 'RANGE'},
                        {'AttributeName': 'string', 'KeyType': 'HASH'}
                    ],
                    'projection': {'ProjectionType': 'ALL'}
                },
            ],
            'local_secondary_indexes': []
        },
        wait=True,
    )


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.RunPython(clear_table),
        migrations.RunPython(create_event_table),
    ]
