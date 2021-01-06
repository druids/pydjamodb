from .models import dynamodb_model_classes


class DynamoDBTestMixin:

    def tearDown(self):
        super().tearDown()
        for model_class in dynamodb_model_classes:
            with model_class.batch_write() as batch:
                for item in model_class.scan():
                    batch.delete(item)
