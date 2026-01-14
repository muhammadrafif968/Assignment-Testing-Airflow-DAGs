from dag_testing_assignment import transform_data
from airflow.models import DagBag


def test_transform_data():
    data = [
        {"name": "apple", "price": 1000},
        {"name": "banana", "price": 500},
    ]

    result = transform_data(data)

    assert result == [
        {"name": "APPLE", "price": 1000},
        {"name": "BANANA", "price": 500},
    ]


def test_dag_integrity():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)

    assert not dagbag.import_errors

    dag = dagbag.get_dag("data_validation_dag")
    assert dag is not None
    assert len(dag.tasks) >= 3
