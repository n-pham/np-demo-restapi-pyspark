from airflow.models import DagBag
import pytest

@pytest.mark.skip(reason="needs Airflow running")
def test_no_import_errors():
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id='daily_transactions_dag')
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
    assert dag is not None