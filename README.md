# Airflow DAG Testing Assignment

Project ini berisi implementasi dan pengujian DAG Apache Airflow bernama **data_validation_dag**.

## Deskripsi DAG
DAG dijalankan dengan konfigurasi:
- Schedule: `@daily`
- Owner: `data-engineering-team`
- Retries: 2 kali (delay 5 menit)
- Catchup: disabled
- Tags: `testing`, `validation`, `dag-testing`

Alur DAG:
start → extract_task → transform_task → load_task → end

## Penjelasan Task
- **extract_task**  
  Mensimulasikan proses ekstraksi data dan menyimpan hasil ke XCom dengan key `raw_data`.

- **transform_task**  
  Mengambil data dari XCom (`extract_task`), mengubah field `name` menjadi uppercase, lalu menyimpan hasil ke XCom dengan key `transformed_data`.

- **load_task**  
  Mengambil data hasil transformasi dari XCom dan mensimulasikan proses load dengan logging.

## Unit Testing
Unit test dibuat menggunakan `pytest` untuk:
- Memastikan fungsi `transform_data` menghasilkan output yang sesuai.
- Memastikan DAG dapat ter-load tanpa error menggunakan `DagBag`.

Test berada di:
`dags/tests/test_dag_testing_assignment.py`

## Integration Testing
Integration test dilakukan dengan menjalankan task secara individual:
```
airflow tasks test data_validation_dag extract_task 2025-10-22
airflow tasks test data_validation_dag transform_task 2025-10-22
airflow tasks test data_validation_dag load_task 2025-10-22
```
Seluruh task berhasil dijalankan tanpa error.

## Catatan
- Seluruh komunikasi antar-task menggunakan XCom.
- Kode menggunakan logging, error handling (`try-except`), dan docstring.
- Tidak ada credential yang di-hardcode.
