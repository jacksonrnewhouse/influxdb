use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::fmt::Debug;

use influxdb3_catalog::catalog::{DatabaseSchema, TableDefinition};
use influxdb3_wal::{FieldData, Row, WriteBatch};
use pyo3::types::PyDict;
use schema::InfluxColumnType;
use std::sync::Arc;

#[pyclass]
#[derive(Debug)]
pub struct PyWriteBatchIterator {
    table_definition: Arc<TableDefinition>,
    rows: Vec<Row>,
    current_index: usize,
}

#[pymethods]
impl PyWriteBatchIterator {
    fn next_point(&mut self) -> PyResult<Option<PyObject>> {
        if self.current_index >= self.rows.len() {
            return Ok(None);
        }

        Python::with_gil(|py| {
            let row = &self.rows[self.current_index];
            self.current_index += 1;

            // Import Point class
            let point_class = py
                .import_bound("influxdb_client_3.write_client.client.write.point")?
                .getattr("Point")
                .unwrap();

            // Create new Point instance with measurement name (table name)
            let point = point_class.call1((self.table_definition.table_name.as_ref(),))?;

            // Set timestamp
            point.call_method1("time", (row.time,))?;

            // Add fields based on column definitions and field data
            for field in &row.fields {
                if let Some(col_def) = self.table_definition.columns.get(&field.id) {
                    let field_name = col_def.name.as_ref();

                    match col_def.data_type {
                        InfluxColumnType::Tag => {
                            let FieldData::Tag(tag) = &field.value else {
                                // error out because we expect a tag
                                return Err(PyValueError::new_err(format!(
                                    "expect FieldData:Tag for tagged columns, not ${:?}",
                                    field
                                )));
                            };
                            point.call_method1("tag", (field_name, tag.as_str()))?;
                        }
                        InfluxColumnType::Timestamp => {}
                        // Do we need to check that the types match? Should already be validated.
                        InfluxColumnType::Field(_) => {
                            match &field.value {
                                FieldData::String(s) => {
                                    point.call_method1("field", (field_name, s.as_str()))?
                                }
                                FieldData::Integer(i) => {
                                    point.call_method1("field", (field_name, *i))?
                                }
                                FieldData::UInteger(u) => {
                                    point.call_method1("field", (field_name, *u))?
                                }
                                FieldData::Float(f) => {
                                    point.call_method1("field", (field_name, *f))?
                                }
                                FieldData::Boolean(b) => {
                                    point.call_method1("field", (field_name, *b))?
                                }
                                // TODO: Check what these should be
                                FieldData::Tag(t) => {
                                    point.call_method1("field", (field_name, t.as_str()))?
                                }
                                FieldData::Key(k) => {
                                    point.call_method1("field", (field_name, k.as_str()))?
                                }
                                FieldData::Timestamp(ts) => {
                                    point.call_method1("field", (field_name, *ts))?
                                }
                            };
                        }
                    }
                }
            }

            Ok(Some(point.into_py(py)))
        })
    }
}

#[pyclass]
#[derive(Debug)]
pub struct PyWriteBatch {
    pub write_batch: WriteBatch,
    pub schema: Arc<DatabaseSchema>,
}

#[pymethods]
impl PyWriteBatch {
    fn get_iterator_for_table(&self, table_name: &str) -> PyResult<PyWriteBatchIterator> {
        // Find table ID from name
        let table_id = self
            .schema
            .table_map
            .get_by_right(&Arc::from(table_name))
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Table '{}' not found",
                    table_name
                ))
            })?;

        // Get table chunks
        let chunks = self.write_batch.table_chunks.get(table_id).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "No data for table '{}'",
                table_name
            ))
        })?;

        // Get table definition
        let table_def = self.schema.tables.get(table_id).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Table definition not found for '{}'",
                table_name
            ))
        })?;

        Ok(PyWriteBatchIterator {
            table_definition: table_def.clone(),
            // TODO: avoid copying all the data at once.
            rows: chunks
                .chunk_time_to_chunk
                .values()
                .flat_map(|chunk| chunk.rows.clone())
                .collect(),
            current_index: 0,
        })
    }
}

impl PyWriteBatch {
    pub fn call_against_table(
        &self,
        table_name: &str,
        setup_code: &str,
        call_site: &str,
    ) -> PyResult<()> {
        let iterator = self.get_iterator_for_table(table_name)?;
        Python::with_gil(|py| {
            py.run_bound(setup_code, None, None)?;
            let py_func = py.eval_bound(call_site, None, None)?;
            py_func.call1((iterator,))?;
            Ok::<(), PyErr>(())
        })
    }
    pub fn print(&self) {
        // Use Python GIL to execute Python code
        Python::with_gil(|py| {
            // Get iterator for the specific table
            let iterator = self
                .get_iterator_for_table("measurement_data")
                .expect("Failed to get iterator for measurement_data");

            // Define the Python code as a string
            let python_code = r#"
import influxdb_client_3
def process_points(iterator):
    while True:
        point = iterator.next_point()
        if point is None:
            break
        print(f"Received point: {point}")
"#;

            // Create the Python function from our code
            let locals = PyDict::new_bound(py);
            py.run_bound(python_code, None, Some(&locals))
                .expect("should have defined process_points");

            // Call the Python function with our iterator
            locals
                .get_item("process_points")?
                .expect("should have function")
                .call1((iterator,))?;

            Ok::<(), PyErr>(())
        })
        .expect("Failed to execute Python code");
    }
}

// Module initialization
#[pymodule]
fn influxdb3_py_api(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWriteBatch>()?;
    m.add_class::<PyWriteBatchIterator>()?;
    Ok(())
}
