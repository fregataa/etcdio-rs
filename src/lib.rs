use pyo3::prelude::*;
use etcd_client::Client;

#[pyclass]
struct EtcdClient {
    address: String,
}

#[pymethods]
impl EtcdClient {
    #[new]
    fn new(address: &str) -> Self {
        Self{address: address.to_string()}
    }

    fn hello<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let addr = self.address.to_string();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            Ok(format!("Hello, {}", addr))
        })
    }

    fn put<'p>(
        &self,
        py: Python<'p>,
        key: &str,
        value: &str,
    ) -> PyResult<&'p PyAny> {
        let key = key.to_string();
        let value = value.to_string();
        let addr = self.address.to_string();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut conn = Client::connect([addr], None).await.unwrap();
            conn.put(key, value, None).await.unwrap();
            Ok(())
        })
    }

    fn get<'p>(
        &self,
        py: Python<'p>,
        key: &str,
    ) -> PyResult<&'p PyAny> {
        let key = key.to_string();
        let addr = self.address.to_string();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut conn = Client::connect([addr], None).await.unwrap();
            let key_str = &key[..];
            let resp = conn.get(key_str, None).await.unwrap();
            if let Some(kv) = resp.kvs().first() {
                return Ok(kv.value_str().unwrap().to_string());
            }
            Ok(format!("{:?} is not found", key_str))
        })
    }

    fn delete<'p>(
        &self,
        py: Python<'p>,
        key: &str,
    ) -> PyResult<&'p PyAny> {
        let key = key.to_string();
        let addr = self.address.to_string();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut conn = Client::connect([addr], None).await.unwrap();
            conn.delete(key, None).await.unwrap();
            Ok(())
        })
    }

    fn lock<'p>(
        &self,
        py: Python<'p>,
        name: &str,
    ) -> PyResult<&'p PyAny> {
        let name = name.to_string();
        let addr = self.address.to_string();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut conn = Client::connect([addr], None).await.unwrap();
            let resp = conn.lock(name, None).await.unwrap();
            let key_str = String::from_utf8_lossy(resp.key()).into_owned();
            Ok(key_str)
        })
    }

    fn unlock<'p>(
        &self,
        py: Python<'p>,
        key: &str,
    ) -> PyResult<&'p PyAny> {
        let key = key.to_string();
        let addr = self.address.to_string();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut conn = Client::connect([addr], None).await.unwrap();
            conn.unlock(key).await.unwrap();
            Ok(())
        })
    }
}

#[pymodule]
fn etcdio_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EtcdClient>()?;
    Ok(())
}
