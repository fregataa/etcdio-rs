use pyo3::prelude::*;
use etcd_client::{
    Client, Txn as PbTxn, CompareOp as PbCompareOp, Compare as PbCompare,
    TxnOp as PbTxnOp,
};
// use etcd_client::{Client, Txn};
// use etcd_client::rpc::etcdserverpb::{CompareTarget};

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

    // fn txn<'p>(
    //     &self,
    //     py: Python<'p>,
    //     when: &str,
    //     and_then: &str,
    //     or_else: &str,
    // ) -> PyResult<&'p PyAny> {
    //     let txn = Txn::new();
    //     let addr = self.address.to_string();
    //     pyo3_asyncio::tokio::future_into_py(py, async move {
    //         let mut conn = Client::connect([addr], None).await.unwrap();
    //         let resp = conn.txn(txn).await.unwrap();
    //         Ok(resp.succeeded())
    //     })
    // }
}

#[derive(Clone, Copy)]
#[pyclass]
enum CompareOp{
    Equal,
    Greater,
    Less,
    NotEqual,
}

#[derive(Clone, Copy)]
#[pyclass]
enum TargetUnion{
    Version,
    CreateRevision,
    ModRevision,
    Value,
    Lease,
}

#[derive(Clone)]
#[pyclass]
struct Compare{
    key: String,
    compare_op: CompareOp,
    target: String,
    target_union: TargetUnion,
    range_end: Option<String>,
    is_prefix: bool,
}

impl Compare{
    fn get_prefix(&self, key: &str) -> String {
        let key = key.as_bytes();
        for (i, v) in key.iter().enumerate().rev() {
            if *v < 0xFF {
                let mut end = Vec::from(&key[..=i]);
                end[i] = *v + 1;
                return String::from_utf8(end).unwrap();
            }
        }

        String::from_utf8(vec![0]).unwrap()
    }
}

#[pymethods]
impl Compare{
    #[new]
    fn new(
        key: &str,
        cmp: CompareOp,
        target: &str,
        target_union: TargetUnion,
    ) -> Self {
        Compare{
            key: key.to_string(),
            compare_op: cmp,
            target: target.to_string(),
            target_union: target_union,
            range_end: None,
            is_prefix: false,
        }
    }

    fn with_range<'p>(
        &self,
        _py: Python<'p>,
        end: &str,
    ) -> PyResult<Py<Compare>> {
        Ok(Python::with_gil(|py| Py::new(py, Compare{
            key: self.key.to_string(),
            compare_op: self.compare_op,
            target: self.target.to_string(),
            target_union: self.target_union,
            range_end: Some(end.to_string()),
            is_prefix: false,
        }).unwrap()))
    }

    fn with_prefix<'p>(
        &self,
        _py: Python<'p>,
    ) -> PyResult<Py<Compare>> {
        let end = self.get_prefix(&self.key.to_string());
        Ok(Python::with_gil(|py| Py::new(py, Compare{
            key: self.key.to_string(),
            compare_op: self.compare_op,
            target: self.target.to_string(),
            target_union: self.target_union,
            range_end: Some(end),
            is_prefix: true,
        }).unwrap()))
    }
}

fn serialize_cmp(cmp: &Compare) -> PbCompare {
    let pb_cmp_op = match cmp.compare_op {
        CompareOp::Equal => PbCompareOp::Equal,
        CompareOp::Greater => PbCompareOp::Greater,
        CompareOp::Less => PbCompareOp::Less,
        CompareOp::NotEqual => PbCompareOp::NotEqual,
    };
    let mut pb_cmp =  match cmp.target_union {
        TargetUnion::Version => PbCompare::version(cmp.key.to_string(), pb_cmp_op, cmp.target.parse::<i64>().unwrap()),
        TargetUnion::CreateRevision => PbCompare::create_revision(cmp.key.to_string(), pb_cmp_op, cmp.target.parse::<i64>().unwrap()),
        TargetUnion::ModRevision => PbCompare::mod_revision(cmp.key.to_string(), pb_cmp_op, cmp.target.parse::<i64>().unwrap()),
        TargetUnion::Value => PbCompare::value(cmp.key.to_string(), pb_cmp_op, cmp.target.to_string()),
        TargetUnion::Lease => PbCompare::lease(cmp.key.to_string(), pb_cmp_op, cmp.target.parse::<i64>().unwrap()),
    };
    if let Some(range_end) = &cmp.range_end {
        pb_cmp = pb_cmp.with_range(range_end.to_string());
    }
    if cmp.is_prefix {
        pb_cmp = pb_cmp.with_prefix();
    }
    pb_cmp
}

#[derive(Clone, Copy)]
enum TxnOpEnum{
    Put,
    Get,
    Delete,
    Txn,
}

#[derive(Clone)]
#[pyclass]
struct TxnOp{
    key: Option<String>,
    value: Option<String>,
    txn: Option<Txn>,
    op: Option<TxnOpEnum>,
}

#[pymethods]
impl TxnOp{
    #[new]
    fn new() -> Self {
        TxnOp{
            key: None,
            value: None,
            txn: None,
            op: None,
        }
    }

    fn put<'p>(
        &self,
        _py: Python<'p>,
        key: &str,
        value: &str,
    ) -> PyResult<Py<TxnOp>> {
        let key = key.to_string();
        let value = value.to_string();
        Ok(Python::with_gil(|py| Py::new(py, TxnOp{
            key: Some(key),
            value: Some(value),
            txn: None,
            op: Some(TxnOpEnum::Put),
        }).unwrap()))
    }

    fn get<'p>(
        &self,
        _py: Python<'p>,
        key: &str,
    ) -> PyResult<Py<TxnOp>> {
        let key = key.to_string();
        Ok(Python::with_gil(|py| Py::new(py, TxnOp{
            key: Some(key),
            value: None,
            txn: None,
            op: Some(TxnOpEnum::Get),
        }).unwrap()))
    }

    fn delete<'p>(
        &self,
        _py: Python<'p>,
        key: &str,
    ) -> PyResult<Py<TxnOp>> {
        let key = key.to_string();
        Ok(Python::with_gil(|py| Py::new(py, TxnOp{
            key: Some(key),
            value: None,
            txn: None,
            op: Some(TxnOpEnum::Delete),
        }).unwrap()))
    }

    fn txn<'p>(
        &self,
        _py: Python<'p>,
        txn: Txn,
    ) -> PyResult<Py<TxnOp>> {
        Ok(Python::with_gil(|py| Py::new(py, TxnOp{
            key: None,
            value: None,
            txn: Some(txn),
            op: Some(TxnOpEnum::Txn),
        }).unwrap()))
    }
}

fn serialize_txn_op(txn_op: &TxnOp) -> PbTxnOp {
    let key_cp = if let Some(key) = &txn_op.key {
        key.to_string()
    } else {
        "".to_string()
    };
    let value_cp = if let Some(value) = &txn_op.value {
        value.to_string()
    } else {
        "".to_string()
    };
    let txn_cp = if let Some(txn) = &txn_op.txn {
        serialize_txn(txn)
    } else {
        PbTxn::new()
    };
    match txn_op.op.unwrap() {
        TxnOpEnum::Put => PbTxnOp::put(key_cp, value_cp, None),
        TxnOpEnum::Get => PbTxnOp::get(key_cp, None),
        TxnOpEnum::Delete => PbTxnOp::delete(key_cp, None),
        TxnOpEnum::Txn => PbTxnOp::txn(txn_cp),
    }
}

#[derive(Clone)]
#[pyclass]
struct Txn{
    when_cmps: Option<Vec<Compare>>,
    and_then_ops: Option<Vec<TxnOp>>,
    or_else_ops: Option<Vec<TxnOp>>,
}

#[pymethods]
impl Txn{
    #[new]
    fn new() -> Self {
        Txn{
            when_cmps: None,
            and_then_ops: None,
            or_else_ops: None,
        }
    }

    fn when<'p>(
        this: Py<Self>,
        py: Python,
        cmps: Vec<Compare>,
    ) -> PyResult<()> {
        let cell: &PyCell<Txn> = this.as_ref(py);
        let mut slf = cell.try_borrow_mut().unwrap();
        slf.when_cmps = Some(cmps);
        Ok(())
        // self.when_cmps = Some(cmps);
        // Ok(Python::with_gil(|py| Py::new(py, *self).unwrap()))
    }

    fn and_then<'p>(
        this: Py<Self>,
        py: Python,
        ops: Vec<TxnOp>,
    ) -> PyResult<()> {
        let cell: &PyCell<Txn> = this.as_ref(py);
        let mut slf = cell.try_borrow_mut().unwrap();
        slf.and_then_ops = Some(ops);
        Ok(())
        // self.and_then_ops = Some(ops);
        // Ok(Python::with_gil(|py| Py::new(py, *self).unwrap()))
    }

    fn or_else<'p>(
        this: Py<Self>,
        py: Python,
        ops: Vec<TxnOp>,
    ) -> PyResult<()> {
        let cell: &PyCell<Txn> = this.as_ref(py);
        let mut slf = cell.try_borrow_mut().unwrap();
        slf.or_else_ops = Some(ops);
        Ok(())
        // self.or_else_ops = Some(ops);
        // Ok(Python::with_gil(|py| Py::new(py, *self).unwrap()))
    }
}

fn serialize_txn(txn: &Txn) -> PbTxn {
    let mut pb_txn = PbTxn::new();
    if let Some(whens) = &txn.when_cmps {
        let serialized_whens: Vec<PbCompare> = whens.iter().map(|when| serialize_cmp(when)).collect();
        pb_txn = pb_txn.when(serialized_whens);
    }

    if let Some(and_then) = &txn.and_then_ops {
        let serialized_and_thens: Vec<PbTxnOp> = and_then.iter().map(|and_then| serialize_txn_op(and_then)).collect();
        pb_txn = pb_txn.and_then(serialized_and_thens);
    }
    pb_txn
}

#[pymodule]
fn etcdio_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<CompareOp>()?;
    m.add_class::<Compare>()?;
    m.add_class::<TxnOp>()?;
    m.add_class::<Txn>()?;
    m.add_class::<EtcdClient>()?;
    m.add_class::<TargetUnion>()?;
    m.add_class::<EtcdClient>()?;

    Ok(())
}
