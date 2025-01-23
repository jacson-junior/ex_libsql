use core::result;
use errors::{TX_CONSUMED, TX_NOT_AVAILABLE};
use libsql::Builder;
use rustler::{
    Atom, Env, LocalPid, NifStruct, NifTaggedEnum, NifUnitEnum, OwnedEnv, Resource, ResourceArc,
    Term,
};
use tokio::sync::Mutex;

pub mod task;
pub mod value;

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

mod errors {
    pub const TX_NOT_AVAILABLE: &str = "transaction not available";
    pub const TX_CONSUMED: &str = "transaction already consumed";
}

struct ConnectionRef(libsql::Connection);
struct TransactionRef(Mutex<Option<libsql::Transaction>>);
struct StatementRef(Mutex<libsql::Statement>);

impl Resource for ConnectionRef {}
impl Resource for TransactionRef {}
impl Resource for StatementRef {}

#[derive(NifTaggedEnum)]
pub enum DatabaseOpenMode {
    Local(String),
    LocalReplica(String),
    Remote(String, String),
    RemoteReplica(String, String, String),
}

#[derive(NifUnitEnum, Default)]
pub enum TransactionBehavior {
    #[default]
    Deferred,
    Immediate,
    Exclusive,
    ReadOnly,
}

impl From<TransactionBehavior> for libsql::TransactionBehavior {
    fn from(behavior: TransactionBehavior) -> Self {
        match behavior {
            TransactionBehavior::Deferred => libsql::TransactionBehavior::Deferred,
            TransactionBehavior::Immediate => libsql::TransactionBehavior::Immediate,
            TransactionBehavior::Exclusive => libsql::TransactionBehavior::Exclusive,
            TransactionBehavior::ReadOnly => libsql::TransactionBehavior::ReadOnly,
        }
    }
}

#[derive(NifStruct, Clone, Debug)]
#[module = "LibSQL.Native.Result"]
struct Result {
    columns: Option<Vec<String>>,
    last_insert_id: Option<i64>,
    num_rows: Option<usize>,
    rows: Option<Vec<Vec<value::Value>>>,
}

#[derive(NifStruct, Clone)]
#[module = "LibSQL.Native.Connection"]
struct Connection {
    conn_ref: ResourceArc<ConnectionRef>,
}

#[derive(NifStruct, Clone)]
#[module = "LibSQL.Native.Transaction"]
struct Transaction {
    tx_ref: ResourceArc<TransactionRef>,
}

#[derive(NifStruct, Clone)]
#[module = "LibSQL.Native.Statement"]
struct Statement {
    stmt_ref: ResourceArc<StatementRef>,
}

fn load(env: Env, _: Term) -> bool {
    env.register::<ConnectionRef>().is_ok()
        && env.register::<TransactionRef>().is_ok()
        && env.register::<StatementRef>().is_ok()
}

#[rustler::nif]
fn open(mode: DatabaseOpenMode) -> result::Result<Connection, String> {
    let result = match mode {
        DatabaseOpenMode::Local(path) => task::block_on(Builder::new_local(path).build()),
        DatabaseOpenMode::LocalReplica(path) => {
            task::block_on(Builder::new_local_replica(path).build())
        }
        DatabaseOpenMode::Remote(host, port) => {
            task::block_on(Builder::new_remote(host, port).build())
        }
        DatabaseOpenMode::RemoteReplica(host, port, replica) => {
            task::block_on(Builder::new_remote_replica(host, port, replica).build())
        }
    };

    match result {
        Ok(database) => match database.connect() {
            Ok(conn) => Ok(Connection {
                conn_ref: ResourceArc::new(ConnectionRef(conn)),
            }),
            Err(error) => return Err(error.to_string()),
        },
        Err(error) => Err(error.to_string()),
    }
}

#[rustler::nif]
fn execute(
    resource: ResourceArc<ConnectionRef>,
    stmt: String,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();
        let result = resource.0.execute(&stmt, params).await;

        let out = match result {
            Ok(num_rows) => Ok(Result {
                num_rows: Some(num_rows.try_into().unwrap()),
                rows: None,
                columns: None,
                last_insert_id: Some(resource.0.last_insert_rowid()),
            }),
            Err(err) => Err(err.to_string()),
        };

        local_env
            .send_and_clear(&pid, |_| out)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn query(
    resource: ResourceArc<ConnectionRef>,
    stmt: String,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Result, String> = async {
            let mut rows = resource
                .0
                .query(&stmt, params)
                .await
                .map_err(|e| e.to_string())?;
            let column_count: usize = rows.column_count().try_into().unwrap();

            let mut columns = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                let name = rows.column_name(idx.try_into().unwrap()).unwrap_or("");
                columns.push(name.to_string());
            }

            let mut data = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                let mut row_data = Vec::with_capacity(columns.len());
                for (idx, _) in columns.iter().enumerate() {
                    row_data.push(
                        row.get_value(idx as i32)
                            .map(|d| d.into())
                            .map_err(|e| e.to_string())?,
                    );
                }
                data.push(row_data);
            }

            Ok(Result {
                num_rows: Some(data.len()),
                rows: Some(data),
                columns: Some(columns),
                last_insert_id: Some(resource.0.last_insert_rowid()),
            })
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn transaction(
    resource: ResourceArc<ConnectionRef>,
    behaviour: TransactionBehavior,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Transaction, String> = async {
            let transaction = resource
                .0
                .transaction_with_behavior(behaviour.into())
                .await
                .map_err(|err| err.to_string())?;

            Ok(Transaction {
                tx_ref: ResourceArc::new(TransactionRef(Mutex::new(Some(transaction)))),
            })
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn commit(resource: ResourceArc<TransactionRef>, pid: LocalPid) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<(), String> = async {
            let transaction = {
                let mut lock = resource.0.lock().await;
                lock.take().ok_or(TX_CONSUMED.to_string())?
            };

            match transaction.commit().await {
                Ok(_) => Ok(()),
                Err(err) => Err(err.to_string()),
            }
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn rollback(resource: ResourceArc<TransactionRef>, pid: LocalPid) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<(), String> = async {
            let transaction = {
                let mut lock = resource.0.lock().await;
                lock.take().ok_or(TX_CONSUMED.to_string())?
            };

            match transaction.rollback().await {
                Ok(_) => Ok(()),
                Err(err) => Err(err.to_string()),
            }
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn tx_execute(
    resource: ResourceArc<TransactionRef>,
    stmt: String,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let out: result::Result<Result, String> = async {
            let lock = resource.0.lock().await;

            let transaction = match lock.as_ref() {
                Some(tx) => tx,
                None => return Err(TX_NOT_AVAILABLE.to_string()),
            };

            let result = transaction.execute(&stmt, params).await;

            match result {
                Ok(num_rows) => Ok(Result {
                    num_rows: Some(num_rows.try_into().unwrap()),
                    rows: None,
                    columns: None,
                    last_insert_id: Some(transaction.last_insert_rowid()),
                }),
                Err(err) => Err(err.to_string()),
            }
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| out)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn tx_query(
    resource: ResourceArc<TransactionRef>,
    stmt: String,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Result, String> = async {
            let lock = resource.0.lock().await;

            let transaction = match lock.as_ref() {
                Some(tx) => tx,
                None => return Err(TX_NOT_AVAILABLE.to_string()),
            };

            let mut rows = transaction
                .query(&stmt, params)
                .await
                .map_err(|e| e.to_string())?;
            let column_count: usize = rows.column_count().try_into().unwrap();

            let mut columns = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                let name = rows.column_name(idx.try_into().unwrap()).unwrap_or("");
                columns.push(name.to_string());
            }

            let mut data = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                let mut row_data = Vec::with_capacity(columns.len());
                for (idx, _) in columns.iter().enumerate() {
                    row_data.push(
                        row.get_value(idx as i32)
                            .map(|d| d.into())
                            .map_err(|e| e.to_string())?,
                    );
                }
                data.push(row_data);
            }

            let last_insert_id = transaction.last_insert_rowid();

            Ok(Result {
                num_rows: Some(data.len()),
                rows: Some(data),
                columns: Some(columns),
                last_insert_id: Some(last_insert_id),
            })
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn tx_prepare(
    resource: ResourceArc<TransactionRef>,
    stmt: String,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Statement, String> = async {
            let lock = resource.0.lock().await;

            let transaction = match lock.as_ref() {
                Some(tx) => tx,
                None => return Err(TX_NOT_AVAILABLE.to_string()),
            };

            let statement = transaction
                .prepare(&stmt)
                .await
                .map_err(|e| e.to_string())?;

            Ok(Statement {
                stmt_ref: ResourceArc::new(StatementRef(Mutex::new(statement))),
            })
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn prepare(
    resource: ResourceArc<ConnectionRef>,
    stmt: String,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Statement, String> = async {
            let statement = resource.0.prepare(&stmt).await.map_err(|e| e.to_string())?;

            Ok(Statement {
                stmt_ref: ResourceArc::new(StatementRef(Mutex::new(statement))),
            })
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn stmt_execute(
    resource: ResourceArc<StatementRef>,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let out: result::Result<Result, String> = async {
            let mut statement = resource.0.lock().await;

            let result = statement.execute(params).await;

            match result {
                Ok(num_rows) => Ok(Result {
                    num_rows: Some(num_rows.try_into().unwrap()),
                    rows: None,
                    columns: None,
                    last_insert_id: None,
                }),
                Err(err) => Err(err.to_string()),
            }
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| out)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn stmt_query(
    resource: ResourceArc<StatementRef>,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Result, String> = async {
            let mut statement = resource.0.lock().await;

            let mut rows = statement.query(params).await.map_err(|e| e.to_string())?;
            let column_count: usize = rows.column_count().try_into().unwrap();

            let mut columns = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                let name = rows.column_name(idx.try_into().unwrap()).unwrap_or("");
                columns.push(name.to_string());
            }

            let mut data = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                let mut row_data = Vec::with_capacity(columns.len());
                for (idx, _) in columns.iter().enumerate() {
                    row_data.push(
                        row.get_value(idx as i32)
                            .map(|d| d.into())
                            .map_err(|e| e.to_string())?,
                    );
                }
                data.push(row_data);
            }

            Ok(Result {
                num_rows: Some(data.len()),
                rows: Some(data),
                columns: Some(columns),
                last_insert_id: None,
            })
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn close(conn: ResourceArc<ConnectionRef>) -> Atom {
    drop(conn);
    atoms::ok()
}

rustler::init!("Elixir.LibSQL.Native", load = load);
