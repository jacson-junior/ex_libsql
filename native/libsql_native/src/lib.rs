use core::result;
use errors::{CONN_CONSUMED, CONN_NOT_AVAILABLE};
use libsql::Builder;
use open::LocalFlags;
use rustler::{
    Atom, Env, LocalPid, NifRecord, NifStruct, NifTaggedEnum, OwnedEnv, Resource, ResourceArc, Term,
};
use tokio::sync::Mutex;

pub mod duration;
pub mod open;
pub mod task;
pub mod value;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        idle,
        transaction,
        blob,
    }
}

mod errors {
    pub const CONN_NOT_AVAILABLE: &str = "connection not available";
    pub const CONN_CONSUMED: &str = "connection already consumed";
}

struct ConnectionRef(Mutex<Option<libsql::Connection>>);
struct StatementRef(Mutex<libsql::Statement>);
struct CursorRef(Mutex<libsql::Rows>);

impl Resource for ConnectionRef {}
impl Resource for StatementRef {}
impl Resource for CursorRef {}

#[derive(NifTaggedEnum)]
pub enum DatabaseOpenMode {
    Local(String, Option<LocalFlags>),
    LocalReplica(String, Option<LocalFlags>),
    Remote(String, String),
    RemoteReplica(String, String, String, Option<RemoteOpts>),
}

#[derive(NifTaggedEnum)]
pub enum CursorResult {
    Continue(Vec<Vec<value::Value>>),
    Halt(Vec<Vec<value::Value>>),
}

#[derive(NifRecord, Clone)]
#[tag = "remote_opts"]
pub struct RemoteOpts {
    read_your_writes: Option<bool>,
    sync_interval: Option<duration::Duration>,
}

#[derive(NifStruct, Clone, Debug)]
#[module = "ExLibSQL.Native.Result"]
struct Result {
    columns: Option<Vec<String>>,
    last_insert_id: Option<i64>,
    num_rows: Option<usize>,
    rows: Option<Vec<Vec<value::Value>>>,
}

#[derive(NifStruct, Clone)]
#[module = "ExLibSQL.Native.Connection"]
struct Connection {
    conn_ref: ResourceArc<ConnectionRef>,
}

#[derive(NifStruct, Clone)]
#[module = "ExLibSQL.Native.Statement"]
struct Statement {
    stmt_ref: ResourceArc<StatementRef>,
}

#[derive(NifStruct, Clone)]
#[module = "ExLibSQL.Native.Cursor"]
struct Cursor {
    cur_ref: ResourceArc<CursorRef>,
}

fn load(env: Env, _: Term) -> bool {
    env.register::<ConnectionRef>().is_ok()
        && env.register::<StatementRef>().is_ok()
        && env.register::<CursorRef>().is_ok()
}

#[rustler::nif]
fn open(mode: DatabaseOpenMode) -> result::Result<Connection, String> {
    let result = match mode {
        DatabaseOpenMode::Local(path, flags) => task::block_on(
            Builder::new_local(path)
                .flags(flags.unwrap_or_default().into())
                .build(),
        ),
        DatabaseOpenMode::LocalReplica(path, flags) => task::block_on(
            Builder::new_local_replica(path)
                .flags(flags.unwrap_or_default().into())
                .build(),
        ),
        DatabaseOpenMode::Remote(url, token) => {
            task::block_on(Builder::new_remote(url, token).build())
        }
        DatabaseOpenMode::RemoteReplica(path, url, token, opts) => task::block_on({
            let mut builder = Builder::new_remote_replica(path, url, token);

            if let Some(opts) = opts {
                if let Some(ryw) = opts.read_your_writes {
                    builder = builder.read_your_writes(ryw);
                }
                if let Some(si) = opts.sync_interval {
                    builder = builder.sync_interval(si.into());
                }
            }

            builder.build()
        }),
    };

    match result {
        Ok(database) => match database.connect() {
            Ok(conn) => Ok(Connection {
                conn_ref: ResourceArc::new(ConnectionRef(Mutex::new(Some(conn)))),
            }),
            Err(error) => Err(error.to_string()),
        },
        Err(error) => Err(error.to_string()),
    }
}

#[rustler::nif]
fn execute(
    connection: ResourceArc<ConnectionRef>,
    sql: String,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let out: result::Result<Result, String> = async {
            let state = connection.0.lock().await;

            let conn = state
                .as_ref()
                .ok_or_else(|| "No connection available".to_string())?;

            let result = conn.execute(&sql, params).await;

            match result {
                Ok(num_rows) => Ok(Result {
                    num_rows: Some(num_rows.try_into().unwrap()),
                    rows: None,
                    columns: None,
                    last_insert_id: Some(conn.last_insert_rowid()),
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
fn query(
    connection: ResourceArc<ConnectionRef>,
    sql: String,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Result, String> = async {
            let state = connection.0.lock().await;

            let conn = state
                .as_ref()
                .ok_or_else(|| "No connection available".to_string())?;

            let mut rows = conn.query(&sql, params).await.map_err(|e| e.to_string())?;

            let column_count: usize = rows.column_count().try_into().unwrap();
            let mut columns = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                let name = rows.column_name(idx.try_into().unwrap()).unwrap_or("");
                columns.push(name.to_string());
            }

            let mut data = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                let mut row_data = Vec::with_capacity(columns.len());
                for idx in 0..column_count {
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
                last_insert_id: Some(conn.last_insert_rowid()),
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
fn tx_status(connection: ResourceArc<ConnectionRef>, pid: LocalPid) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let out: result::Result<Atom, String> = async {
            let lock = connection.0.lock().await;

            let connection = match lock.as_ref() {
                Some(conn) => conn,
                None => return Err(CONN_NOT_AVAILABLE.to_string()),
            };

            match connection.is_autocommit() {
                true => Ok(atoms::idle()),
                false => Ok(atoms::transaction()),
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
fn prepare(
    connection: ResourceArc<ConnectionRef>,
    sql: String,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let out: result::Result<Statement, String> = async {
            let state = connection.0.lock().await;

            let conn = state
                .as_ref()
                .ok_or_else(|| "No connection available".to_string())?;

            let statement = conn.prepare(&sql).await.map_err(|e| e.to_string())?;

            Ok(Statement {
                stmt_ref: ResourceArc::new(StatementRef(Mutex::new(statement))),
            })
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| out)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn stmt_execute(
    connection: ResourceArc<ConnectionRef>,
    statement: ResourceArc<StatementRef>,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let out: result::Result<Result, String> = async {
            let _ = connection.0.lock().await;
            let mut stmt = statement.0.lock().await;

            let result = stmt.execute(params).await;

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
    connection: ResourceArc<ConnectionRef>,
    statement: ResourceArc<StatementRef>,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Result, String> = async {
            let _ = connection.0.lock().await;
            let mut stmt = statement.0.lock().await;

            let mut rows = stmt.query(params).await.map_err(|e| e.to_string())?;

            let column_count: usize = rows.column_count().try_into().unwrap();

            let mut columns = Vec::with_capacity(column_count);
            for idx in 0..column_count {
                let name = rows.column_name(idx.try_into().unwrap()).unwrap_or("");
                columns.push(name.to_string());
            }

            let mut data = Vec::new();
            while let Ok(Some(row)) = rows.next().await {
                let mut row_data = Vec::with_capacity(columns.len());
                for idx in 0..column_count {
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
fn stmt_reset(
    connection: ResourceArc<ConnectionRef>,
    statement: ResourceArc<StatementRef>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<(), String> = async {
            let _ = connection.0.lock().await;
            let mut stmt = statement.0.lock().await;

            stmt.reset();

            Ok(())
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn stmt_cursor(
    connection: ResourceArc<ConnectionRef>,
    statement: ResourceArc<StatementRef>,
    params: Vec<value::Value>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<Cursor, String> = async {
            let _ = connection.0.lock().await;
            let mut stmt = statement.0.lock().await;

            let rows = stmt.query(params).await.map_err(|e| e.to_string())?;

            Ok(Cursor {
                cur_ref: ResourceArc::new(CursorRef(Mutex::new(rows))),
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
fn stmt_fetch(
    connection: ResourceArc<ConnectionRef>,
    cursor: ResourceArc<CursorRef>,
    amount: i64,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<CursorResult, String> = async {
            let _ = connection.0.lock().await;
            let mut cursor = cursor.0.lock().await;

            let mut data = Vec::new();
            let mut has_more = true;

            for _ in 0..amount {
                match cursor.next().await {
                    Ok(Some(row)) => {
                        let column_count: usize = cursor.column_count().try_into().unwrap();
                        let mut row_data = Vec::with_capacity(column_count);

                        for idx in 0..column_count {
                            row_data.push(
                                row.get_value(idx as i32)
                                    .map(|d| d.into())
                                    .map_err(|e| e.to_string())?,
                            );
                        }
                        data.push(row_data);
                    }
                    Ok(None) => {
                        has_more = false;
                        break;
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }

            Ok(if has_more {
                CursorResult::Continue(data)
            } else {
                CursorResult::Halt(data)
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
fn stmt_finalize(
    connection: ResourceArc<ConnectionRef>,
    statement: ResourceArc<StatementRef>,
    pid: LocalPid,
) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result: result::Result<(), String> = async {
            let _ = connection.0.lock().await;
            let mut stmt = statement.0.lock().await;

            stmt.finalize();

            Ok(())
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

#[rustler::nif]
fn close(connection: ResourceArc<ConnectionRef>, pid: LocalPid) -> result::Result<(), String> {
    let _: tokio::task::JoinHandle<()> = task::spawn(async move {
        let mut local_env = OwnedEnv::new();

        let result = async {
            let mut conn = connection.0.lock().await;
            if let Some(connection) = conn.take() {
                drop(connection);
                Ok(())
            } else {
                Err(CONN_CONSUMED.to_string())
            }
        }
        .await;

        local_env
            .send_and_clear(&pid, |_| result)
            .expect("to send message");
    });

    Ok(())
}

rustler::init!("Elixir.ExLibSQL.Native", load = load);
