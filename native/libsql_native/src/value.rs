use rustler::{Binary, Decoder, Encoder, Term};

#[derive(Debug, Clone)]
pub struct Value(libsql::Value);

impl From<Value> for libsql::Value {
    fn from(value: Value) -> Self {
        value.0
    }
}

impl From<libsql::Value> for Value {
    fn from(value: libsql::Value) -> Self {
        Value(value)
    }
}

impl<'a> Decoder<'a> for Value {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        if let Ok(data) = term.atom_to_string() {
            if data == "nil" {
                return Ok(libsql::Value::Null.into());
            } else {
                return Ok(libsql::Value::Text(data).into());
            }
        }
        if let Ok(data) = term.decode::<i64>() {
            return Ok(libsql::Value::Integer(data).into());
        }
        if let Ok(data) = term.decode::<bool>() {
            if data {
                return Ok(libsql::Value::Integer(1).into());
            } else {
                return Ok(libsql::Value::Integer(0).into());
            }
        }
        if let Ok(data) = term.decode::<f64>() {
            return Ok(libsql::Value::Real(data).into());
        }
        if let Ok(data) = term.decode::<String>() {
            return Ok(libsql::Value::Text(data).into());
        }
        if let Ok(data) = term.decode::<Binary>() {
            return Ok(libsql::Value::Blob(data.as_slice().to_vec()).into());
        }

        return Err(rustler::Error::BadArg);
    }
}

impl Encoder for Value {
    fn encode<'a>(&self, env: rustler::Env<'a>) -> Term<'a> {
        match self.0 {
            libsql::Value::Null => None::<()>.encode(env),
            libsql::Value::Integer(data) => data.encode(env),
            libsql::Value::Real(data) => data.encode(env),
            libsql::Value::Text(ref data) => data.encode(env),
            libsql::Value::Blob(ref data) => data.encode(env),
        }
    }
}
