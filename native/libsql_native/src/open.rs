use libsql::ffi::{SQLITE_OPEN_CREATE, SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE};
use rustler::{Decoder, Encoder, Term};

#[derive(Debug, Clone)]
pub struct LocalFlags(libsql::OpenFlags);

impl From<LocalFlags> for libsql::OpenFlags {
    fn from(value: LocalFlags) -> Self {
        value.0
    }
}

impl From<libsql::OpenFlags> for LocalFlags {
    fn from(value: libsql::OpenFlags) -> Self {
        LocalFlags(value)
    }
}

impl Default for LocalFlags {
    #[inline]
    fn default() -> LocalFlags {
        LocalFlags(libsql::OpenFlags::default())
    }
}

impl<'a> Decoder<'a> for LocalFlags {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        if let Ok(data) = term.decode::<i32>() {
            let mut open_flags = libsql::OpenFlags::default();
            if (data & SQLITE_OPEN_READONLY) != 0 {
                open_flags |= libsql::OpenFlags::SQLITE_OPEN_READ_ONLY;
            }
            if (data & SQLITE_OPEN_READWRITE) != 0 {
                open_flags |= libsql::OpenFlags::SQLITE_OPEN_READ_WRITE;
            }
            if (data & SQLITE_OPEN_CREATE) != 0 {
                open_flags |= libsql::OpenFlags::SQLITE_OPEN_CREATE;
            }

            return Ok(open_flags.into());
        }

        return Err(rustler::Error::BadArg);
    }
}

impl Encoder for LocalFlags {
    fn encode<'a>(&self, env: rustler::Env<'a>) -> Term<'a> {
        let mut flags = 0;
        if self.0.contains(libsql::OpenFlags::SQLITE_OPEN_READ_ONLY) {
            flags |= SQLITE_OPEN_READONLY;
        }
        if self.0.contains(libsql::OpenFlags::SQLITE_OPEN_READ_WRITE) {
            flags |= SQLITE_OPEN_READWRITE;
        }
        if self.0.contains(libsql::OpenFlags::SQLITE_OPEN_CREATE) {
            flags |= SQLITE_OPEN_CREATE;
        }

        flags.encode(env)
    }
}
