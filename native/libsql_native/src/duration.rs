use std::time;

use rustler::{Decoder, Encoder, Term};

#[derive(Debug, Clone)]
pub struct Duration(time::Duration);

impl From<Duration> for time::Duration {
    fn from(value: Duration) -> Self {
        value.0
    }
}

impl From<time::Duration> for Duration {
    fn from(value: time::Duration) -> Self {
        Duration(value)
    }
}

impl Default for Duration {
    #[inline]
    fn default() -> Duration {
        Duration(time::Duration::default())
    }
}

impl<'a> Decoder<'a> for Duration {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        if let Ok(millis) = term.decode::<u64>() {
            return Ok(Duration(time::Duration::from_millis(millis)));
        }

        return Err(rustler::Error::BadArg);
    }
}

impl Encoder for Duration {
    fn encode<'a>(&self, env: rustler::Env<'a>) -> Term<'a> {
        (self.0.as_millis() as u64).encode(env)
    }
}
