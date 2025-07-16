pub mod app;
pub(crate) mod database;

pub(crate) type Result<T, E = anyhow::Error> = core::result::Result<T, E>;
