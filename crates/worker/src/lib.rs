pub(crate) mod database;
pub mod ingestion;

pub(crate) type Result<T, E = anyhow::Error> = core::result::Result<T, E>;
