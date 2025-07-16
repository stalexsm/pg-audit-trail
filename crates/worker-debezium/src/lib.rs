pub(crate) mod database;
pub mod debezium;

pub(crate) type Result<T, E = anyhow::Error> = core::result::Result<T, E>;
