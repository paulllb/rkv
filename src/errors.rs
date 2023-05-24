use std::result;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedReadFromDataFile,

    #[error("failed to write to data file")]
    FailedWriteToDataFile,

    #[error("failed sync data to file")]
    FailedSyncDataFile,

    #[error("failed to open file")]
    FailedToOpenDataFile,

    #[error("key is empty")]
    KeyIsEmpty,

    #[error("memory index failed to update")]
    IndexUpdateFailed,

    #[error("key is not found in database")]
    KeyNotFound,

    #[error("datafile is not found in database")]
    DataFileNotFound,

    #[error("dir path is empty")]
    DirPathEmpty,

    #[error("data file size must be greater than 0")]
    DataFileSizeIllegal,

    #[error("create dir path all error")]
    FailedToCreateDatabaseDir,
}

pub type Result<T> = result::Result<T, Errors>;
