use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    TableNotFound(String),
    TableExists(String),
    ColumnNotFound(String),
    ColumnCountMismatch { expected: usize, got: usize },
    TypeMismatch { expected: String, got: String },
    DuplicateKey(String),
    NotNullViolation(String),
    MultiplePrimaryKeys,
    MissingPrimaryKey,
    ParseError(String),
    TxnNotActive,
    TxnConflict(String),
    Io(String),
    Corrupted(String),
    NotSupported(String),
    IndexNotFound(String),
    IndexExists(String),
    ForeignKeyViolation(String),
    InvalidSchema(String),
    DatabaseNotFound(String),
    DatabaseExists(String),
    QueryTimeout,
    ReadOnlyReplica,
    InvalidQuery(String),
    ExecutionError(String),
    /// Valid statement, wrong node role (e.g. PROMOTE on a primary).
    InvalidOperation(String),
    /// RBAC denial: user lacks the privilege on the object. The Display
    /// text is the MySQL 1142 message; the PG frontend reformats the
    /// fields into its 42501 text (see `wire/pg/codec.rs`).
    AccessDenied {
        user: String,
        command: String,
        object: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::QueryTimeout => write!(f, "Query execution was interrupted, maximum statement execution time exceeded"),
            Error::DatabaseNotFound(d) => write!(f, "database '{d}' does not exist"),
            Error::DatabaseExists(d) => write!(f, "database '{d}' already exists"),
            Error::TableNotFound(t) => write!(f, "table '{t}' does not exist"),
            Error::TableExists(t) => write!(f, "table '{t}' already exists"),
            Error::ColumnNotFound(c) => write!(f, "column '{c}' not found"),
            Error::ColumnCountMismatch { expected, got } => {
                write!(f, "expected {expected} values, got {got}")
            }
            Error::TypeMismatch { expected, got } => {
                write!(f, "type mismatch: expected {expected}, got {got}")
            }
            Error::DuplicateKey(k) => write!(f, "duplicate primary key: {k}"),
            Error::NotNullViolation(c) => write!(f, "column '{c}' cannot be NULL"),
            Error::MultiplePrimaryKeys => write!(f, "only a single-column PRIMARY KEY is supported"),
            Error::MissingPrimaryKey => write!(f, "table requires a PRIMARY KEY column"),
            Error::ParseError(m) => write!(f, "parse error: {m}"),
            Error::TxnNotActive => write!(f, "no active transaction"),
            Error::TxnConflict(m) => write!(f, "transaction conflict: {m}"),
            Error::Io(m) => write!(f, "io error: {m}"),
            Error::Corrupted(m) => write!(f, "corrupted storage: {m}"),
            Error::NotSupported(m) => write!(f, "not supported: {m}"),
            Error::IndexNotFound(i) => write!(f, "index '{i}' does not exist"),
            Error::IndexExists(i) => write!(f, "index '{i}' already exists"),
            Error::ForeignKeyViolation(m) => write!(f, "foreign key constraint fails: {m}"),
            Error::InvalidSchema(m) => write!(f, "invalid schema: {m}"),
            Error::ReadOnlyReplica => write!(
                f,
                "The server is running with the --read-only option so it cannot execute this statement"
            ),
            Error::InvalidQuery(m) => write!(f, "invalid query: {m}"),
            Error::ExecutionError(m) => write!(f, "execution error: {m}"),
            Error::InvalidOperation(m) => write!(f, "invalid operation: {m}"),
            Error::AccessDenied { user, command, object } => {
                write!(f, "{command} command denied to user '{user}'@'localhost' for table '{object}'")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
