use std::collections::HashMap;
use std::future::IntoFuture;

use futures::Future;

use crate::{AsyncDB, Connection as ConnectionName, DBOutput};

/// Trait for making connections to an [`AsyncDB`].
///
/// This is introduced to allow querying the database with different connections
/// (then generally different sessions) in a single test file with `connection` records.
pub trait MakeConnection {
    /// The database type.
    type Conn: AsyncDB;
    /// The future returned by [`MakeConnection::make`].
    type MakeFuture: Future<Output=Result<Self::Conn, <Self::Conn as AsyncDB>::Error>>;

    /// Creates a new connection to the database.
    fn make(&mut self) -> Self::MakeFuture;
}

/// Make connections directly from a closure returning a future.
impl<D: AsyncDB, F, Fut> MakeConnection for F
where
    F: FnMut() -> Fut,
    Fut: IntoFuture<Output=Result<D, D::Error>>,
{
    type Conn = D;
    type MakeFuture = Fut::IntoFuture;

    fn make(&mut self) -> Self::MakeFuture {
        self().into_future()
    }
}

/// Connections established in a [`Runner`](crate::Runner).
pub(crate) struct Connections<D, M> {
    make_conn: M,
    conns: HashMap<ConnectionName, D>,
}

impl<D: AsyncDB, M: MakeConnection<Conn=D>> Connections<D, M> {
    pub fn new(make_conn: M) -> Self {
        Connections {
            make_conn,
            conns: HashMap::new(),
        }
    }

    /// Get a connection by name. Make a new connection if it doesn't exist.
    pub async fn get(&mut self, name: ConnectionName) -> Result<&mut D, D::Error> {
        use std::collections::hash_map::Entry;

        let conn = match self.conns.entry(name) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let conn = self.make_conn.make().await?;
                v.insert(conn)
            }
        };

        Ok(conn)
    }

    /// Run a SQL statement on the default connection.
    ///
    /// This is a shortcut for calling `get(Default)` then `run`.
    pub async fn run_default(&mut self, sql: &str) -> Result<DBOutput<D::ColumnType>, D::Error> {
        self.get(ConnectionName::Default).await?.run(sql, Default::default()).await
    }
}
