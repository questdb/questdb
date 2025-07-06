mod extended;
mod simple;

use std::marker::PhantomData;
use std::sync::Arc;

use tokio::task::JoinHandle;

type Result<T> = std::result::Result<T, tokio_postgres::Error>;

/// Marker type for the Postgres simple query protocol.
pub struct Simple;
/// Marker type for the Postgres extended query protocol.
pub struct Extended;

/// Generic Postgres engine based on the client from [`tokio_postgres`]. The protocol `P` can be
/// either [`Simple`] or [`Extended`].
pub struct Postgres<P> {
    client: Arc<tokio_postgres::Client>,
    join_handle: JoinHandle<()>,
    _protocol: PhantomData<P>,
}

/// Postgres engine using the simple query protocol.
pub type PostgresSimple = Postgres<Simple>;
/// Postgres engine using the extended query protocol.
pub type PostgresExtended = Postgres<Extended>;

/// Connection configuration. This is a re-export of [`tokio_postgres::Config`].
pub type PostgresConfig = tokio_postgres::Config;

impl<P> Postgres<P> {
    /// Connects to the Postgres server with the given `config`.
    pub async fn connect(config: PostgresConfig) -> Result<Self> {
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;

        let join_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Postgres connection error: {:?}", e);
            }
        });

        Ok(Self {
            client: Arc::new(client),
            join_handle,
            _protocol: PhantomData,
        })
    }

    /// Returns a reference of the inner Postgres client.
    pub fn pg_client(&self) -> &tokio_postgres::Client {
        &self.client
    }
}

impl<P> Drop for Postgres<P> {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}
