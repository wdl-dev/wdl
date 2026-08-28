use redis::aio::ConnectionManager;
use redis::io::tcp::TcpSettings;
use redis::{Client, IntoConnectionInfo};

pub fn redis_client_from_url(url: &str) -> Result<Client, redis::RedisError> {
    redis_client_from_url_with_db(url, None)
}

pub fn redis_client_from_url_with_db(
    url: &str,
    db: Option<i64>,
) -> Result<Client, redis::RedisError> {
    let mut connection_info = url.into_connection_info()?;
    if let Some(db) = db {
        let redis_settings = connection_info.redis_settings().clone().set_db(db);
        connection_info = connection_info.set_redis_settings(redis_settings);
    }
    let connection_info =
        connection_info.set_tcp_settings(TcpSettings::default().set_nodelay(true));
    Client::open(connection_info)
}

#[derive(Clone)]
pub struct RedisConnection {
    conn: ConnectionManager,
}

impl RedisConnection {
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    pub async fn with_conn<T, F, Fut>(&self, f: F) -> Result<T, redis::RedisError>
    where
        F: FnOnce(ConnectionManager) -> Fut,
        Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
    {
        f(self.conn.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_client_enables_tcp_nodelay_and_preserves_url_settings() {
        let client = redis_client_from_url("redis://user:secret@127.0.0.1:6380/2")
            .expect("Redis client configuration");
        let info = client.get_connection_info();

        assert!(info.tcp_settings().nodelay());
        assert_eq!(info.redis_settings().db(), 2);
        assert_eq!(info.redis_settings().username(), Some("user"));
        assert_eq!(info.redis_settings().password(), Some("secret"));
    }

    #[test]
    fn redis_client_db_override_preserves_unix_socket_and_query_settings() {
        let client = redis_client_from_url_with_db(
            "redis+unix:///run/redis.sock?db=7&protocol=resp3",
            Some(2),
        )
        .expect("Unix Redis client configuration");
        let info = client.get_connection_info();

        assert_eq!(
            info.addr(),
            &redis::ConnectionAddr::Unix("/run/redis.sock".into())
        );
        assert_eq!(info.redis_settings().db(), 2);
        assert_eq!(
            info.redis_settings().protocol(),
            redis::ProtocolVersion::RESP3
        );
    }
}
