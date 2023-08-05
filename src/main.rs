use std::net::SocketAddr;

use axum::{routing::post, Router};

mod handler;

#[tokio::main]
async fn main() {
    let pool = create_database_pool().await.unwrap();
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let app = Router::new()
        .route("/mssql.v1alpha1.Database/Execute", post(handler::execute))
        .with_state(pool);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

pub type DatabasePool = bb8::Pool<bb8_tiberius::ConnectionManager>;

async fn create_database_pool() -> Result<DatabasePool, bb8_tiberius::Error> {
    let connection_str = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        panic!("DATABASE_URL must be set in order to connect to SQL Server");
    });

    let manager = bb8_tiberius::ConnectionManager::build(connection_str.as_str())?;

    bb8::Pool::builder().build(manager).await
}
