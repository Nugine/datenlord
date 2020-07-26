use anyhow::Result;
use futures::future;
use hyper::server::Server;
use hyper::service::make_service_fn;
use std::net::TcpListener;

use s3_service::{storage::FileSystem, S3Service};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let addr = "127.0.0.1:7026";

    let fs = FileSystem::new("./")?;
    log::debug!("fs: {:?}", &fs);
    let service = S3Service::new(fs);

    let server = {
        let listener = TcpListener::bind(&addr)?;
        let make_service: _ =
            make_service_fn(move |_| future::ready(Ok::<_, anyhow::Error>(service.clone())));
        Server::from_tcp(listener)?.serve(make_service)
    };

    log::info!("server is listening on {}", addr);
    server.await?;

    Ok(())
}
