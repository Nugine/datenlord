use anyhow::Result;
use futures::future;
use hyper::server::Server;
use hyper::service::make_service_fn;
use rusoto_core::Region;
use rusoto_s3::S3Client;
use s3_service::S3Service;
use std::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:8080";
    let region = Region::CnNorth1;

    let service = S3Service::new(S3Client::new(region));

    let server = {
        let listener = TcpListener::bind(&addr)?;
        let make_service: _ =
            make_service_fn(move |_| future::ready(Ok::<_, anyhow::Error>(service.clone())));
        Server::from_tcp(listener)?.serve(make_service)
    };

    eprintln!("server is listening on {}", addr);
    server.await?;

    Ok(())
}
