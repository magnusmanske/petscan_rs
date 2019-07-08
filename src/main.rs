extern crate hyper;

use hyper::rt::Future;
use hyper::service::service_fn_ok;
use hyper::{Body, Request, Response, Server};

//const PHRASE: &str = "Hello, World!";

fn hello_world(req: Request<Body>) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let body = format!("{:#?}\n\n{:?}", &parts, &body);
    Response::new(Body::from(body))
}

fn main() {
    //let addr = ([127, 0, 0, 1], 3000).into();
    let addr = "127.0.0.1:3000".parse().unwrap();
    let new_svc = || service_fn_ok(hello_world);

    let server = Server::bind(&addr)
        .serve(new_svc)
        .map_err(|e| eprintln!("server error: {}", e));

    // Run this server for... forever!
    hyper::rt::run(server);
}
