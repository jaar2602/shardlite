//! The built React SPA, embedded into the binary so the console ships as one self-contained
//! executable (scoping decision 4). `console/web/dist` is compiled in at build time; the
//! frontend build must have run first. Any path the SPA does not have a file for falls back to
//! `index.html`, so client-side routes (e.g. `/cluster`) load the app rather than 404.

use include_dir::{include_dir, Dir};
use tiny_http::{Header, Request, Response, StatusCode};

static DIST: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../web/dist");

fn mime_for(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json" | "map") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        _ => "application/octet-stream",
    }
}

pub fn serve(request: Request, url_path: &str) -> std::io::Result<()> {
    let path = url_path.trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    let (bytes, ctype) = match DIST.get_file(path) {
        Some(file) => (file.contents(), mime_for(path)),
        // SPA fallback: unknown paths get index.html so client-side routing works.
        None => match DIST.get_file("index.html") {
            Some(index) => (index.contents(), "text/html; charset=utf-8"),
            None => return request.respond(Response::empty(StatusCode(404))),
        },
    };

    let header = Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).expect("ctype header");
    request.respond(Response::from_data(bytes.to_vec()).with_header(header))
}
