pub(crate) const MAX_SUPERVISOR_RESPONSE_BYTES: usize = 256 * 1024;

pub(crate) async fn read_bounded_response_text(
    mut response: reqwest::Response,
) -> Result<String, String> {
    if response
        .content_length()
        .is_some_and(|len| len > MAX_SUPERVISOR_RESPONSE_BYTES as u64)
    {
        return Err(format!(
            "response body exceeds {MAX_SUPERVISOR_RESPONSE_BYTES} bytes"
        ));
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("failed to read response body: {error}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_SUPERVISOR_RESPONSE_BYTES {
            return Err(format!(
                "response body exceeds {MAX_SUPERVISOR_RESPONSE_BYTES} bytes"
            ));
        }
        bytes.extend_from_slice(&chunk);
    }

    String::from_utf8(bytes).map_err(|error| format!("response body is not valid UTF-8: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    async fn raw_http_response(
        response: Vec<u8>,
    ) -> (reqwest::Response, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            let _ = stream.write_all(&response);
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("http://{addr}/internal"))
            .send()
            .await
            .unwrap();
        (response, server)
    }

    #[tokio::test]
    async fn rejects_oversized_declared_response() {
        let (response, server) = raw_http_response(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_SUPERVISOR_RESPONSE_BYTES + 1
            )
            .into_bytes(),
        )
        .await;

        let error = read_bounded_response_text(response).await.unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error,
            format!("response body exceeds {MAX_SUPERVISOR_RESPONSE_BYTES} bytes")
        );
    }

    #[tokio::test]
    async fn rejects_oversized_chunked_response() {
        let body = vec![b'x'; MAX_SUPERVISOR_RESPONSE_BYTES + 1];
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(&body);
        response.extend_from_slice(b"\r\n0\r\n\r\n");
        let (response, server) = raw_http_response(response).await;

        let error = read_bounded_response_text(response).await.unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error,
            format!("response body exceeds {MAX_SUPERVISOR_RESPONSE_BYTES} bytes")
        );
    }
}
