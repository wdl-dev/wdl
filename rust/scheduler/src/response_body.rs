pub(crate) async fn read_bounded_response_text(
    mut response: reqwest::Response,
    max_bytes: usize,
    body_name: &str,
) -> Result<String, String> {
    if response
        .content_length()
        .is_some_and(|len| len > max_bytes as u64)
    {
        return Err(format!("{body_name} exceeds {max_bytes} bytes"));
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("failed to read {body_name}: {error}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(format!("{body_name} exceeds {max_bytes} bytes"));
        }
        bytes.extend_from_slice(&chunk);
    }

    String::from_utf8(bytes).map_err(|error| format!("{body_name} is not valid UTF-8: {error}"))
}
