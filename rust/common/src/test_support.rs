fn read_resp_usize(packed: &[u8], offset: &mut usize) -> usize {
    let start = *offset;
    while &packed[*offset..*offset + 2] != b"\r\n" {
        *offset += 1;
    }
    let value = std::str::from_utf8(&packed[start..*offset])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    *offset += 2;
    value
}

pub fn parse_packed_commands(packed: &[u8]) -> Vec<Vec<String>> {
    let mut offset = 0_usize;
    let mut commands = Vec::new();
    while offset < packed.len() {
        assert_eq!(packed[offset], b'*');
        offset += 1;
        let count = read_resp_usize(packed, &mut offset);
        let mut command = Vec::new();
        for _ in 0..count {
            assert_eq!(packed[offset], b'$');
            offset += 1;
            let len = read_resp_usize(packed, &mut offset);
            let end = offset + len;
            command.push(String::from_utf8(packed[offset..end].to_vec()).unwrap());
            offset = end;
            assert_eq!(&packed[offset..offset + 2], b"\r\n");
            offset += 2;
        }
        commands.push(command);
    }
    commands
}

#[cfg(test)]
mod tests {
    use super::parse_packed_commands;

    #[test]
    fn parses_concatenated_resp_commands() {
        assert_eq!(
            parse_packed_commands(
                b"*2\r\n$3\r\nGET\r\n$3\r\none\r\n*3\r\n$3\r\nSET\r\n$3\r\ntwo\r\n$1\r\n2\r\n"
            ),
            vec![vec!["GET", "one"], vec!["SET", "two", "2"]]
        );
    }
}
