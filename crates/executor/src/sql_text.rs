//! Raw-SQL text utilities for pre-parse interception.

/// Strip `/* ... */` block comments and `-- ...` line comments outside of
/// single-quoted string literals, replacing each block with a single space so
/// surrounding tokens do not fuse.
///
/// Needed because several SQL statements are matched pre-parse on the raw text
/// (CLONE, WAREHOUSE, STREAM/TASK, UNDROP, ACCOUNT DDL) with a whitespace
/// tokenizer that has no comment awareness — and ecosystem clients (dbt)
/// append a trailing `/* {"app": "dbt", ...} */` tag comment to every
/// statement they send.
#[must_use]
pub fn strip_comments(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_string = false;
    while i < chars.len() {
        let c = chars[i];
        if in_string {
            out.push(c);
            if c == '\'' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match c {
            '\'' => {
                in_string = true;
                out.push(c);
                i += 1;
            }
            '-' if chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if chars.get(i + 1) == Some(&'*') => {
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                i += 2; // past the closing `*/` (or past end when unterminated)
                out.push(' ');
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::strip_comments;

    #[test]
    fn strips_block_comments() {
        assert_eq!(strip_comments("SELECT 1 /* {\"app\": \"dbt\"} */"), "SELECT 1  ");
        assert_eq!(
            strip_comments("/* tag */ CREATE TABLE t2 CLONE t1"),
            "  CREATE TABLE t2 CLONE t1"
        );
        assert_eq!(
            strip_comments("A /* c1 */ B /* c2 */ C"),
            "A   B   C"
        );
    }

    #[test]
    fn strips_multiline_block_comments() {
        assert_eq!(
            strip_comments("CREATE TABLE t2 CLONE t1\n/* {\"app\": \"dbt\",\n\"v\": 1} */"),
            "CREATE TABLE t2 CLONE t1\n "
        );
    }

    #[test]
    fn strips_line_comments() {
        assert_eq!(strip_comments("SELECT 1 -- done"), "SELECT 1 ");
        assert_eq!(
            strip_comments("CREATE TABLE t2 CLONE t1; -- trailing"),
            "CREATE TABLE t2 CLONE t1; "
        );
    }

    #[test]
    fn preserves_string_literals() {
        assert_eq!(
            strip_comments("INSERT INTO t VALUES ('a /* not a comment */', '--nope')"),
            "INSERT INTO t VALUES ('a /* not a comment */', '--nope')"
        );
        assert_eq!(
            strip_comments("SELECT 'it''s /* x */' -- end"),
            "SELECT 'it''s /* x */' "
        );
    }

    #[test]
    fn unterminated_block_comment_dropped() {
        assert_eq!(strip_comments("SELECT 1 /* open"), "SELECT 1  ");
    }
}