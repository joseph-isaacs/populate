//! Java library behaviour the datagen relies on: `String.split`,
//! `BufferedReader.readLine`, `String.trim`, `Double.parseDouble`,
//! `HashMap` iteration order, `Arrays.binarySearch`, `java.time` dates.

use populate_core::StrBuf;
use std::cmp::Ordering;

/// `BufferedReader.readLine` over a whole file: lines end at `\n`, `\r` or
/// `\r\n`.
pub fn lines(s: &str) -> Vec<&str> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\n' => {
                out.push(&s[start..i]);
                i += 1;
                start = i;
            }
            b'\r' => {
                out.push(&s[start..i]);
                i += 1;
                if i < b.len() && b[i] == b'\n' {
                    i += 1;
                }
                start = i;
            }
            _ => i += 1,
        }
    }
    if start < b.len() {
        out.push(&s[start..]);
    }
    out
}

/// `String.split(sep)` for a literal separator: trailing empty strings are
/// dropped.
pub fn split<'a>(s: &'a str, sep: &str) -> Vec<&'a str> {
    if s.is_empty() {
        // "".split(x) is [""]
        return vec![""];
    }
    let mut v: Vec<&str> = s.split(sep).collect();
    while v.len() > 1 && v.last().is_some_and(|x| x.is_empty()) {
        v.pop();
    }
    if v.len() == 1 && v[0].is_empty() {
        // a string of separators only splits to [] in Java
        v.clear();
    }
    v
}

/// `String.trim()`: strips chars `<= ' '`.
pub fn trim(s: &str) -> &str {
    s.trim_matches(|c: char| c <= ' ')
}

/// `Double.parseDouble`: surrounding whitespace allowed.
pub fn parse_f64(s: &str) -> f64 {
    let t = trim(s);
    t.parse::<f64>()
        .unwrap_or_else(|_| panic!("bad double `{t}`"))
}

/// `Float.parseFloat`.
pub fn parse_f32(s: &str) -> f32 {
    let t = trim(s);
    t.parse::<f32>()
        .unwrap_or_else(|_| panic!("bad float `{t}`"))
}

/// `Integer.parseInt`.
pub fn parse_i32(s: &str) -> i32 {
    s.parse::<i32>().unwrap_or_else(|_| panic!("bad int `{s}`"))
}

/// `Arrays.binarySearch(Float[], key)`: the index of `key`, or
/// `-(insertion point) - 1`.
pub fn binary_search_f32(a: &[f32], key: f32) -> i32 {
    let (mut low, mut high) = (0i32, a.len() as i32 - 1);
    while low <= high {
        let mid = ((low + high) as u32 >> 1) as i32;
        let v = a[mid as usize];
        // Float.compareTo: total order (no NaNs here)
        match v.total_cmp(&key) {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid - 1,
            Ordering::Equal => return mid,
        }
    }
    -(low + 1)
}

/// The order in which a small `java.util.HashMap<Long, _>` (default
/// capacity 16, fewer than 13 entries) iterates its keys, given the keys in
/// insertion order (no duplicates): by bucket, then insertion order.
pub fn hashmap_long_order(keys: &mut [(i64, i64)]) {
    debug_assert!(keys.len() <= 12);
    keys.sort_by_key(|&(k, _)| {
        let h = (k ^ ((k as u64) >> 32) as i64) as i32;
        let h = h ^ ((h as u32) >> 16) as i32;
        (h & 15) as u32
    });
}

pub const MS_PER_DAY: i64 = 86_400_000;

/// Days since 1970-01-01 of a civil date (proleptic Gregorian).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `(year, month, day)` of a day number.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// UTC `(year, month, day)` of epoch milliseconds.
pub fn ymd_of_ms(ms: i64) -> (i64, u32, u32) {
    civil_from_days(ms.div_euclid(MS_PER_DAY))
}

/// Epoch milliseconds of midnight UTC on a date.
pub fn ms_of_ymd(y: i64, m: u32, d: u32) -> i64 {
    days_from_civil(y, m, d) * MS_PER_DAY
}

/// The characters of a UTF-16 string as Java encodes them: unpaired
/// surrogates become `?`.
pub fn java_chars(s: &[u16]) -> impl Iterator<Item = char> + '_ {
    char::decode_utf16(s.iter().copied()).map(|c| c.unwrap_or('?'))
}

/// Append a UTF-16 string, as Java encodes it, to a string value: runs
/// of ASCII in one go, anything else a character at a time.
#[inline]
pub fn push_utf16(out: &mut StrBuf<'_>, mut s: &[u16]) {
    while !s.is_empty() {
        let ascii = s.iter().position(|&u| u >= 0x80).unwrap_or(s.len());
        out.extend_ascii(s[..ascii].iter().map(|&u| u as u8));
        s = &s[ascii..];
        // one character: a surrogate pair takes two units
        let n = if s.len() > 1
            && (0xD800..0xDC00).contains(&s[0])
            && (0xDC00..0xE000).contains(&s[1])
        {
            2
        } else {
            s.len().min(1)
        };
        for c in java_chars(&s[..n]) {
            out.push_char(c);
        }
        s = &s[n..];
    }
}

/// UTF-16 length of a string (Java's `String.length()`).
pub fn utf16_len(s: &str) -> usize {
    s.chars().map(char::len_utf16).sum()
}

/// `str.substring(0, n)` if longer than `n` UTF-16 units
/// (`StringUtils.clampString`).
pub fn clamp(s: &str, n: usize) -> String {
    if utf16_len(s) <= n {
        return s.to_string();
    }
    let u: Vec<u16> = s.encode_utf16().take(n).collect();
    java_chars(&u).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_like_java() {
        assert_eq!(split("a  b  ", "  "), vec!["a", "b"]);
        assert_eq!(split("  a", "  "), vec!["", "a"]);
        assert_eq!(split("a", " "), vec!["a"]);
    }

    #[test]
    fn lines_like_java() {
        assert_eq!(lines("a\r\nb\rc\nd"), vec!["a", "b", "c", "d"]);
        assert_eq!(lines("a\n\nb\n"), vec!["a", "", "b"]);
    }

    #[test]
    fn dates() {
        assert_eq!(ms_of_ymd(2010, 1, 1), 1_262_304_000_000);
        assert_eq!(ymd_of_ms(1_262_304_000_000 - 1), (2009, 12, 31));
        for d in -800_000..800_000 {
            let (y, m, dd) = civil_from_days(d);
            assert_eq!(days_from_civil(y, m, dd), d);
        }
    }
}
