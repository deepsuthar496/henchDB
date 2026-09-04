//! Value types and their binary encodings.
//!
//! Two encodings live here:
//! - `Datum::encode/decode`: row payload encoding (type-tagged, compact).
//! - `KeyBytes`: order-preserving key encoding so memcmp order on the B+ tree
//!   equals logical value order (big-endian, sign-flipped integers).

use crate::error::{Error, Result};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Null => write!(f, "NULL"),
            Datum::Int(v) => write!(f, "{v}"),
            Datum::Float(v) => write!(f, "{v}"),
            Datum::Text(v) => write!(f, "{v}"),
            Datum::Bool(v) => write!(f, "{v}"),
        }
    }
}

impl Datum {
    pub fn type_name(&self) -> &'static str {
        match self {
            Datum::Null => "NULL",
            Datum::Int(_) => "INT",
            Datum::Float(_) => "FLOAT",
            Datum::Text(_) => "TEXT",
            Datum::Bool(_) => "BOOL",
        }
    }

    /// Total order across all datum kinds. Type rank: Null < Bool < numeric
    /// (Int/Float compared numerically) < Text. Floats use a NaN-safe total
    /// order. Used by ORDER BY and predicate evaluation.
    fn type_rank(&self) -> u8 {
        match self {
            Datum::Null => 0,
            Datum::Bool(_) => 1,
            Datum::Int(_) | Datum::Float(_) => 2,
            Datum::Text(_) => 3,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Datum::Int(v) => Some(*v as f64),
            Datum::Float(v) => Some(*v),
            _ => None,
        }
    }
}

impl Eq for Datum {}

impl Ord for Datum {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let (ra, rb) = (self.type_rank(), other.type_rank());
        if ra != rb {
            return ra.cmp(&rb);
        }
        match (self, other) {
            (Datum::Bool(a), Datum::Bool(b)) => a.cmp(b),
            (Datum::Int(a), Datum::Int(b)) => a.cmp(b),
            (Datum::Float(a), Datum::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Datum::Text(a), Datum::Text(b)) => a.cmp(b),
            _ => match (self.as_f64(), other.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            },
        }
    }
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Datum {
    /// Type-tagged payload encoding (used for row values).
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Datum::Null => out.push(0),
            Datum::Int(v) => {
                out.push(1);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Float(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Text(v) => {
                out.push(3);
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v.as_bytes());
            }
            Datum::Bool(v) => {
                out.push(4);
                out.push(*v as u8);
            }
        }
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v);
        v
    }

    pub fn decode(buf: &[u8], off: &mut usize) -> Result<Datum> {
        let tag = *buf.get(*off).ok_or_else(|| Error::Corrupted("datum: EOF".into()))?;
        *off += 1;
        Ok(match tag {
            0 => Datum::Null,
            1 => {
                let b = take(buf, off, 8)?;
                Datum::Int(i64::from_le_bytes(b.try_into().unwrap()))
            }
            2 => {
                let b = take(buf, off, 8)?;
                Datum::Float(f64::from_le_bytes(b.try_into().unwrap()))
            }
            3 => {
                let b = take(buf, off, 4)?;
                let len = u32::from_le_bytes(b.try_into().unwrap()) as usize;
                let b = take(buf, off, len)?;
                Datum::Text(String::from_utf8(b).map_err(|_| Error::Corrupted("utf8".into()))?)
            }
            4 => {
                let b = take(buf, off, 1)?;
                Datum::Bool(b[0] != 0)
            }
            t => return Err(Error::Corrupted(format!("unknown datum tag {t}"))),
        })
    }
}

fn take(buf: &[u8], off: &mut usize, n: usize) -> Result<Vec<u8>> {
    if *off + n > buf.len() {
        return Err(Error::Corrupted("datum: truncated".into()));
    }
    let s = buf[*off..*off + n].to_vec();
    *off += n;
    Ok(s)
}

/// Column type as declared in the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int,
    BigInt,
    Float,
    Double,
    Text,
    VarChar,
    Bool,
}

impl ColumnType {
    pub fn parse(s: &str) -> Result<ColumnType> {
        Ok(match s.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" => ColumnType::Int,
            "BIGINT" | "LONG" => ColumnType::BigInt,
            "FLOAT" | "REAL" => ColumnType::Float,
            "DOUBLE" => ColumnType::Double,
            "TEXT" | "STRING" => ColumnType::Text,
            "VARCHAR" => ColumnType::VarChar,
            "BOOL" | "BOOLEAN" => ColumnType::Bool,
            other => return Err(Error::ParseError(format!("unknown type '{other}'"))),
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            ColumnType::Int => "INT",
            ColumnType::BigInt => "BIGINT",
            ColumnType::Float => "FLOAT",
            ColumnType::Double => "DOUBLE",
            ColumnType::Text => "TEXT",
            ColumnType::VarChar => "VARCHAR",
            ColumnType::Bool => "BOOL",
        }
    }

    pub fn accepts(&self, d: &Datum) -> bool {
        match self {
            ColumnType::Int | ColumnType::BigInt => matches!(d, Datum::Int(_) | Datum::Null),
            ColumnType::Float | ColumnType::Double => {
                matches!(d, Datum::Int(_) | Datum::Float(_) | Datum::Null)
            }
            ColumnType::Text | ColumnType::VarChar => matches!(d, Datum::Text(_) | Datum::Null),
            ColumnType::Bool => matches!(d, Datum::Bool(_) | Datum::Null),
        }
    }
}

/// Order-preserving key encoding: tag byte (type ordering) + big-endian
/// payload. Integer encoding is sign-flipped so memcmp order == numeric
/// order for negative and positive values alike.
pub fn encode_key(d: &Datum) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match d {
        Datum::Int(v) => {
            out.push(1);
            out.extend_from_slice(&(*v ^ i64::MIN).to_be_bytes());
        }
        Datum::Float(v) => {
            // IEEE754 total order: flip sign bit for positives, all bits for
            // negatives; not needed for the common path but kept correct.
            let bits = v.to_bits();
            let ordered = if *v >= 0.0 { bits ^ 0x8000_0000_0000_0000 } else { !bits };
            out.push(2);
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        Datum::Text(v) => {
            out.push(3);
            out.extend_from_slice(v.as_bytes());
        }
        Datum::Bool(v) => {
            out.push(4);
            out.push(*v as u8);
        }
        Datum::Null => return Err(Error::NotNullViolation("primary key".into())),
    }
    Ok(out)
}

/// Decode an encoded key back into a Datum (keys are always non-NULL).
pub fn decode_key(key: &[u8]) -> Result<Datum> {
    if key.is_empty() {
        return Err(Error::Corrupted("empty key".into()));
    }
    match key[0] {
        1 => {
            let b: [u8; 8] = key[1..9].try_into().unwrap();
            Ok(Datum::Int(i64::from_be_bytes(b) ^ i64::MIN))
        }
        2 => {
            let b: [u8; 8] = key[1..9].try_into().unwrap();
            let bits = u64::from_be_bytes(b);
            // Sign of the original float is recoverable from the flipped bit.
            let v = if bits & 0x8000_0000_0000_0000 != 0 {
                f64::from_bits(bits ^ 0x8000_0000_0000_0000)
            } else {
                f64::from_bits(!bits)
            };
            Ok(Datum::Float(v))
        }
        3 => Ok(Datum::Text(
            String::from_utf8(key[1..].to_vec()).map_err(|_| Error::Corrupted("utf8 key".into()))?,
        )),
        4 => Ok(Datum::Bool(key[1] != 0)),
        t => Err(Error::Corrupted(format!("unknown key tag {t}"))),
    }
}

/// Encode a secondary key prefix (used for point lookups and range scans on secondary indexes).
pub fn encode_sec_key_prefix(d: &Datum) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match d {
        Datum::Null => {
            out.push(0);
        }
        Datum::Int(v) => {
            out.push(1);
            out.extend_from_slice(&(*v ^ i64::MIN).to_be_bytes());
        }
        Datum::Float(v) => {
            let bits = v.to_bits();
            let ordered = if *v >= 0.0 { bits ^ 0x8000_0000_0000_0000 } else { !bits };
            out.push(2);
            out.extend_from_slice(&ordered.to_be_bytes());
        }
        Datum::Text(v) => {
            out.push(3);
            for &b in v.as_bytes() {
                if b == 0 {
                    out.push(0);
                    out.push(0xFF);
                } else {
                    out.push(b);
                }
            }
            out.push(0);
            out.push(0);
        }
        Datum::Bool(v) => {
            out.push(4);
            out.push(*v as u8);
        }
    }
    Ok(out)
}

/// Encode a secondary index entry: order-preserving secondary key followed by
/// the primary key. This ensures entries with the same secondary value are
/// ordered by primary key and are uniquely addressable in the B+ tree.
pub fn encode_sec_index_key(sec: &Datum, pk: &Datum) -> Result<Vec<u8>> {
    let mut key = encode_sec_key_prefix(sec)?;
    let pk_bytes = encode_key(pk)?;
    key.extend_from_slice(&pk_bytes);
    Ok(key)
}

/// Decode a secondary index entry back into `(secondary_datum, primary_datum)`.
pub fn decode_sec_index_key(key: &[u8]) -> Result<(Datum, Datum)> {
    if key.is_empty() {
        return Err(Error::Corrupted("empty secondary index key".into()));
    }
    let tag = key[0];
    let (sec, off) = match tag {
        0 => (Datum::Null, 1),
        1 => {
            if key.len() < 9 {
                return Err(Error::Corrupted("truncated int secondary key".into()));
            }
            let b: [u8; 8] = key[1..9].try_into().unwrap();
            (Datum::Int(i64::from_be_bytes(b) ^ i64::MIN), 9)
        }
        2 => {
            if key.len() < 9 {
                return Err(Error::Corrupted("truncated float secondary key".into()));
            }
            let b: [u8; 8] = key[1..9].try_into().unwrap();
            let bits = u64::from_be_bytes(b);
            let v = if bits & 0x8000_0000_0000_0000 != 0 {
                f64::from_bits(bits ^ 0x8000_0000_0000_0000)
            } else {
                f64::from_bits(!bits)
            };
            (Datum::Float(v), 9)
        }
        3 => {
            let mut off = 1;
            let mut text_bytes = Vec::new();
            loop {
                if off >= key.len() {
                    return Err(Error::Corrupted("unterminated text in secondary key".into()));
                }
                if key[off] == 0 {
                    if off + 1 >= key.len() {
                        return Err(Error::Corrupted("truncated text terminator in secondary key".into()));
                    }
                    if key[off + 1] == 0 {
                        off += 2;
                        break;
                    } else if key[off + 1] == 0xFF {
                        text_bytes.push(0);
                        off += 2;
                    } else {
                        return Err(Error::Corrupted("invalid text escape in secondary key".into()));
                    }
                } else {
                    text_bytes.push(key[off]);
                    off += 1;
                }
            }
            let s = String::from_utf8(text_bytes).map_err(|_| Error::Corrupted("invalid utf8 in secondary key".into()))?;
            (Datum::Text(s), off)
        }
        4 => {
            if key.len() < 2 {
                return Err(Error::Corrupted("truncated bool secondary key".into()));
            }
            (Datum::Bool(key[1] != 0), 2)
        }
        t => return Err(Error::Corrupted(format!("unknown secondary key tag {t}"))),
    };
    let pk = decode_key(&key[off..])?;
    Ok((sec, pk))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datum_roundtrip() {
        for d in [
            Datum::Null,
            Datum::Int(-42),
            Datum::Float(3.25),
            Datum::Text("hello".into()),
            Datum::Bool(true),
        ] {
            let enc = d.encode_to_vec();
            let mut off = 0;
            assert_eq!(Datum::decode(&enc, &mut off).unwrap(), d);
            assert_eq!(off, enc.len());
        }
    }

    #[test]
    fn int_key_ordering() {
        let mut keys: Vec<Vec<u8>> = [-5i64, 0, 3, -1, 100, i64::MIN, i64::MAX]
            .iter()
            .map(|v| encode_key(&Datum::Int(*v)).unwrap())
            .collect();
        keys.sort();
        let decoded: Vec<i64> = keys
            .iter()
            .map(|k| match decode_key(k).unwrap() {
                Datum::Int(v) => v,
                _ => panic!(),
            })
            .collect();
        let mut sorted = [-5i64, 0, 3, -1, 100, i64::MIN, i64::MAX];
        sorted.sort();
        assert_eq!(decoded, sorted);
    }

    #[test]
    fn sec_key_roundtrip_and_ordering() {
        let items = [
            (Datum::Null, Datum::Int(10)),
            (Datum::Int(-100), Datum::Int(1)),
            (Datum::Int(0), Datum::Int(2)),
            (Datum::Int(0), Datum::Int(5)),
            (Datum::Int(50), Datum::Int(3)),
            (Datum::Float(-1.5), Datum::Int(4)),
            (Datum::Float(0.0), Datum::Int(5)),
            (Datum::Float(2.5), Datum::Int(6)),
            (Datum::Text("cat".into()), Datum::Int(1)),
            (Datum::Text("cat".into()), Datum::Int(2)),
            (Datum::Text("caterpillar".into()), Datum::Int(3)),
            (Datum::Text("dog".into()), Datum::Int(4)),
            (Datum::Bool(false), Datum::Int(1)),
            (Datum::Bool(true), Datum::Int(2)),
        ];

        for (sec, pk) in &items {
            let enc = encode_sec_index_key(sec, pk).unwrap();
            let (dec_sec, dec_pk) = decode_sec_index_key(&enc).unwrap();
            assert_eq!(&dec_sec, sec);
            assert_eq!(&dec_pk, pk);
        }

        // Test string ordering specifically: "cat" pk 2 must be < "caterpillar" pk 1
        let cat_2 = encode_sec_index_key(&Datum::Text("cat".into()), &Datum::Int(2)).unwrap();
        let cat_p = encode_sec_index_key(&Datum::Text("caterpillar".into()), &Datum::Int(1)).unwrap();
        assert!(cat_2 < cat_p);

        // Test composite ordering: ("cat", 1) < ("cat", 2)
        let cat_1 = encode_sec_index_key(&Datum::Text("cat".into()), &Datum::Int(1)).unwrap();
        assert!(cat_1 < cat_2);
    }
}

