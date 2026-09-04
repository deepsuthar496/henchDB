use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
    Ident(String),
    Number(String),
    Str(String),
    Sym(char),
    Eof,
}

pub(crate) struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub(crate) fn new(src: &str) -> Lexer {
        Lexer {
            chars: src.chars().collect(),
            pos: 0,
        }
    }

    pub(crate) fn tokenize(mut self) -> Result<Vec<Token>> {
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            let Some(&c) = self.chars.get(self.pos) else {
                out.push(Token::Eof);
                break;
            };
            match c {
                '\'' | '"' => out.push(self.read_string(c)?),
                c if c.is_ascii_digit() => out.push(self.read_number()),
                c if c.is_ascii_alphabetic() || c == '_' => {
                    out.push(Token::Ident(self.read_ident()));
                }
                '-' if self.peek_digit() => out.push(self.read_number()),
                ',' | '(' | ')' | '*' | ';' | '=' | '.' => {
                    out.push(Token::Sym(c));
                    self.pos += 1;
                }
                '<' => {
                    self.pos += 1;
                    if self.chars.get(self.pos) == Some(&'=') {
                        self.pos += 1;
                        out.push(Token::Sym('≤'));
                    } else if self.chars.get(self.pos) == Some(&'>') {
                        self.pos += 1;
                        out.push(Token::Sym('≠'));
                    } else {
                        out.push(Token::Sym('<'));
                    }
                }
                '>' => {
                    self.pos += 1;
                    if self.chars.get(self.pos) == Some(&'=') {
                        self.pos += 1;
                        out.push(Token::Sym('≥'));
                    } else {
                        out.push(Token::Sym('>'));
                    }
                }
                '!' => {
                    self.pos += 1;
                    if self.chars.get(self.pos) == Some(&'=') {
                        self.pos += 1;
                        out.push(Token::Sym('≠'));
                    } else {
                        return Err(Error::ParseError("expected '!='".into()));
                    }
                }
                other => {
                    return Err(Error::ParseError(format!("unexpected character '{other}'")));
                }
            }
        }
        Ok(out)
    }

    fn skip_ws(&mut self) {
        while let Some(&c) = self.chars.get(self.pos) {
            if c.is_whitespace() {
                self.pos += 1;
            } else if c == '-' && self.chars.get(self.pos + 1) == Some(&'-') {
                while self.pos < self.chars.len() && self.chars[self.pos] != '\n' {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn peek_digit(&self) -> bool {
        self.chars
            .get(self.pos + 1)
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
    }

    fn read_string(&mut self, quote: char) -> Result<Token> {
        self.pos += 1;
        let mut s = String::new();
        loop {
            match self.chars.get(self.pos) {
                None => return Err(Error::ParseError("unterminated string".into())),
                Some(&c) if c == quote => {
                    self.pos += 1;
                    if self.chars.get(self.pos) == Some(&quote) {
                        s.push(quote);
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                Some(&c) => {
                    s.push(c);
                    self.pos += 1;
                }
            }
        }
        Ok(Token::Str(s))
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        if self.chars[self.pos] == '-' {
            self.pos += 1;
        }
        let mut is_float = false;
        while let Some(&c) = self.chars.get(self.pos) {
            if c.is_ascii_digit() {
                self.pos += 1;
            } else if c == '.' && !is_float {
                is_float = true;
                self.pos += 1;
            } else {
                break;
            }
        }
        Token::Number(self.chars[start..self.pos].iter().collect())
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;
        while let Some(&c) = self.chars.get(self.pos) {
            if c.is_ascii_alphanumeric() || c == '_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.chars[start..self.pos].iter().collect()
    }
}
