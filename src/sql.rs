//! Implementation of a minimal SQL tokenizer and parser for the subset of operators
//! that we have implemented.
use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Select,
    From,
    Where,
    OrderBy,
    Limit,
    And,
    Or,
    Not,
    Identifier(String),
    Varchar(String),
    Number(i64),
    Comma,
    Semicolon,
    OpenParen,
    CloseParen,
    Equal,
    GreaterThan,
    LessThan,
    EOF,
}

pub struct Tokenizer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    pub fn next(&mut self) -> Token {
        self.skip_whitespace();

        if self.pos >= self.input.len() {
            return Token::EOF;
        }

        let current_char = self.peek();

        match current_char {
            'a'..='z' | 'A'..='Z' => self.ident(),
            '0'..='9' => self.number(),
            '\'' => self.varchar(),
            ',' => {
                self.pos += 1;
                Token::Comma
            }
            ';' => {
                self.pos += 1;
                Token::Semicolon
            }
            '(' => {
                self.pos += 1;
                Token::OpenParen
            }
            ')' => {
                self.pos += 1;
                Token::CloseParen
            }
            '=' => {
                self.pos += 1;
                Token::Equal
            }
            '>' => {
                self.pos += 1;
                Token::GreaterThan
            }
            '<' => {
                self.pos += 1;
                Token::LessThan
            }
            _ => panic!("Unexpected character: {}", current_char),
        }
    }

    fn peek(&self) -> char {
        self.input[self.pos..].chars().next().unwrap()
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.peek().is_whitespace() {
            self.pos += 1;
        }
    }

    fn ident(&mut self) -> Token {
        let start_pos = self.pos;
        while self.pos < self.input.len() && self.peek().is_alphanumeric() {
            self.pos += 1;
        }
        let identifier = &self.input[start_pos..self.pos];
        match identifier.to_lowercase().as_str() {
            "select" => Token::Select,
            "from" => Token::From,
            "where" => Token::Where,
            "orderby" => Token::OrderBy,
            "limit" => Token::Limit,
            "and" => Token::And,
            "or" => Token::Or,
            "not" => Token::Not,
            _ => Token::Identifier(identifier.to_string()),
        }
    }

    fn varchar(&mut self) -> Token {
        // Skip opening quote.
        self.pos += 1;
        let start_pos = self.pos;
        while self.pos < self.input.len() && self.peek().is_ascii_alphanumeric() {
            self.pos += 1;
        }
        let varchar: String = self.input[start_pos..self.pos].to_string();
        // Skip closing quote.
        self.pos += 1;
        Token::Varchar(varchar)
    }

    fn number(&mut self) -> Token {
        let start_pos = self.pos;
        while self.pos < self.input.len() && self.peek().is_ascii_digit() {
            self.pos += 1;
        }
        let number: i64 = self.input[start_pos..self.pos].parse().unwrap();
        Token::Number(number)
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(String),
    Value(i64),
    Varchar(String),
    Comparison(Box<Expr>, String, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Select {
        columns: Vec<String>,
        table: String,
        filter: Option<Expr>,
        order_by: Option<String>,
        limit: Option<i64>,
    },
}

pub struct Parser<'a, T: Iterator<Item = Token>> {
    tokenizer: T,
    current_token: Token,
    phantom: PhantomData<&'a T>,
}

impl<'a, T: Iterator<Item = Token>> Parser<'a, T> {
    pub fn new(tokenizer: T) -> Self {
        let mut parser = Self {
            tokenizer,
            current_token: Token::EOF,
            phantom: PhantomData,
        };
        // Synchronize the first token in the parser.
        parser.next();
        parser
    }

    // Update `current_token` with the next token.
    fn next(&mut self) {
        self.current_token = match self.tokenizer.next() {
            Some(token) => token,
            None => Token::EOF,
        };
    }

    // Parse an identifier.
    fn ident(&mut self) -> String {
        if let Token::Identifier(ref id) = self.current_token {
            let identifier = id.clone();
            self.next();
            identifier
        } else {
            panic!("Expected identifier")
        }
    }

    // Parse a numerical value.
    fn number(&mut self) -> i64 {
        if let Token::Number(num) = self.current_token {
            self.next();
            num
        } else {
            panic!("Expected number")
        }
    }

    // Parse an expression, expression parsing is done without much care for precedence.
    fn expr(&mut self) -> Expr {
        // Parse primary expressions (identifiers or numbers)
        let mut left = match self.current_token {
            Token::Identifier(ref id) => {
                // Here we assume all identifiers are columns, no schema required.
                let identifier = id.clone();
                self.next();
                Expr::Column(identifier)
            }
            Token::Varchar(ref ident) => {
                let ident = ident.clone();
                self.next();
                Expr::Varchar(ident)
            }
            Token::Number(num) => {
                self.next(); // Move past number
                Expr::Value(num)
            }
            Token::OpenParen => {
                self.next(); // Move past open parenthesis
                let expr = self.expr(); // Parse expression within parentheses
                if let Token::CloseParen = self.current_token {
                    self.next(); // Move past close parenthesis
                    expr
                } else {
                    panic!("Expected closing parenthesis")
                }
            }
            _ => panic!("Unexpected token: {:?}", self.current_token),
        };

        // Handle binary operators and logical operators
        while matches!(
            self.current_token,
            Token::Equal | Token::GreaterThan | Token::LessThan | Token::And | Token::Or
        ) {
            let op = match self.current_token {
                Token::Equal => "=".to_string(),
                Token::GreaterThan => ">".to_string(),
                Token::LessThan => "<".to_string(),
                Token::And => "AND".to_string(),
                Token::Or => "OR".to_string(),
                _ => unreachable!(),
            };
            // Move past the operator
            self.next();
            // Recursively parse the right-hand side expression
            let right = self.expr();

            left = if op == "AND" {
                Expr::And(Box::new(left), Box::new(right))
            } else if op == "OR" {
                Expr::Or(Box::new(left), Box::new(right))
            } else {
                // Comparison operators
                Expr::Comparison(Box::new(left), op, Box::new(right))
            }
        }

        left
    }

    // Parse the tokenized query returning a `Query` object.
    pub fn parse(&mut self) -> Query {
        // Ensure we're starting with a SELECT statement
        if let Token::Select = self.current_token {
            self.next(); // Move past SELECT

            // Parse columns
            let mut columns = vec![];
            while let Token::Identifier(ref col) = self.current_token {
                columns.push(col.clone());
                self.next();
                if let Token::Comma = self.current_token {
                    self.next();
                } else {
                    break;
                }
            }

            // Ensure we're at the FROM keyword
            if let Token::From = self.current_token {
                self.next(); // Move past FROM
                let table = self.ident();

                // Parse optional WHERE clause
                let mut filter = None;
                if let Token::Where = self.current_token {
                    self.next(); // Move past WHERE
                    filter = Some(self.expr());
                }

                // Parse optional ORDER BY clause
                let mut order_by = None;
                if let Token::OrderBy = self.current_token {
                    self.next(); // Move past ORDER BY
                    order_by = Some(self.ident());
                }

                // Parse optional LIMIT clause
                let mut limit = None;
                if let Token::Limit = self.current_token {
                    self.next(); // Move past LIMIT
                    limit = Some(self.number());
                }

                // Ensure we're at the end of the statement
                if let Token::Semicolon = self.current_token {
                    self.next(); // Move past semicolon
                } else if self.current_token != Token::EOF {
                    panic!(
                        "Expected semicolon or end of input found {:?}",
                        self.current_token
                    );
                }

                Query::Select {
                    columns,
                    table,
                    filter,
                    order_by,
                    limit,
                }
            } else {
                panic!("Expected FROM keyword")
            }
        } else {
            panic!("Expected SELECT keyword")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock Tokenizer for testing
    struct MockTokenizer<'a> {
        tokens: &'a [Token],
        index: usize,
    }

    impl<'a> MockTokenizer<'a> {
        fn new(tokens: &'a [Token]) -> Self {
            Self { tokens, index: 0 }
        }
    }

    impl<'a> Iterator for MockTokenizer<'a> {
        type Item = Token;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.tokens.len() {
                let token = self.tokens[self.index].clone();
                self.index += 1;
                Some(token)
            } else {
                None
            }
        }
    }

    #[test]
    fn can_parse_comparison_expressions() {
        let tokens = vec![
            Token::Identifier("age".to_string()),
            Token::GreaterThan,
            Token::Number(30),
            Token::EOF,
        ];
        let mut parser = Parser::new(tokens.into_iter());

        let expr = parser.expr();
        assert_eq!(
            expr,
            Expr::Comparison(
                Box::new(Expr::Column("age".to_string())),
                ">".to_string(),
                Box::new(Expr::Value(30))
            )
        );
    }

    #[test]
    fn can_parse_parenthesized_expression() {
        let tokens = vec![
            Token::OpenParen,
            Token::Identifier("age".to_string()),
            Token::GreaterThan,
            Token::Number(30),
            Token::CloseParen,
            Token::And,
            Token::OpenParen,
            Token::Identifier("salary".to_string()),
            Token::GreaterThan,
            Token::Number(50000),
            Token::CloseParen,
            Token::EOF,
        ];
        let mut parser = Parser::new(tokens.into_iter());

        let expr = parser.expr();
        assert_eq!(
            expr,
            Expr::And(
                Box::new(Expr::Comparison(
                    Box::new(Expr::Column("age".to_string())),
                    ">".to_string(),
                    Box::new(Expr::Value(30))
                )),
                Box::new(Expr::Comparison(
                    Box::new(Expr::Column("salary".to_string())),
                    ">".to_string(),
                    Box::new(Expr::Value(50000))
                ))
            )
        );
    }

    #[test]
    fn can_parse_basic_query() {
        let tokens = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::From,
            Token::Identifier("employees".to_string()),
            Token::EOF,
        ];
        let tokenizer = MockTokenizer::new(&tokens);
        let mut parser = Parser::new(tokenizer);
        let query = parser.parse();

        assert_eq!(
            query,
            Query::Select {
                columns: vec!["id".to_string(), "name".to_string()],
                table: "employees".to_string(),
                filter: None,
                order_by: None,
                limit: None
            }
        );
    }

    #[test]
    fn can_parse_query_with_filter() {
        let tokens = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::From,
            Token::Identifier("employees".to_string()),
            Token::Where,
            Token::Identifier("role".to_string()),
            Token::Equal,
            Token::Number(1),
            Token::EOF,
        ];
        let tokenizer = MockTokenizer::new(&tokens);
        let mut parser = Parser::new(tokenizer);
        let query = parser.parse();

        assert_eq!(
            query,
            Query::Select {
                columns: vec!["id".to_string(), "name".to_string()],
                table: "employees".to_string(),
                filter: Some(Expr::Comparison(
                    Box::new(Expr::Column("role".to_string())),
                    "=".to_string(),
                    Box::new(Expr::Value(1))
                )),
                order_by: None,
                limit: None
            }
        );
    }

    #[test]
    fn can_parse_query_with_order_by() {
        let tokens = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::From,
            Token::Identifier("employees".to_string()),
            Token::OrderBy,
            Token::Identifier("id".to_string()),
            Token::EOF,
        ];
        let tokenizer = MockTokenizer::new(&tokens);
        let mut parser = Parser::new(tokenizer);
        let query = parser.parse();

        assert_eq!(
            query,
            Query::Select {
                columns: vec!["id".to_string(), "name".to_string()],
                table: "employees".to_string(),
                filter: None,
                order_by: Some("id".to_string()),
                limit: None
            }
        );
    }

    #[test]
    fn can_parse_query_with_limit() {
        let tokens = vec![
            Token::Select,
            Token::Identifier("id".to_string()),
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::From,
            Token::Identifier("employees".to_string()),
            Token::Limit,
            Token::Number(10),
            Token::EOF,
        ];
        let tokenizer = MockTokenizer::new(&tokens);
        let mut parser = Parser::new(tokenizer);
        let query = parser.parse();

        assert_eq!(
            query,
            Query::Select {
                columns: vec!["id".to_string(), "name".to_string()],
                table: "employees".to_string(),
                filter: None,
                order_by: None,
                limit: Some(10)
            }
        );
    }
}
