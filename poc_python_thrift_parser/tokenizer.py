from enum import Enum, auto
from typing import NamedTuple, Iterator


class ParseException(Exception):
    """Raised when the tokenizer encounters an unexpected character."""
    def __init__(self, message: str, pos: "Pos"):
        super().__init__(f"{message} at {pos}")
        self.pos = pos


class TokenType(Enum):
    # our subset of keywords
    ENUM      = auto()
    UNION     = auto()
    STRUCT    = auto()
    LIST      = auto()

    # full set of Thrift keywords (so parser can handle types, etc.)
    BOOL      = auto()
    BYTE      = auto()
    UUID      = auto()
    I8        = auto()
    I16       = auto()
    I32       = auto()
    I64       = auto()
    DOUBLE    = auto()
    STRING    = auto()
    BINARY    = auto()
    REQUIRED  = auto()
    OPTIONAL  = auto()
    CONST     = auto()
    EXCEPTION = auto()
    SERVICE   = auto()
    EXTENDS   = auto()
    TYPEDEF   = auto()
    VOID      = auto()
    ONEWAY    = auto()
    SINK      = auto()

    # integer constant for field tags, default values, etc.
    INT_CONST = auto()

    # identifiers for names
    IDENT     = auto()

    # punctuation
    LBRACE    = auto()   # {
    RBRACE    = auto()   # }
    LPAREN    = auto()   # (
    RPAREN    = auto()   # )
    LT        = auto()   # <
    GT        = auto()   # >
    SEMICOLON = auto()   # ;
    COMMA     = auto()   # ,
    COLON     = auto()   # :
    EQUAL     = auto()   # =


class Pos(NamedTuple):
    idx: int   # index into the source string
    row: int   # line number, 1-based
    col: int   # column number, 1-based


class Token(NamedTuple):
    tp: TokenType
    start: Pos
    end: Pos


# map all keywords in the full Thrift grammar to their TokenType
_KEYWORDS = {
    "enum":      TokenType.ENUM,
    "union":     TokenType.UNION,
    "struct":    TokenType.STRUCT,
    "list":      TokenType.LIST,
    "bool":      TokenType.BOOL,
    "byte":      TokenType.BYTE,
    "uuid":      TokenType.UUID,
    "i8":        TokenType.I8,
    "i16":       TokenType.I16,
    "i32":       TokenType.I32,
    "i64":       TokenType.I64,
    "double":    TokenType.DOUBLE,
    "string":    TokenType.STRING,
    "binary":    TokenType.BINARY,
    "required":  TokenType.REQUIRED,
    "optional":  TokenType.OPTIONAL,
    "const":     TokenType.CONST,
    "exception": TokenType.EXCEPTION,
    "service":   TokenType.SERVICE,
    "extends":   TokenType.EXTENDS,
    "typedef":   TokenType.TYPEDEF,
    "void":      TokenType.VOID,
    "oneway":    TokenType.ONEWAY,
    "sink":      TokenType.SINK,
}

# which top-level statements to silently skip
_SKIP_STATEMENTS = {"include", "namespace"}

# which identifiers truly imply unsupported features
_NOT_IMPL = {"map", "set", "cpp_type", "throws"}


def all_tokens(src: str) -> Iterator[Token]:
    """
    Token generator for the Thrift subset:
      - enum, union, struct, list<...>
      - base types (i32, string, etc.) as keywords
      - signed integer constants for field tags/defaults
      - identifiers for names
    Skips whitespace, comments, include/namespace lines, and all @annotations.
    Raises NotImplemented for map, set, cpp_type, throws.
    """
    idx = 0
    row, col = 1, 1
    length = len(src)

    def cur_char() -> str:
        return src[idx] if idx < length else ""

    def advance(n: int = 1):
        nonlocal idx, row, col
        for _ in range(n):
            if idx >= length:
                return
            ch = src[idx]
            idx += 1
            if ch == "\n":
                row += 1
                col = 1
            else:
                col += 1

    def make_pos() -> Pos:
        return Pos(idx, row, col)

    while idx < length:
        c = cur_char()

        # --- skip whitespace ---
        if c in " \t\r\n":
            advance()
            continue

        # --- skip comments ---
        if c == "/" and idx+1 < length and src[idx+1] == "/":
            advance(2)
            while idx < length and src[idx] != "\n":
                advance()
            continue
        if c == "/" and idx+1 < length and src[idx+1] == "*":
            advance(2)
            while idx < length:
                if src[idx] == "*" and idx+1 < length and src[idx+1] == "/":
                    advance(2)
                    break
                advance()
            else:
                raise ParseException("Unterminated /* comment */", make_pos())
            continue

        # --- skip annotations (@Foo = "bar") ---
        if c == "@":
            advance()
            if not (cur_char().isalpha() or cur_char() == "_"):
                raise ParseException("Bad annotation name", make_pos())
            while idx < length and (src[idx].isalnum() or src[idx] == "_"):
                advance()
            while cur_char() in " \t\r\n":
                advance()
            if cur_char() == "=":
                advance()
                while cur_char() in " \t\r\n":
                    advance()
                if cur_char() == '"':
                    advance()
                    while idx < length:
                        if src[idx] == "\\":
                            advance(2)
                        elif src[idx] == '"':
                            advance()
                            break
                        else:
                            advance()
                else:
                    while idx < length and (src[idx].isdigit() or src[idx] in "+-"):
                        advance()
            continue

        # record start position for next token
        start = make_pos()

        # --- integer constants (['+'|'-']? Digit+) ---
        if c.isdigit() or (c in "+-" and idx+1 < length and src[idx+1].isdigit()):
            # optional sign
            if c in "+-":
                advance()
            # digits
            if not cur_char().isdigit():
                raise ParseException("Invalid integer literal", make_pos())
            while idx < length and src[idx].isdigit():
                advance()
            end = make_pos()
            yield Token(TokenType.INT_CONST, start, end)
            continue

        # --- identifiers & keywords ---
        if c.isalpha() or c == "_":
            buf = []
            while idx < length and (src[idx].isalnum() or src[idx] == "_"):
                buf.append(src[idx])
                advance()
            word = "".join(buf)

            # skip include/namespace directives
            if word in _SKIP_STATEMENTS:
                while idx < length and src[idx] != ";":
                    if src[idx] in " \t\r\n":
                        advance()
                    elif src[idx] == "/":
                        break  # let outer loop handle comment
                    else:
                        advance()
                if cur_char() == ";":
                    advance()
                continue

            # truly unsupported
            if word in _NOT_IMPL:
                raise NotImplemented(f"Unsupported feature '{word}'", start)

            tp = _KEYWORDS.get(word, TokenType.IDENT)
            end = make_pos()
            yield Token(tp, start, end)
            continue

        # --- punctuation ---
        punct_map = {
            "{": TokenType.LBRACE,   "}": TokenType.RBRACE,
            "(": TokenType.LPAREN,   ")": TokenType.RPAREN,
            "<": TokenType.LT,       ">": TokenType.GT,
            ";": TokenType.SEMICOLON,",": TokenType.COMMA,
            ":": TokenType.COLON,    "=": TokenType.EQUAL,
        }
        if c in punct_map:
            advance()
            yield Token(punct_map[c], start, make_pos())
            continue

        # nothing matched
        raise ParseException(f"Unexpected character '{c}'", start)


if __name__ == '__main__':
    src = open('parquet.thrift', 'r').read()
    gen = all_tokens(src)
    print(list(gen))
