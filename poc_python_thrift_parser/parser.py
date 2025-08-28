from typing import NamedTuple, List, Optional, Union
from tokenizer import (
    all_tokens,
    ParseException,
    Token,
    TokenType,
    Pos,
)


#
# === AST node definitions ===
#

class IDLFile(NamedTuple):
    """A Thrift IDL file = a list of top-level definitions."""
    definitions: List["Definition"]


class EnumMember(NamedTuple):
    name: str
    value: Optional[int]


class EnumDef(NamedTuple):
    name: str
    members: List[EnumMember]


class Field(NamedTuple):
    id: int
    # TODO: there is also 'default requiredness', which is when you leave
    # out the "required/optional" token. Parquet thrift always has it set. I 
    # didn't understand the semantics when reading 
    # https://thrift.apache.org/docs/idl#default-requiredness-implicit
    # so not supporting ATM
    required: bool
    type: "Type"
    name: str
    default: Optional[Union[int, str]]    # integer or identifier/string literal


class StructDef(NamedTuple):
    name: str
    fields: List[Field]


class UnionDef(NamedTuple):
    name: str
    fields: List[Field]


class NamedType(NamedTuple):
    """Either a builtin (i32, string, …) or a user-defined type."""
    name: str


class ListType(NamedTuple):
    elem_type: "Type"


# A Field’s type can be either a NamedType or a ListType
Type = Union[NamedType, ListType]

# A top-level definition
Definition = Union[EnumDef, StructDef, UnionDef]


#
# === The parser itself ===
#

class Parser:
    def __init__(self, src: str) -> None:
        self.src = src
        self.tokens: List[Token] = list(all_tokens(src))
        self.pos = 0

    def peek(self) -> Optional[Token]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def next(self) -> Token:
        tok = self.peek()
        if tok is None:
            raise ParseException("Unexpected end of input", Pos(-1, -1, -1))
        self.pos += 1
        return tok

    def match(self, tp: TokenType) -> bool:
        if (tok := self.peek()) and tok.tp is tp:
            self.pos += 1
            return True
        return False

    def expect(self, tp: TokenType) -> Token:
        tok = self.next()
        if tok.tp is not tp:
            raise ParseException(f"Expected {tp}, got {tok.tp}", tok.start)
        return tok

    def text(self, tok: Token) -> str:
        return self.src[tok.start.idx : tok.end.idx]

    def parse(self) -> IDLFile:
        defs: List[Definition] = []
        while self.peek() is not None:
            # skip namespace directives
            if (tok := self.peek()) and tok.tp is TokenType.IDENT and self.text(tok) == "namespace":
                self.next()
                self.expect(TokenType.IDENT)
                self.expect(TokenType.IDENT)
                continue
            defs.append(self.parse_definition())
            # optional list separator after a definition
            _ = self.match(TokenType.COMMA) or self.match(TokenType.SEMICOLON)
        return IDLFile(defs)

    def parse_definition(self) -> Definition:
        tok = self.peek()
        if tok.tp is TokenType.ENUM:
            return self.parse_enum()
        if tok.tp is TokenType.STRUCT:
            return self.parse_struct()
        if tok.tp is TokenType.UNION:
            return self.parse_union()
        raise ParseException("Expected enum/struct/union", tok.start)

    def parse_enum(self) -> EnumDef:
        self.expect(TokenType.ENUM)
        name_tok = self.expect(TokenType.IDENT)
        name = self.text(name_tok)

        self.expect(TokenType.LBRACE)
        members: List[EnumMember] = []
        while not self.match(TokenType.RBRACE):
            m_tok = self.expect(TokenType.IDENT)
            m_name = self.text(m_tok)
            if self.match(TokenType.EQUAL):
                v_tok = self.expect(TokenType.INT_CONST)
                m_val = int(self.text(v_tok))
            else:
                m_val = None
            # optional list separator
            _ = self.match(TokenType.COMMA) or self.match(TokenType.SEMICOLON)
            members.append(EnumMember(m_name, m_val))
        return EnumDef(name, members)

    def parse_struct(self) -> StructDef:
        self.expect(TokenType.STRUCT)
        name_tok = self.expect(TokenType.IDENT)
        name = self.text(name_tok)
        # optional xsd_all
        if (tok := self.peek()) and tok.tp is TokenType.IDENT and self.text(tok) == "xsd_all":
            self.next()
        if self.match(TokenType.EXTENDS):
            raise NotImplementedError("`extends` not supported", name_tok.start)
        self.expect(TokenType.LBRACE)
        fields: List[Field] = []
        while not self.match(TokenType.RBRACE):
            fields.append(self.parse_field())
        return StructDef(name, fields)

    def parse_union(self) -> UnionDef:
        self.expect(TokenType.UNION)
        name_tok = self.expect(TokenType.IDENT)
        name = self.text(name_tok)
        # optional xsd_all
        if (tok := self.peek()) and tok.tp is TokenType.IDENT and self.text(tok) == "xsd_all":
            self.next()
        self.expect(TokenType.LBRACE)
        fields: List[Field] = []
        while not self.match(TokenType.RBRACE):
            fields.append(self.parse_field())
        return UnionDef(name, fields)

    def parse_field(self) -> Field:
        id_tok = self.expect(TokenType.INT_CONST)
        field_id = int(self.text(id_tok))
        self.expect(TokenType.COLON)
        if self.match(TokenType.REQUIRED):
            required = True
        elif self.match(TokenType.OPTIONAL):
            required = False
        else:
            # This should ONLY happen when parsing unions; in that case
            # code gen doesn't look at the required field, but it's treated as 
            # optional.
            required = False
        ftype = self.parse_type()
        name_tok = self.expect(TokenType.IDENT)
        name = self.text(name_tok)
        default: Optional[Union[int, str]] = None
        if self.match(TokenType.EQUAL):
            v_tok = self.next()
            if v_tok.tp is TokenType.INT_CONST:
                default = int(self.text(v_tok))
            elif v_tok.tp is TokenType.IDENT:
                default = self.text(v_tok)
            else:
                raise ParseException("Expected constant or identifier for default", v_tok.start)
        # optional list separator after field
        _ = self.match(TokenType.COMMA) or self.match(TokenType.SEMICOLON)
        return Field(field_id, required, ftype, name, default)

    def parse_type(self) -> Type:
        if self.match(TokenType.LIST):
            self.expect(TokenType.LT)
            elem = self.parse_type()
            self.expect(TokenType.GT)
            return ListType(elem)
        tok = self.next()
        # built-in base types or user-defined
        if tok.tp in {
            TokenType.BOOL, TokenType.BYTE, TokenType.I8, TokenType.I16, TokenType.I32,
            TokenType.I64, TokenType.DOUBLE, TokenType.STRING, TokenType.BINARY, TokenType.UUID
        } or tok.tp is TokenType.IDENT:
            return NamedType(self.text(tok))
        raise ParseException("Expected a type", tok.start)


def parse_idl(src: str) -> IDLFile:
    """
    Convenience entry point.
    >>> ast = parse_idl(open("foo.thrift").read())
    """
    try:
        return Parser(src).parse()
    except ParseException as e:
        lines = src.splitlines()
        row = e.pos.row
        if row > 1:
            print(lines[row - 2])
        if 1 <= row <= len(lines):
            line = lines[row - 1]
            print(line)
            print(' ' * (e.pos.col - 1) + '^')
        print(f"Parse error: {e}")
        raise


#
# === Pretty printer ===
#

def _type_to_str(t: Type) -> str:
    if isinstance(t, NamedType):
        return t.name
    if isinstance(t, ListType):
        return f"list<{_type_to_str(t.elem_type)}>"
    raise ValueError(f"Unknown type: {t}")


def pretty_print(idl: IDLFile) -> str:
    """
    Produce a Thrift-IDL-like textual representation of the parsed AST.
    """
    lines: List[str] = []
    for d in idl.definitions:
        if isinstance(d, EnumDef):
            lines.append(f"enum {d.name} {{")
            for m in d.members:
                if m.value is not None:
                    lines.append(f"  {m.name} = {m.value},")
                else:
                    lines.append(f"  {m.name},")
            lines.append("}\n")
        elif isinstance(d, StructDef):
            lines.append(f"struct {d.name} {{")
            for f in d.fields:
                # TODO: not correct, there is also default requiredness
                req = "required " if f.required else "optional "
                tstr = _type_to_str(f.type)
                default = f" = {f.default}" if f.default is not None else ""
                lines.append(f"  {f.id}: {req}{tstr} {f.name}{default};")
            lines.append("}\n")
        elif isinstance(d, UnionDef):
            lines.append(f"union {d.name} {{")
            for f in d.fields:
                # TODO: not correct, there is also default requiredness
                req = "required " if f.required else "optional "
                tstr = _type_to_str(f.type)
                default = f" = {f.default}" if f.default is not None else ""
                lines.append(f"  {f.id}: {req}{tstr} {f.name}{default};")
            lines.append("}\n")
    return "\n".join(lines)


if __name__ == '__main__':
    import sys
    src = open(sys.argv[1]).read()
    idl = parse_idl(src)
    print(pretty_print(idl))
