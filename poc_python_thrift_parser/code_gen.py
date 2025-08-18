import sys
import os


from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Dict, Union

from parser import (
    parse_idl,
    IDLFile,
    StructDef, EnumDef, UnionDef, Definition,
    Field,
    Type,
    NamedType, ListType,
    EnumMember,
)


BASIC_NAMED_TO_ZIG = {
    "bool": "bool",
    "double": "f64",
    "string": "[]const u8",
    "binary": "[]const u8",
    "i8": "i8",
    "i16": "i16",
    "i32": "i32",
    "i64": "i64",
}

BASIC_NAMED_TO_TTYPE = {
    "bool": "BOOL",
    "double": "DOUBLE",
    "string": "STRING",
    "binary": "STRING",
    "i8": "BYTE",
    "i16": "I16",
    "i32": "I32",
    "i64": "I64",
}

# TODO: TTYPE (thrift logical types) differentiates between BYTE and I08, but
# CTYPE (compact protocol enum type values) uses BYTE for both. Current codegen
# maps CTYPE.I08 to TTYPE.BYTE and assumes you always want .readI08 both when thrift
# type is byte and i8. Consequence - thrift IDL with i8 works (and maps to zig i8),
# but byte does not. This is solvable, but requires some rewriting in the code gen.
TTYPE_TO_CMD = {
    "BYTE": "I08",
    "I16": "I16",
    "I32": "I32",
    "I64": "I64",
    "BOOL": "Bool",
    "STRING": "Binary",
    "DOUBLE": "TODO_NOT_IMPLEMENTED",
}

# Reader method names for basic, non-allocating types
TTYPE_TO_READFN = {
    "BYTE": "readI08",
    "I16": "readI16",
    "I32": "readI32",
    "I64": "readI64",
    "BOOL": "readBool",
    "DOUBLE": "readDouble",
    # "STRING" handled specially (requires allocation)
}

SAMPLE_LITERALS = {
    "bool": "true",
    "i8": "12",
    "i16": "123",
    "i32": "12345",
    "i64": "123456789",
    "string": '"hello world"',
    "binary": '"hello world"',
}


def _get_list_elem_type(list_t: ListType) -> Type:
    for attr in ("elem", "elem_type", "element", "value_type", "ty"):
        if hasattr(list_t, attr):
            return getattr(list_t, attr)
    raise NotImplementedError(f"Unsupported ListType representation: {list_t!r}")


def classify_type(t: Type, defsmap: dict[str, Definition]) -> tuple[str, bool]:
    if isinstance(t, NamedType):
        if t.name in BASIC_NAMED_TO_TTYPE:
            return BASIC_NAMED_TO_TTYPE[t.name], False
        df = defsmap[t.name]
        if isinstance(df, (StructDef, UnionDef)):
            return "STRUCT", False
        if isinstance(df, EnumDef):
            return "I32", True
    if isinstance(t, ListType):
        raise NotImplementedError(f"Unsupported type: {t}")
    raise NotImplementedError(f"Unsupported type: {t}")


def classify_list_elem_type(elem_t: Type, defsmap: dict[str, Definition]) -> tuple[str, bool]:
    if isinstance(elem_t, NamedType):
        if elem_t.name in BASIC_NAMED_TO_TTYPE:
            return BASIC_NAMED_TO_TTYPE[elem_t.name], False
        df = defsmap[elem_t.name]
        if isinstance(df, (StructDef, UnionDef)):
            return "STRUCT", False
        if isinstance(df, EnumDef):
            return "I32", True
    raise NotImplementedError(f"Unsupported list element type: {elem_t}")


def _is_allocating_named(name: str) -> bool:
    return name in ("string", "binary")


def emit_write_payload(indent: str, ttype: str, value_expr: str, is_enum: bool) -> list[str]:
    if is_enum:
        return [f"{indent}try writer.write(.{{ .I32 = @intFromEnum({value_expr}) }});"]
    if ttype in TTYPE_TO_CMD:
        cmd = TTYPE_TO_CMD[ttype]
        if cmd == "TODO_NOT_IMPLEMENTED":
            raise NotImplementedError("DOUBLE writing not implemented yet")
        return [f"{indent}try writer.write(.{{ .{cmd} = {value_expr} }});"]
    if ttype == "STRUCT":
        return [f"{indent}try {value_expr}.write(writer);"]
    raise NotImplementedError("not yet implemented...")


class ZigTypeSystem:
    def get_zig_type(self, type: Type, is_required: bool) -> str:
        if isinstance(type, NamedType):
            base_type = BASIC_NAMED_TO_ZIG.get(type.name, type.name)
        elif isinstance(type, ListType):
            elem_t = _get_list_elem_type(type)
            elem_zig = self.get_zig_type(elem_t, True)
            base_type = f"std.ArrayList({elem_zig})"
        else:
            base_type = ""
        return base_type if is_required else f"?{base_type}"


def _emit_write_list_field(indent: str, field_name: str, list_expr: str, elem_ttype: str, elem_is_enum: bool) -> list[str]:
    out: list[str] = []
    out.append(f'{indent}try writer.write(.{{ .FieldBegin = .{{ .tp = TType.LIST, .fid = @intFromEnum(FieldTag.{field_name}) }} }});')
    out.append(f'{indent}try writer.write(.{{ .ListBegin = .{{ .elem_type = TType.{elem_ttype}, .size = @intCast({list_expr}.items.len) }} }});')
    out.append(f'{indent}for ({list_expr}.items) |item| {{')
    out.extend(emit_write_payload(indent + "    ", elem_ttype, "item", elem_is_enum))
    out.append(f"{indent}}}")
    out.append(f"{indent}try writer.write(.ListEnd);")
    out.append(f"{indent}try writer.write(.FieldEnd);")
    return out


def write_field(f: Field, defsmap: dict[str, Definition]) -> str:
    indent = "        "
    if isinstance(f.type, ListType):
        # writing lists is already supported; keep as-is
        elem_t = _get_list_elem_type(f.type)
        elem_ttype, elem_is_enum = classify_list_elem_type(elem_t, defsmap)
        out: list[str] = []
        if not f.required:
            out.append(f"{indent}if (self.{f.name}) |list| {{")
            inner_indent = indent + "    "
            out.extend(_emit_write_list_field(inner_indent, f.name, "list", elem_ttype, elem_is_enum))
            out.append(indent + "}")
        else:
            out.extend(_emit_write_list_field(indent, f.name, f"self.{f.name}", elem_ttype, elem_is_enum))
        return "\n".join(out)

    ttype, is_enum = classify_type(f.type, defsmap)
    value = f"self.{f.name}" if f.required else "value"
    out = []
    if not f.required:
        out.append(f"{indent}if (self.{f.name}) |value| {{")
        indent += "    "
    out.append(f'{indent}try writer.write(.{{ .FieldBegin = .{{ .tp = TType.{ttype}, .fid = @intFromEnum(FieldTag.{f.name}) }} }});')
    out.extend(emit_write_payload(indent, ttype, value, is_enum))
    out.append(f"{indent}try writer.write(.FieldEnd);")
    if not f.required:
        out.append(indent[:-4] + "}")
    return "\n".join(out)


def _emit_read_list_item_lines(indent: str, list_expr: str, elem_t: Type, defsmap: dict[str, Definition]) -> list[str]:
    lines: list[str] = []
    if isinstance(elem_t, NamedType):
        n = elem_t.name
        if n in BASIC_NAMED_TO_TTYPE:
            ttype = BASIC_NAMED_TO_TTYPE[n]
            if ttype == "STRING":
                lines.append(f"{indent}const item = try r.readBinary(alloc);")
                lines.append(f"{indent}errdefer alloc.free(item);")
                lines.append(f"{indent}try {list_expr}.append(alloc, item);")
                return lines
            if ttype in TTYPE_TO_READFN:
                read_fn = TTYPE_TO_READFN[ttype]
                lines.append(f"{indent}const item = try r.{read_fn}();")
                lines.append(f"{indent}try {list_expr}.append(alloc, item);")
                return lines
        if n in defsmap and isinstance(defsmap[n], EnumDef):
            lines.append(f"{indent}const item: {n} = @enumFromInt(try r.readI32());")
            lines.append(f"{indent}try {list_expr}.append(alloc, item);")
            return lines
        if n in defsmap and isinstance(defsmap[n], (StructDef, UnionDef)):
            lines.append(f"{indent}if (try readCatchThrift({n}, r, alloc)) |item| {{")
            lines.append(f"{indent}    // @constCast OKAY here: deinit only frees heap pointers inside stack-copied 'item'.")
            lines.append(f"{indent}    errdefer @constCast(&item).deinit(alloc);")
            lines.append(f"{indent}    try {list_expr}.append(alloc, item);")
            lines.append(f"{indent}}}")
            return lines
    raise NotImplementedError(f"Unsupported list element type for reading: {elem_t!r}")

def _emit_read_field_case(f: Field, defsmap: dict[str, Definition], type_system: ZigTypeSystem) -> list[str]:
    # Implement reading for enums, STRING/BINARY, structs, unions, and now LISTs
    if isinstance(f.type, NamedType):
        n = f.type.name
        # Basic primitives (non-allocating)
        if n in BASIC_NAMED_TO_TTYPE:
            ttype = BASIC_NAMED_TO_TTYPE[n]
            if ttype == "STRING":
                # STRING/BINARY require allocation
                return [
                    f"            .{f.name} => {{",
                    f"                if (field.tp == TType.STRING) {{",
                    f"                    self.{f.name} = try r.readBinary(alloc);",
                    f"                    isset.{f.name} = true;",
                    f"                    return;",
                    f"                }}",
                    f"                continue :sw .default;",
                    f"            }},",
                ]
            if ttype in TTYPE_TO_READFN:
                read_fn = TTYPE_TO_READFN[ttype]
                return [
                    f"            .{f.name} => {{",
                    f"                if (field.tp == TType.{ttype}) {{",
                    f"                    self.{f.name} = try r.{read_fn}();",
                    f"                    isset.{f.name} = true;",
                    f"                    return;",
                    f"                }}",
                    f"                continue :sw .default;",
                    f"            }},",
                ]
        # Enums (I32 on the wire)
        if n in defsmap and isinstance(defsmap[n], EnumDef):
            return [
                f"            .{f.name} => {{",
                f"                if (field.tp == TType.I32) {{",
                f"                    self.{f.name} = @enumFromInt(try r.readI32());",
                f"                    isset.{f.name} = true;",
                f"                    return;",
                f"                }}",
                f"                continue :sw .default;",
                f"            }},",
            ]
        # Struct field (named) — use readCatchThrift
        if n in defsmap and isinstance(defsmap[n], StructDef):
            return [
                f"            .{f.name} => {{",
                f"                if (field.tp == TType.STRUCT) {{",
                f"                    if (try readCatchThrift({n}, r, alloc)) |value| {{",
                f"                        self.{f.name} = value;",
                f"                        isset.{f.name} = true;",
                f"                    }}",
                f"                    return;",
                f"                }}",
                f"                continue :sw .default;",
                f"            }},",
            ]

        # Union field (named) — use readCatchThrift
        if n in defsmap and isinstance(defsmap[n], UnionDef):
            return [
                f"            .{f.name} => {{",
                f"                if (field.tp == TType.STRUCT) {{",
                f"                    if (try readCatchThrift({n}, r, alloc)) |value| {{",
                f"                        self.{f.name} = value;",
                f"                        isset.{f.name} = true;",
                f"                    }}",
                f"                    return;",
                f"                }}",
                f"                continue :sw .default;",
                f"            }},",
            ]

    if isinstance(f.type, ListType):
        elem_t = _get_list_elem_type(f.type)
        elem_zig = type_system.get_zig_type(elem_t, True)
        list_expr = f"self.{f.name}.?" if not f.required else f"self.{f.name}"
        lines = [
            f"            .{f.name} => {{",
            f"                if (field.tp == TType.LIST) {{",
            f"                    const list_meta = try r.readListBegin();",
            f"                    self.{f.name} = std.ArrayList({elem_zig}).empty;",
            f"                    isset.{f.name} = true;",
            f"                    try {list_expr}.ensureTotalCapacity(alloc, list_meta.size);",
            f"                    for (0..list_meta.size) |_| {{",
        ]
        # delegate per-element
        lines.extend(_emit_read_list_item_lines("                        ", list_expr, elem_t, defsmap))
        lines.extend([
            f"                    }}",
            f"                    try r.readListEnd();",
            f"                    return;",
            f"                }}",
            f"                continue :sw .default;",
            f"            }},",
        ])
        return lines

    # default skip
    return [
        f"            .{f.name} => {{",
        f"                continue :sw .default;",
        f"            }},",
    ]

def _emit_read_union_field_case(union_name: str, f: Field, defsmap: dict[str, Definition]) -> list[str]:
    if isinstance(f.type, NamedType):
        n = f.type.name
        # primitives / string / binary — unchanged
        if n in BASIC_NAMED_TO_TTYPE:
            ttype = BASIC_NAMED_TO_TTYPE[n]
            if ttype == "STRING":
                return [
                    f"            .{f.name} => {{",
                    f"                if (field.tp == TType.STRING) {{",
                    f"                    return {union_name}{{ .{f.name} = try r.readBinary(alloc) }};",
                    f"                }}",
                    f"                continue :sw .default;",
                    f"            }},",
                ]
            if ttype in TTYPE_TO_READFN:
                read_fn = TTYPE_TO_READFN[ttype]
                return [
                    f"            .{f.name} => {{",
                    f"                if (field.tp == TType.{ttype}) {{",
                    f"                    return {union_name}{{ .{f.name} = try r.{read_fn}() }};",
                    f"                }}",
                    f"                continue :sw .default;",
                    f"            }},",
                ]
        # enum — unchanged
        if n in defsmap and isinstance(defsmap[n], EnumDef):
            return [
                f"            .{f.name} => {{",
                f"                if (field.tp == TType.I32) {{",
                f"                    return {union_name}{{ .{f.name} = @enumFromInt(try r.readI32()) }};",
                f"                }}",
                f"                continue :sw .default;",
                f"            }},",
            ]
        # nested struct — use readCatchThrift
        if n in defsmap and isinstance(defsmap[n], StructDef):
            return [
                f"            .{f.name} => {{",
                f"                if (field.tp == TType.STRUCT) {{",
                f"                    if (try readCatchThrift({n}, r, alloc)) |value| {{",
                f"                        return {union_name}{{ .{f.name} = value }};",
                f"                    }}",
                f"                    return null;",
                f"                }}",
                f"                continue :sw .default;",
                f"            }},",
            ]
        # nested union — use readCatchThrift
        if n in defsmap and isinstance(defsmap[n], UnionDef):
            return [
                f"            .{f.name} => {{",
                f"                if (field.tp == TType.STRUCT) {{",
                f"                    if (try readCatchThrift({n}, r, alloc)) |value| {{",
                f"                        return {union_name}{{ .{f.name} = value }};",
                f"                    }}",
                f"                    return null;",
                f"                }}",
                f"                continue :sw .default;",
                f"            }},",
            ]
    # lists/others not supported for unions
    return [
        f"            .{f.name} => {{",
        f"                continue :sw .default;",
        f"            }},",
    ]

def _emit_deinit_for_elem(indent: str, item_expr: str, elem_t: Type, defsmap: dict[str, Definition]) -> list[str]:
    lines: list[str] = []
    if isinstance(elem_t, NamedType):
        n = elem_t.name
        if n in ("string", "binary"):
            lines.append(f"{indent}alloc.free(({item_expr}).*);")
        elif n in defsmap and isinstance(defsmap[n], StructDef):
            lines.append(f"{indent}{item_expr}.deinit(alloc);")
        elif n in defsmap and isinstance(defsmap[n], UnionDef):
            lines.append(f"{indent}{item_expr}.deinit(alloc);")
    return lines

def _emit_deinit_for_field(indent: str, container_expr: str, f: Field, defsmap: dict[str, Definition]) -> list[str]:
    """
    container_expr is e.g. 'self' for struct.deinit, or 'obj' for errdefer cleanup.
    This emits unconditional cleanup (for deinit). The caller should wrap with option/isset guards as appropriate.
    """
    lines: list[str] = []
    if isinstance(f.type, NamedType):
        n = f.type.name
        if n in ("string", "binary"):
            # free string/binary
            if f.required:
                lines.append(f"{indent}alloc.free({container_expr}.{f.name});")
            else:
                lines.append(f"{indent}if ({container_expr}.{f.name}) |s| alloc.free(s);")
        else:
            if n in defsmap and isinstance(defsmap[n], StructDef):
                if f.required:
                    lines.append(f"{indent}{container_expr}.{f.name}.deinit(alloc);")
                else:
                    lines.append(f"{indent}if ({container_expr}.{f.name}) |*v| v.deinit(alloc);")
            elif n in defsmap and isinstance(defsmap[n], UnionDef):
                if f.required:
                    lines.append(f"{indent}{container_expr}.{f.name}.deinit(alloc);")
                else:
                    lines.append(f"{indent}if ({container_expr}.{f.name}) |*v| v.deinit(alloc);")
    elif isinstance(f.type, ListType):
        elem_t = _get_list_elem_type(f.type)
        if f.required:
            # iterate items first, then deinit list
            lines.append(f"{indent}for ({container_expr}.{f.name}.items) |*item| {{")
            lines.append(f"{indent}    use_arg(item);")
            lines.extend(_emit_deinit_for_elem(indent + "    ", "item", elem_t, defsmap))
            lines.append(f"{indent}}}")
            lines.append(f"{indent}{container_expr}.{f.name}.deinit(alloc);")
        else:
            lines.append(f"{indent}if ({container_expr}.{f.name}) |*list| {{")
            lines.append(f"{indent}    for (list.items) |*item| {{")
            lines.append(f"{indent}        use_arg(item);")
            lines.extend(_emit_deinit_for_elem(indent + "        ", "item", elem_t, defsmap))
            lines.append(f"{indent}    }}")
            lines.append(f"{indent}    list.deinit(alloc);")
            lines.append(f"{indent}}}")
    return lines


def _emit_errdefer_cleanup(indent: str, obj_name: str, struct_def: StructDef, defsmap: dict[str, Definition]) -> str:
    """
    Generate:
        errdefer {
            if (isset.field) { ...free obj.field... }
        }
    """
    lines: list[str] = [f"{indent}errdefer {{"]

    for f in struct_def.fields:
        per_field: list[str] = []
        # Only attempt cleanup if field had been set during parse
        per_field.append(f"{indent}    if (isset.{f.name}) {{")
        per_field.extend(_emit_deinit_for_field(indent + "        ", obj_name, f, defsmap))
        per_field.append(f"{indent}    }}")
        lines.extend(per_field)

    lines.append(f"{indent}}}")
    return "\n".join(lines)


class CodeGenerator:
    def __init__(self, idl_file: IDLFile):
        self.idl = idl_file
        self.structs: list[StructDef] = [x for x in idl_file.definitions if isinstance(x, StructDef)]
        self.enums: list[EnumDef] = [x for x in idl_file.definitions if isinstance(x, EnumDef)]
        self.unions: list[UnionDef] = [x for x in idl_file.definitions if isinstance(x, UnionDef)]
        self.defsmap: dict[str, Definition] = {}
        for x in idl_file.definitions:
            if isinstance(x, (StructDef, EnumDef, UnionDef)):
                self.defsmap[x.name] = x
        self.type_system = ZigTypeSystem()

    def generate(self) -> str:
        parts = [self._generate_header()]
        for definition in self.idl.definitions:
            if isinstance(definition, StructDef):
                parts.append(self.generate_struct(definition))
            elif isinstance(definition, EnumDef):
                parts.append(self.generate_enum(definition))
            elif isinstance(definition, UnionDef):
                parts.append(self.generate_union(definition))
            else:
                assert False, f"Unsupported definition type: {type(definition)}"
        parts.append(self._generate_test_block())
        return "\n\n".join(parts)

    def _generate_header(self) -> str:
        return '''// Generated by thrift-zig-codegen
const std = @import("std");
const TCompactProtocol = @import("src/TCompactProtocol.zig");
const Writer = TCompactProtocol.Writer;
const Reader = TCompactProtocol.Reader;
const TType = TCompactProtocol.TType;
const FieldMeta = TCompactProtocol.FieldMeta;
const WriterError = Writer.WriterError;
const CompactProtocolError = Reader.CompactProtocolError || error{NotImplemented};
const ThriftError = Reader.ThriftError;

fn use_arg(t: anytype) void {
    _ = t;
}

fn readFieldOrStop(r: *Reader) CompactProtocolError!?FieldMeta {
    const field = try r.readFieldBegin();
    if (field.tp == .STOP) return null;
    return field;
}


/// Wraps struct/union .read and returns 'null' on ThriftError
fn readCatchThrift(T: type, r: *Reader, alloc: std.mem.Allocator) CompactProtocolError!?T {
    if (T.read(r, alloc)) |value| {
        return value;
    } else |err| switch (err) {
        ThriftError.CantParseUnion, ThriftError.RequiredFieldMissing => {
            return null;
        },
        else => |err2| return err2,
    }
}
'''

    def generate_enum(self, enum_def: EnumDef) -> str:
        lines: list[str] = [f'pub const {enum_def.name} = enum(i32) {{']
        indent = '    '
        for member in enum_def.members:
            name = member.name
            value = member.value
            if value is None:
                raise NotImplementedError("can't auto-assign enum values yet; include in Thrift")
            lines.append(f"{indent}{name} = {value},")
        lines.append(f"{indent}_,")
        lines.append("};")
        return '\n'.join(lines)

    def _gen_struct_deinit(self, struct_def: StructDef) -> str:
        body_lines: list[str] = []
        for f in struct_def.fields:
            body_lines.extend(_emit_deinit_for_field("        ", "self", f, self.defsmap))
        body = "\n".join(body_lines) if body_lines else "        return;"
        return f"""    pub fn deinit(self: *{struct_def.name}, alloc: std.mem.Allocator) void {{
        use_arg(self);
        use_arg(alloc);
{body}
    }}"""

    def _gen_union_deinit(self, union_def: UnionDef) -> str:
        cases: list[str] = []
        for f in union_def.fields:
            payload_cleanup = _emit_deinit_for_elem("                ", "payload", f.type, self.defsmap)
            if payload_cleanup:
                cases.append(f"            .{f.name} => |*payload| {{")
                cases.append(f"                use_arg(payload);")
                cases.extend(payload_cleanup)
                cases.append("            },")
            else:
                cases.append(f"            .{f.name} => |payload| {{ use_arg(payload); }},")

        joined = "\n".join(cases)
        return f"""    pub fn deinit(self: *{union_def.name}, alloc: std.mem.Allocator) void {{
        use_arg(alloc);
        switch (self.*) {{
{joined}
        }}
    }}"""

    def generate_struct(self, struct_def: StructDef) -> str:
        fields_str = []
        for f in struct_def.fields:
            zig_type = self.type_system.get_zig_type(f.type, f.required)
            fields_str.append(f"    {f.name}: {zig_type},")
        indent = '        '
        field_tags_str = []
        for f in struct_def.fields:
            field_tags_str.append(f"{indent}{f.name} = {f.id},")
        field_tags_str.append(f"{indent}default = std.math.maxInt(i16),")
        field_tags_str.append(f"{indent}_,")
        write_calls = [f"{indent}use_arg(self);"]
        write_calls.append(f"{indent}try writer.write(.StructBegin);")
        for f in struct_def.fields:
            write_calls.append(write_field(f, self.defsmap))
        write_calls.append(f"{indent}try writer.write(.FieldStop);")
        write_calls.append(f"{indent}try writer.write(.StructEnd);")
        fields_joined = "\n".join(fields_str)
        field_tags_joined = "\n".join(field_tags_str)
        write_calls_joined = "\n".join(write_calls)

        isset_fields = ", ".join([f"{f.name}: bool = false" for f in struct_def.fields])
        isset_block = f"    const Isset = struct {{ {isset_fields} }};"

        req_checks = []
        for f in struct_def.fields:
            if f.required:
                req_checks.append(f"        if (!is.{f.name}) return ThriftError.RequiredFieldMissing;")
        if req_checks:
            validate_body = "\n".join(req_checks + ["        return;"])
        else:
            validate_body = "        use_arg(is);\n        return;"
        validate_fn = f"""    fn validate(is: Isset) ThriftError!void {{
{validate_body}
    }}"""

        field_cases_lines: list[str] = []
        for f in struct_def.fields:
            field_cases_lines.extend(_emit_read_field_case(f, self.defsmap, self.type_system))
        read_field_helper = f"""    fn read{struct_def.name}Field(self: *{struct_def.name}, r: *Reader, alloc: std.mem.Allocator, isset: *Isset, field: FieldMeta) CompactProtocolError!void {{
        use_arg(self);
        use_arg(r);
        use_arg(alloc);
        use_arg(isset);
        // list field parsing now implemented
        sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {{
{'\n'.join(field_cases_lines)}
            .default => try r.skip(field.tp),
            else => continue :sw .default,
        }}
    }}"""

        init_parts = []
        for f in struct_def.fields:
            init_val = "undefined" if f.required else "null"
            init_parts.append(f".{f.name} = {init_val}")
        init_init = ", ".join(init_parts)

        # read() with errdefer cleanup
        errdefer_block = _emit_errdefer_cleanup("        ", "obj", struct_def, self.defsmap)

        read_method = f"""    pub fn read(r: *Reader, alloc: std.mem.Allocator) (CompactProtocolError || ThriftError)!{struct_def.name} {{
        var obj: {struct_def.name} = .{{ {init_init} }};
        var isset: Isset = .{{}};
{errdefer_block}

        try r.readStructBegin();
        while (try readFieldOrStop(r)) |field| {{
            try obj.read{struct_def.name}Field(r, alloc, &isset, field);
            try r.readFieldEnd();
        }}
        try r.readStructEnd();

        try validate(isset);
        return obj;
    }}"""

        return f'''pub const {struct_def.name} = struct {{
{fields_joined}

    const FieldTag = enum(i16) {{
{field_tags_joined}
    }};

{isset_block}

    pub fn write(self: *const {struct_def.name}, writer: *Writer) WriterError!void {{
{write_calls_joined}
    }}

{self._gen_struct_deinit(struct_def)}

{validate_fn}

{read_field_helper}

{read_method}
}};'''

    def generate_union(self, union_def: UnionDef) -> str:
        fields_str = []
        for f in union_def.fields:
            zig_type = self.type_system.get_zig_type(f.type, True)
            fields_str.append(f"    {f.name}: {zig_type},")
        indent = '        '
        field_tags_str = []
        for f in union_def.fields:
            field_tags_str.append(f"{indent}{f.name} = {f.id},")
        field_tags_str.append(f"{indent}default = std.math.maxInt(i16),")
        field_tags_str.append(f"{indent}_,")
        write_cases = []
        for f in union_def.fields:
            if isinstance(f.type, ListType):
                # lists: not supported for union writing
                write_cases.append(f"            .{f.name} => |payload| {{ ")
                write_cases.append(f"                // list writing handled elsewhere; unreachable if we don't generate such unions")
                write_cases.append(f"            }}, ")
            else:
                ttype, is_enum = classify_type(f.type, self.defsmap)
                write_cases.append(f"            .{f.name} => |payload| {{ ")
                write_cases.append(f"                try writer.write(.{{ .FieldBegin = .{{ .tp = TType.{ttype}, .fid = @intFromEnum(FieldTag.{f.name}) }} }});")
                write_cases.extend(emit_write_payload("                ", ttype, "payload", is_enum))
                write_cases.append(f"                try writer.write(.FieldEnd);")
                write_cases.append(f"            }}, ")
        write_cases_joined = "\n".join(write_cases)
        fields_joined = "\n".join(fields_str)
        field_tags_joined = "\n".join(field_tags_str)

        # read helper for union
        union_field_cases: list[str] = []
        for f in union_def.fields:
            union_field_cases.extend(_emit_read_union_field_case(union_def.name, f, self.defsmap))
        read_union_field_helper = f"""    fn read{union_def.name}Field(r: *Reader, alloc: std.mem.Allocator, field: FieldMeta) (CompactProtocolError || ThriftError)!?{union_def.name} {{
        use_arg(alloc);
        sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {{
{'\n'.join(union_field_cases)}
            .default => try r.skip(field.tp),
            else => continue :sw .default,
        }}
        return null;
    }}"""

        # read method for union
        read_method = f"""    pub fn read(r: *Reader, alloc: std.mem.Allocator) (CompactProtocolError || ThriftError)!{union_def.name} {{
        var obj: ?{union_def.name} = null;
        try r.readStructBegin();
        while (try readFieldOrStop(r)) |field| {{
            if (try read{union_def.name}Field(r, alloc, field)) |new_obj| {{
                obj = new_obj;
            }}
            try r.readFieldEnd();
        }}
        try r.readStructEnd();
        return obj orelse ThriftError.CantParseUnion;
    }}"""

        return f'''pub const {union_def.name} = union(enum) {{
{fields_joined}

    const FieldTag = enum(i16) {{
{field_tags_joined}
    }};

    pub fn write(self: *const {union_def.name}, writer: *Writer) WriterError!void {{
        try writer.write(.StructBegin);
        switch (self.*) {{
{write_cases_joined}
        }}
        try writer.write(.FieldStop);
        try writer.write(.StructEnd);
    }}

{read_union_field_helper}

{read_method}

{self._gen_union_deinit(union_def)}
}};'''

    def _sample_value(self, t: Type, structs: dict[str, StructDef], unions: dict[str, UnionDef], enums: dict[str, EnumDef]) -> str:
        if isinstance(t, NamedType):
            n = t.name
            if n in SAMPLE_LITERALS:
                return SAMPLE_LITERALS[n]
            df = self.defsmap[n]
            if isinstance(df, StructDef):
                return self._gen_struct_value(structs[n], structs, unions, enums)
            if isinstance(df, EnumDef):
                first_member = enums[n].members[0].name
                return f".{first_member}"
            if isinstance(df, UnionDef):
                return self._gen_union_value(unions[n], structs, unions, enums)
        if isinstance(t, ListType):
            elem_t = _get_list_elem_type(t)
            elem_zig = self.type_system.get_zig_type(elem_t, True)
            return f"std.ArrayList({elem_zig}).empty"
        x = t
        return ""

    def _gen_struct_value(self, definition: StructDef, structs, unions, enums) -> str:
        construction_args = []
        for f in definition.fields:
            val = self._sample_value(f.type, structs, unions, enums)
            if val == "":
                t = f.type.name
                if t in SAMPLE_LITERALS:
                    val = SAMPLE_LITERALS[t]
                else:
                    df = self.defsmap[t]
                    if isinstance(df, StructDef):
                        val = self._gen_struct_value(structs[t], structs, unions, enums)
                    elif isinstance(df, EnumDef):
                        first_member = enums[t].members[0].name
                        val = f".{first_member}"
                    elif isinstance(df, UnionDef):
                        val = self._gen_union_value(unions[t], structs, unions, enums)
            arg = f".{f.name} = {val}"
            if not f.required:
                arg = f".{f.name} = null"
            construction_args.append(arg)
        return f"{definition.name}{{ {', '.join(construction_args)} }}"

    def _gen_union_value(self, definition: UnionDef, structs, unions, enums) -> str:
        f = definition.fields[0]
        val = self._sample_value(f.type, structs, unions, enums)
        if val == "":
            t = f.type.name
            if t in SAMPLE_LITERALS:
                val = SAMPLE_LITERALS[t]
            else:
                df = self.defsmap[t]
                if isinstance(df, StructDef):
                    val = self._gen_struct_value(structs[t], structs, unions, enums)
                elif isinstance(df, EnumDef):
                    first_member = enums[t].members[0].name
                    val = f".{first_member}"
                elif isinstance(df, UnionDef):
                    val = self._gen_union_value(unions[t], structs, unions, enums)
        return f".{{ .{f.name} = {val} }}"

    def _gen_fill_list_fields(self, var_name: str, definition: StructDef, structs: dict[str, StructDef], unions: dict[str, UnionDef], enums: dict[str, EnumDef]) -> list[str]:
        lines: list[str] = []
        for f in definition.fields:
            if isinstance(f.type, ListType):
                elem_t = _get_list_elem_type(f.type)
                elem_zig = self.type_system.get_zig_type(elem_t, True)
                sample = self._sample_value(elem_t, structs, unions, enums)
                # ensure non-empty to exercise reader; add two items
                if f.required:
                    lines.append(f"    try {var_name}.{f.name}.ensureTotalCapacity(alloc, 2);")
                    lines.append(f"    try {var_name}.{f.name}.append(alloc, {sample});")
                    lines.append(f"    try {var_name}.{f.name}.append(alloc, {sample});")
                    lines.append(f"    defer {var_name}.{f.name}.deinit(alloc);")
                else:
                    lines.append(f"    {var_name}.{f.name} = std.ArrayList({elem_zig}).empty;")
                    lines.append(f"    try {var_name}.{f.name}.?.ensureTotalCapacity(alloc, 2);")
                    lines.append(f"    try {var_name}.{f.name}.?.append(alloc, {sample});")
                    lines.append(f"    try {var_name}.{f.name}.?.append(alloc, {sample});")
                    lines.append(f"    defer {var_name}.{f.name}.?.deinit(alloc);")
        return lines

    def _generate_test_block(self) -> str:
        test_calls = []
        structs: dict[str, StructDef] = {}
        unions: dict[str, UnionDef] = {}
        enums: dict[str, EnumDef] = {}
        for definition in self.idl.definitions:
            if isinstance(definition, StructDef):
                structs[definition.name] = definition
            elif isinstance(definition, UnionDef):
                unions[definition.name] = definition
            elif isinstance(definition, EnumDef):
                enums[definition.name] = definition
        struct_counter = 0
        union_counter = 0
        # Write in definition order
        for definition in self.idl.definitions:
            if isinstance(definition, StructDef):
                var_name = f"struct{struct_counter}"
                test_calls.append(f"    var {var_name}: {definition.name} = {self._gen_struct_value(definition, structs, unions, enums)};")
                # populate list fields (and defer list backing to avoid leaks)
                test_calls.extend(self._gen_fill_list_fields(var_name, definition, structs, unions, enums))
                test_calls.append(f"    try {var_name}.write(&w);")
                struct_counter += 1
            elif isinstance(definition, UnionDef):
                var_name = f"union{union_counter}"
                test_calls.append(f"    var {var_name}: {definition.name} = {self._gen_union_value(unions[definition.name], structs, unions, enums)};")
                test_calls.append(f"    try {var_name}.write(&w);")
                union_counter += 1

        test_calls.extend([
            "    const written: []const u8 = w.writer.buffered();",
            "    var r: Reader = undefined;",
            "    r.init(.fixed(written));"
        ])

        # Read back in the same order (now including unions)
        struct_counter = 0
        union_counter = 0
        for definition in self.idl.definitions:
            if isinstance(definition, StructDef):
                name = f"struct{struct_counter}"
                test_calls.extend([
                    f"    var {name}_read = try {definition.name}.read(&r, alloc);",
                    f"    defer {name}_read.deinit(alloc);",
                    f"    try std.testing.expectEqualDeep({name}, {name}_read);"
                ])
                struct_counter += 1
            elif isinstance(definition, UnionDef):
                name = f"union{union_counter}"
                test_calls.extend([
                    f"    var {name}_read = try {definition.name}.read(&r, alloc);",
                    f"    defer {name}_read.deinit(alloc);",
                    f"    try std.testing.expectEqualDeep({name}, {name}_read);"
                ])
                union_counter += 1
        
        test_calls_joined = "\n".join(test_calls)
        return f'''

test "generated code compiles, writes, and reads structs & unions" {{ 
    var buf: [1024]u8 = undefined;
    const alloc = std.testing.allocator;
    // var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // const alloc = arena.allocator();
    // defer arena.deinit();

    var w: Writer = undefined;
    w.init(.fixed(&buf));

{test_calls_joined}
}} 
'''


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python code_gen.py <thrift_file>")
        sys.exit(1)

    with open(sys.argv[1], "r") as f:
        p_idl = parse_idl(f.read())

    generator = CodeGenerator(p_idl)
    generated_code = generator.generate()
    print(generated_code)