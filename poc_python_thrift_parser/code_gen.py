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
    "double": "3.14",
}


def _get_list_elem_type(list_t: ListType) -> Type:
    for attr in ("elem", "elem_type", "element", "value_type", "ty"):
        if hasattr(list_t, attr):
            return getattr(list_t, attr)
    raise NotImplementedError(f"Unsupported ListType representation: {list_t!r}")


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
const TCompactProtocol = @import("TCompactProtocol.zig");
const Writer = TCompactProtocol.Writer;
const Reader = TCompactProtocol.Reader;
const TType = TCompactProtocol.TType;
const FieldMeta = TCompactProtocol.FieldMeta;
const WriterError = Writer.WriterError;
const CompactProtocolError = Reader.CompactProtocolError || error{NotImplemented};
const ThriftError = Reader.ThriftError;
const Meta = @import("Meta.zig");

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
            item = f"    {f.name}: {zig_type}"
            if f.default is not None:
                item += f" = {f.default}"
            fields_str.append(f"{item},")
        indent = '        '
        field_tags_str = []
        for f in struct_def.fields:
            field_tags_str.append(f"{indent}{f.name} = {f.id},")

        fields_joined = "\n".join(fields_str)
        field_tags_joined = "\n".join(field_tags_str)

        req_checks = []
        for f in struct_def.fields:
            if f.required:
                req_checks.append(f"        if (!is.{f.name}) return ThriftError.RequiredFieldMissing;")

        init_parts = []
        for f in struct_def.fields:
            init_val = "undefined" if f.required else "null"
            init_parts.append(f".{f.name} = {init_val}")

        return f'''pub const {struct_def.name} = struct {{
{fields_joined}

    pub const FieldTag = enum(i16) {{
{field_tags_joined}
    }};

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
        fields_joined = "\n".join(fields_str)
        field_tags_joined = "\n".join(field_tags_str)

        return f'''pub const {union_def.name} = union(enum) {{
{fields_joined}

    pub const FieldTag = enum(i16) {{
{field_tags_joined}
    }};

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
                    lines.append(f"    if (@sizeOf({elem_zig}) > 0) {{")
                    lines.append(f"        try {var_name}.{f.name}.ensureTotalCapacity(alloc, 2);")
                    lines.append(f"        try {var_name}.{f.name}.append(alloc, {sample});")
                    lines.append(f"        try {var_name}.{f.name}.append(alloc, {sample});")
                    lines.append(f"    }}")
                    lines.append(f"    defer {var_name}.{f.name}.deinit(alloc);")

                else:
                    lines.append(f"    {var_name}.{f.name} = std.ArrayList({elem_zig}).empty;")
                    lines.append(f"    if (@sizeOf({elem_zig}) > 0) {{")
                    lines.append(f"        try {var_name}.{f.name}.?.ensureTotalCapacity(alloc, 2);")
                    lines.append(f"        try {var_name}.{f.name}.?.append(alloc, {sample});")
                    lines.append(f"        try {var_name}.{f.name}.?.append(alloc, {sample});")
                    lines.append(f"    }}")
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
                lst_fields = self._gen_fill_list_fields(var_name, definition, structs, unions, enums)
                qual = "const" if not lst_fields else "var"
                test_calls.append(f"    {qual} {var_name}: {definition.name} = {self._gen_struct_value(definition, structs, unions, enums)};")
                # populate list fields (and defer list backing to avoid leaks)
                test_calls.extend(lst_fields)
                test_calls.append(f"    try Meta.structWrite(@TypeOf({var_name}), {var_name}, &w);")
                struct_counter += 1
            elif isinstance(definition, UnionDef):
                var_name = f"union{union_counter}"
                test_calls.append(f"    const {var_name}: {definition.name} = {self._gen_union_value(unions[definition.name], structs, unions, enums)};")
                test_calls.append(f"    try Meta.unionWrite(@TypeOf({var_name}), {var_name}, &w);")
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
                    f"    const {name}_read = try Meta.structRead({definition.name}, alloc, &r);",
                    f"    defer Meta.deinit({definition.name}, {name}_read, alloc);",
                    f"    try std.testing.expectEqualDeep({name}, {name}_read);"
                ])
                struct_counter += 1
            elif isinstance(definition, UnionDef):
                name = f"union{union_counter}"
                test_calls.extend([
                    f"    const {name}_read = try Meta.unionRead({definition.name}, alloc, &r);",
                    f"    defer Meta.deinit({definition.name}, {name}_read, alloc);",
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