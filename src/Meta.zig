const std = @import("std");
const TCompactProtocol = @import("TCompactProtocol.zig");
const Reader = TCompactProtocol.Reader;
const ThriftError = Reader.ThriftError;
const FieldMeta = TCompactProtocol.FieldMeta;
const CompactProtocolError = Reader.CompactProtocolError || error{NotImplemented};

fn isArrayList(T: type) bool {
    return @hasField(T, "items") and
        @hasDecl(T, "empty") and
        @hasDecl(T, "deinit") and
        @hasDecl(T, "append") and
        @typeInfo(std.meta.fieldInfo(T, .items).type) == .pointer;
}

// from zig type to logical TType. .BYTE vs .I08 is confusing - both are stored as a
// single byte, and CompactProtocol does not retain info about whether the byte is a u8
// or i8. See 'TCompactProtocol.CType'.
fn type2ttype(comptime T: type) TCompactProtocol.TType {
    return switch (@typeInfo(T)) {
        .int => {
            if (T == i8) return .I08;
            if (T == u8) return .BYTE;
            if (T == i16) return .I16;
            if (T == i32) return .I32;
            if (T == i64) {
                return .I64;
            } else @compileError("Original type incompatible with Thrift: " ++ @typeName(T));
        },
        .bool => .BOOL,
        .float => .DOUBLE, // todo check it's f64?
        .pointer => {
            if (T == []const u8) {
                return .STRING;
            } else @compileError("Original type incompatible with Thrift: " ++ @typeName(T));
        },
        .@"struct", .@"union" => {
            if (isArrayList(T)) return .LIST;
            return .STRUCT;
        },
        .optional => {
            return type2ttype(@typeInfo(T).optional.child);
        },
        .@"enum" => .I32,
        else => {
            @compileError("Original type incompatible with Thrift: " ++ @typeName(T));
        },
    };
}

fn valueWrite(comptime T: type, value: T, w: *TCompactProtocol.Writer) !void {
    const TT = comptime type2ttype(T);
    switch (TT) {
        .BOOL => try w.write(.{ .Bool = value }),
        .BYTE => try w.write(.{ .Byte = value }),
        .I08 => try w.write(.{ .I08 = value }),
        .I16 => try w.write(.{ .I16 = value }),
        .I32 => {
            // thrift enums are i32; this handles them too.
            const v: i32 = if (T == i32) value else @intFromEnum(value);
            try w.write(.{ .I32 = v });
        },
        .I64 => try w.write(.{ .I64 = value }),
        .STRING => try w.write(.{ .Binary = value }),
        .STRUCT => {
            if (comptime @typeInfo(T) == .@"struct") {
                try structWrite(T, value, w);
            } else try unionWrite(T, value, w);
        },
        .LIST => {
            const ITT = comptime type2ttype(@TypeOf(value.items[0]));
            try w.write(.{ .ListBegin = .{ .elem_type = ITT, .size = @intCast(value.items.len) } });
            for (value.items) |item| {
                try valueWrite(@TypeOf(item), item, w);
            }
            try w.write(.ListEnd);
        },

        .DOUBLE => try w.write(.{ .Double = value }),

        else => @compileError("Field type not supported by our Thrift subset. Original: " ++ @typeName(T) ++ ", thrift: " ++ @tagName(TT)),
    }
}

fn fieldWrite(comptime T: type, value: T, fid: i16, w: *TCompactProtocol.Writer) !void {
    std.debug.print("Writing field with id {} and value {any}\n", .{ fid, value });
    const TT = comptime type2ttype(T);
    try w.write(.{ .FieldBegin = .{ .tp = TT, .fid = fid } });
    try valueWrite(T, value, w);
    try w.write(.FieldEnd);
}

// Checks that the field tag struct is an enum(i16) with values for each field of T.
fn validateFieldTag(T: type, FieldTag: type) void {
    const tagType = @typeInfo(FieldTag).@"enum".tag_type;
    if (tagType != i16) {
        @compileError("Tag type for field ids should be i16; got " ++ @typeName(tagType));
    }

    inline for (std.meta.fields(T)) |fl| {
        if (!@hasField(FieldTag, fl.name)) {
            @compileError("Can't serialize " ++ @typeName(T) ++ "." ++ fl.name ++ ": field id for '" ++ fl.name ++ "' is missing from " ++ @typeName(FieldTag));
        }
    }
}

pub fn unionWrite(T: type, value: T, w: *TCompactProtocol.Writer) !void {
    validateFieldTag(T, T.FieldTag);
    if (!comptime @typeInfo(T) == .@"union") @compileError("Must be union, got " ++ @typeName(T));
    try w.write(.StructBegin);
    switch (value) {
        inline else => |inner, tag| {
            const fid: i16 = @intFromEnum(@field(T.FieldTag, @tagName(tag)));
            try fieldWrite(@TypeOf(inner), inner, fid, w);
        },
    }
    try w.write(.FieldStop);
    try w.write(.StructEnd);
}

pub fn structWrite(T: type, value: T, w: *TCompactProtocol.Writer) !void {
    validateFieldTag(T, T.FieldTag);
    if (!comptime @typeInfo(T) == .@"struct") @compileError("Must be struct, got " ++ @typeName(T));
    try w.write(.StructBegin);

    inline for (@typeInfo(T).@"struct".fields) |fl| {
        const fid: i16 = @intFromEnum(@field(T.FieldTag, fl.name));
        const field_value = @field(value, fl.name);
        if (@typeInfo(fl.type) == .optional) {
            if (field_value) |inner| {
                try fieldWrite(@TypeOf(inner), inner, fid, w);
            }
        } else {
            try fieldWrite(@TypeOf(field_value), field_value, fid, w);
        }
    }
    try w.write(.FieldStop);
    try w.write(.StructEnd);
}

// A struct with bool fields with the same name for each field of T.
fn IsSet(T: type) type {
    return std.enums.EnumFieldStruct(std.meta.FieldEnum(T), bool, false);
}

test "isset" {
    const A = struct { x: i16, y: void, z: ?std.ArrayList(std.builtin.Type) };
    const i: IsSet(A) = .{};
    try std.testing.expectEqualDeep(IsSet(A){ .x = false, .y = false, .z = false }, i);
}

// T: a struct type. Creates an undefined T setting fields with defaults to their default values.
// TODO: we don't support default values yet since they are tricky to deinit. Remove? Keep?
fn initDefaults(T: type) T {
    var obj: T = undefined;
    const fields = @typeInfo(T).@"struct".fields;
    inline for (fields) |field| {
        if (field.default_value_ptr) |dvp| {
            @field(obj, field.name) = @as(*const field.type, @alignCast(@ptrCast(dvp))).*;
        } else if (@typeInfo(field.type) == .optional) {
            @field(obj, field.name) = null;
        }
    }
    return obj;
}

test "initDefaults" {
    const A = struct { x: i16 = 1234, y: i32 };
    const a: A = initDefaults(A);
    try std.testing.expectEqual(a.x, 1234);
}

fn readFieldOrStop(r: *TCompactProtocol.Reader) CompactProtocolError!?FieldMeta {
    const field = try r.readFieldBegin();
    if (field.tp == .STOP) return null;
    return field;
}

// TODO unions?
fn StructReadingContext(T: type) type {
    return struct {
        obj: T,
        isset: IsSet(T),
        r: *Reader,
        alloc: std.mem.Allocator,
        //field: FieldMeta,
        // Don't think this works, the whole struct would need to be comptime:
        //tField: ?std.builtin.Type.StructField = null
    };
}

fn ttypesCompatible(read: TCompactProtocol.TType, logical: TCompactProtocol.TType) bool {
    return read == logical or (read == .BYTE and (logical == .I08 or logical == .BYTE));
}

fn stripOptional(T: type) type {
    const ti = @typeInfo(T);
    return if (ti == .optional) ti.optional.child else T;
}

fn readValue(T: type, r: *Reader, alloc: std.mem.Allocator) (CompactProtocolError || ThriftError)!T {
    const TT = comptime type2ttype(T);
    switch (TT) {
        .BOOL => return try r.readBool(),
        .BYTE, .I08 => {
            return @bitCast(try r.readByte());
        },
        .I16 => return try r.readI16(),
        .I32 => {
            const i = try r.readI32();
            if (comptime isEnumOrOptEnum(T)) {
                return @enumFromInt(i);
            } else return i;
        },
        .I64 => return try r.readI64(),
        .STRING => return try r.readBinary(alloc),
        .STRUCT => {
            const To = stripOptional(T);
            if (comptime @typeInfo(To) == .@"struct") {
                return try structRead(To, alloc, r);
            } else if (comptime @typeInfo(To) == .@"union") {
                return try unionRead(To, alloc, r);
            } else @compileError("wtf?");
        },

        .LIST => {
            const list_meta = try r.readListBegin();
            var res = stripOptional(T).empty;
            try res.ensureTotalCapacityPrecise(alloc, list_meta.size);
            errdefer deinit(T, res, alloc);
            for (0..list_meta.size) |_| {
                try res.append(alloc, try readValue(@TypeOf(res.items[0]), r, alloc));
            }
            try r.readListEnd();
            return res;
        },

        else => unreachable,
    }
}

fn isEmptyEnum(FieldTag: type) bool {
    const fields = comptime @typeInfo(FieldTag).@"enum".fields;
    return fields.len == 0;
}

// fn fieldTagFromFid(FieldTag: type, fid: i16) ?FieldTag {
//     if (comptime isEmptyEnum(FieldTag)) return null;
//     return std.enums.fromInt(FieldTag, fid);
// }

// Checks the tag and looks up the field.
fn readField(T: type, ctx: *StructReadingContext(T), field_meta: FieldMeta) (CompactProtocolError || ThriftError)!void {
    const r = ctx.r;
    if (comptime isEmptyEnum(T.FieldTag)) {
        try r.skip(field_meta.tp);
        return;
    }
    const tagOpt = std.enums.fromInt(T.FieldTag, field_meta.fid);
    if (tagOpt == null) {
        try r.skip(field_meta.tp);
        return;
    }

    switch (tagOpt.?) {
        inline else => |tag| {
            @setEvalBranchQuota(100000);
            const field = comptime std.meta.fieldInfo(T, std.meta.stringToEnum(std.meta.FieldEnum(T), @tagName(tag)).?);
            const TT = comptime type2ttype(field.type);
            if (!ttypesCompatible(field_meta.tp, TT)) {
                try r.skip(field_meta.tp);
                return;
            }
            @field(ctx.obj, field.name) = try readValue(field.type, ctx.r, ctx.alloc);
            @field(ctx.isset, field.name) = true;
        },
    }
}

fn isEnumOrOptEnum(T: type) bool {
    return switch (@typeInfo(T)) {
        .optional => |opt| isEnumOrOptEnum(opt.child),
        .@"enum" => true,
        else => false,
    };
}

fn validate(T: type, isset: IsSet(T)) ThriftError!void {
    inline for (std.meta.fields(T)) |field| {
        if (field.default_value_ptr == null and @typeInfo(field.type) != .optional) {
            const present: bool = @field(isset, field.name);
            if (!present) {
                std.debug.print("Field name '{s}: {s}' missing \n", .{ field.name, @typeName(field.type) });
                return ThriftError.RequiredFieldMissing;
            }
        }
    }
}

// DOES NOT SUPPORT fields with default value yet - would try to free static strings! Also - O(N^2) code for N struct definitions.
pub fn deinit(T: type, obj: T, alloc: std.mem.Allocator) void {
    switch (@typeInfo(T)) {
        .pointer => {
            if (T != []const u8) unreachable;
            alloc.free(obj);
        },
        .optional => |opt| {
            if (obj != null) deinit(opt.child, obj.?, alloc);
        },
        .@"struct" => |structInfo| {
            if (comptime isArrayList(T)) {
                for (obj.items) |item| deinit(@TypeOf(item), item, alloc);
                // constCast: 'obj: ArrayList(Something)' is a function parameter and therefore 'const'.
                // The const cast is safe and correct here, since it
                // deallocates heap-allocated obj.slice[0..obj.capacity]
                @constCast(&obj).deinit(alloc);
                return;
            }
            inline for (structInfo.fields) |fld| {
                deinit(fld.type, @field(obj, fld.name), alloc);
            }
        },
        .@"union" => {
            switch (obj) {
                inline else => |value| {
                    deinit(@TypeOf(value), value, alloc);
                },
            }
        },
        else => {},
    }
}

fn deinitWithIsset(T: type, obj: T, isset: IsSet(T), alloc: std.mem.Allocator) void {
    inline for (std.meta.fields(T)) |field| {
        if (@field(isset, field.name)) {
            deinit(field.type, @field(obj, field.name), alloc);
        }
    }
}

pub fn structRead(T: type, alloc: std.mem.Allocator, r: *Reader) (CompactProtocolError || ThriftError)!T {
    // TODO: default values - copy default strings or others to allocated memory.
    var ctx: StructReadingContext(T) = .{ .obj = initDefaults(T), .isset = .{}, .r = r, .alloc = alloc };

    errdefer deinitWithIsset(T, ctx.obj, ctx.isset, alloc);

    try r.readStructBegin();
    while (try readFieldOrStop(r)) |field_meta| {
        try readField(T, &ctx, field_meta);
        try r.readFieldEnd();
    }

    try r.readStructEnd();
    try validate(T, ctx.isset);
    return ctx.obj;
}

fn readUnionField(T: type, alloc: std.mem.Allocator, r: *Reader, field_meta: FieldMeta) (CompactProtocolError || ThriftError)!?T {
    const tagOpt = std.enums.fromInt(T.FieldTag, field_meta.fid);
    if (tagOpt == null) {
        try r.skip(field_meta.tp);
        return null;
    }

    switch (tagOpt.?) {
        inline else => |tag| {
            @setEvalBranchQuota(100000);
            const field: std.builtin.Type.UnionField = comptime std.meta.fieldInfo(T, std.meta.stringToEnum(std.meta.FieldEnum(T), @tagName(tag)).?);
            const TT = comptime type2ttype(field.type);
            if (!ttypesCompatible(field_meta.tp, TT)) {
                try r.skip(field_meta.tp);
                return null;
            }
            return @unionInit(T, field.name, try readValue(field.type, r, alloc));
        },
    }
    return null;
}

// // FROM std.testing, want special case for arraylist capacity:
// fn print(comptime fmt: []const u8, args: anytype) void {
//     if (@inComptime()) {
//         @compileError(std.fmt.comptimePrint(fmt, args));
//     } else {
//         std.debug.print(fmt, args);
//     }
// }

// pub inline fn expectEqualDeep(expected: anytype, actual: anytype) error{TestExpectedEqual}!void {
//     const T = @TypeOf(expected, actual);
//     return expectEqualDeepInner(T, expected, actual);
// }

// fn expectEqualDeepInner(comptime T: type, expected: T, actual: T) error{TestExpectedEqual}!void {
//     switch (@typeInfo(@TypeOf(actual))) {
//         .noreturn,
//         .@"opaque",
//         .frame,
//         .@"anyframe",
//         => @compileError("value of type " ++ @typeName(@TypeOf(actual)) ++ " encountered"),

//         .undefined,
//         .null,
//         .void,
//         => return,

//         .type => {
//             if (actual != expected) {
//                 print("expected type {s}, found type {s}\n", .{ @typeName(expected), @typeName(actual) });
//                 return error.TestExpectedEqual;
//             }
//         },

//         .bool,
//         .int,
//         .float,
//         .comptime_float,
//         .comptime_int,
//         .enum_literal,
//         .@"enum",
//         .@"fn",
//         .error_set,
//         => {
//             if (actual != expected) {
//                 print("expected {}, found {}\n", .{ expected, actual });
//                 return error.TestExpectedEqual;
//             }
//         },

//         .pointer => |pointer| {
//             switch (pointer.size) {
//                 // We have no idea what is behind those pointers, so the best we can do is `==` check.
//                 .c, .many => {
//                     if (actual != expected) {
//                         print("expected {*}, found {*}\n", .{ expected, actual });
//                         return error.TestExpectedEqual;
//                     }
//                 },
//                 .one => {
//                     // Length of those pointers are runtime value, so the best we can do is `==` check.
//                     switch (@typeInfo(pointer.child)) {
//                         .@"fn", .@"opaque" => {
//                             if (actual != expected) {
//                                 print("expected {*}, found {*}\n", .{ expected, actual });
//                                 return error.TestExpectedEqual;
//                             }
//                         },
//                         else => try expectEqualDeep(expected.*, actual.*),
//                     }
//                 },
//                 .slice => {
//                     if (expected.len != actual.len) {
//                         print("Slice len not the same, expected {d}, found {d}\n", .{ expected.len, actual.len });
//                         return error.TestExpectedEqual;
//                     }
//                     var i: usize = 0;
//                     while (i < expected.len) : (i += 1) {
//                         expectEqualDeep(expected[i], actual[i]) catch |e| {
//                             print("index {d} incorrect. expected {any}, found {any}\n", .{
//                                 i, expected[i], actual[i],
//                             });
//                             return e;
//                         };
//                     }
//                 },
//             }
//         },

//         .array => |_| {
//             if (expected.len != actual.len) {
//                 print("Array len not the same, expected {d}, found {d}\n", .{ expected.len, actual.len });
//                 return error.TestExpectedEqual;
//             }
//             var i: usize = 0;
//             while (i < expected.len) : (i += 1) {
//                 expectEqualDeep(expected[i], actual[i]) catch |e| {
//                     print("index {d} incorrect. expected {any}, found {any}\n", .{
//                         i, expected[i], actual[i],
//                     });
//                     return e;
//                 };
//             }
//         },

//         .vector => |info| {
//             if (info.len != @typeInfo(@TypeOf(actual)).vector.len) {
//                 print("Vector len not the same, expected {d}, found {d}\n", .{ info.len, @typeInfo(@TypeOf(actual)).vector.len });
//                 return error.TestExpectedEqual;
//             }
//             var i: usize = 0;
//             while (i < info.len) : (i += 1) {
//                 expectEqualDeep(expected[i], actual[i]) catch |e| {
//                     print("index {d} incorrect. expected {any}, found {any}\n", .{
//                         i, expected[i], actual[i],
//                     });
//                     return e;
//                 };
//             }
//         },

//         .@"struct" => |structType| {
//             inline for (structType.fields) |field| {
//                 expectEqualDeep(@field(expected, field.name), @field(actual, field.name)) catch |e| {
//                     print("Field {s} incorrect. expected {any}, found {any}\n", .{ field.name, @field(expected, field.name), @field(actual, field.name) });
//                     return e;
//                 };
//             }
//         },

//         .@"union" => |union_info| {
//             if (union_info.tag_type == null) {
//                 @compileError("Unable to compare untagged union values for type " ++ @typeName(@TypeOf(actual)));
//             }

//             const Tag = std.meta.Tag(@TypeOf(expected));

//             const expectedTag = @as(Tag, expected);
//             const actualTag = @as(Tag, actual);

//             try expectEqual(expectedTag, actualTag);

//             // we only reach this switch if the tags are equal
//             switch (expected) {
//                 inline else => |val, tag| {
//                     try expectEqualDeep(val, @field(actual, @tagName(tag)));
//                 },
//             }
//         },

//         .optional => {
//             if (expected) |expected_payload| {
//                 if (actual) |actual_payload| {
//                     try expectEqualDeep(expected_payload, actual_payload);
//                 } else {
//                     print("expected {any}, found null\n", .{expected_payload});
//                     return error.TestExpectedEqual;
//                 }
//             } else {
//                 if (actual) |actual_payload| {
//                     print("expected null, found {any}\n", .{actual_payload});
//                     return error.TestExpectedEqual;
//                 }
//             }
//         },

//         .error_union => {
//             if (expected) |expected_payload| {
//                 if (actual) |actual_payload| {
//                     try expectEqualDeep(expected_payload, actual_payload);
//                 } else |actual_err| {
//                     print("expected {any}, found {any}\n", .{ expected_payload, actual_err });
//                     return error.TestExpectedEqual;
//                 }
//             } else |expected_err| {
//                 if (actual) |actual_payload| {
//                     print("expected {any}, found {any}\n", .{ expected_err, actual_payload });
//                     return error.TestExpectedEqual;
//                 } else |actual_err| {
//                     try expectEqualDeep(expected_err, actual_err);
//                 }
//             }
//         },
//     }
// }

pub fn unionRead(T: type, alloc: std.mem.Allocator, r: *Reader) (CompactProtocolError || ThriftError)!T {
    var obj: ?T = null;
    try r.readStructBegin();
    while (try readFieldOrStop(r)) |field| {
        if (try readUnionField(T, alloc, r, field)) |new_obj| {
            obj = new_obj;
        }
        try r.readFieldEnd();
    }
    try r.readStructEnd();
    return obj orelse ThriftError.CantParseUnion;
}

test "union" {
    const U = union(enum) { x: i16, y: i8 };
    const TagU = @typeInfo(U).@"union".tag_type.?;
    const tag: TagU = .x;
    const u: U = @unionInit(U, @tagName(tag), 10);
    std.debug.print("u: {}\n", .{u});
}

test "read union" {
    const U = union(enum) {
        x: i16,
        y: i8,
        z: []const u8,
        pub const FieldTag = enum(i16) { x = 1, y = 2, z = 3 };
    };

    var buf: [100]u8 = undefined;
    const w: std.Io.Writer = .fixed(&buf);

    var wr: TCompactProtocol.Writer = undefined;
    wr.init(w);

    const u = U{ .x = 10 };

    try unionWrite(U, u, &wr);
    std.debug.print("bytes written: {}, byte contents: {x}\n", .{ wr.writer.buffered().len, wr.writer.buffered() });

    const io_reader: std.Io.Reader = .fixed(wr.writer.buffered());
    var reader: Reader = undefined;
    reader.init(io_reader);

    _ = try unionRead(U, std.testing.allocator, &reader);
}

test {
    const D = enum(i32) { LEFT = 0, RIGHT = 1 };
    const C = union(enum) {
        x: i32,
        y: i64,
        z: []const u8,
        // pub fn write(self: @This(), w: *TCompactProtocol.Writer) !void {
        //     try unionWrite(@This(), enum(i16) { x = 1, y = 2 }, self, w);
        // }
        pub const FieldTag = enum(i16) { x = 1, y = 2, z = 3 };
    };
    //_ = C;

    const B = struct {
        x: i32,
        pub const FieldTag = enum(i16) { x = 1 };
        // pub fn write(self: @This(), w: *TCompactProtocol.Writer) !void {
        //     try structWrite(@This(), , self, w);
        // }
    };

    const A = struct {
        x: i32,
        y: bool,
        z: u8,
        w: i8,
        v: i16,
        a: i64,
        aa: ?i32,
        aaa: ?i32,
        b: B,
        bb: ?B,
        c: C,
        s: []const u8,
        ss: ?[]const u8,
        sss: []const u8, // = "hello, world!",
        is: std.ArrayList(i32),
        bs: ?std.ArrayList(B),
        d: D,
        ds: std.ArrayList(D),
        //fn dummy() void {}
        cc: ?C,

        pub const FieldTag = enum(i16) {
            x = 1,
            y = 2,
            z = 3,
            w = 4,
            v = 5,
            a = 6,
            aa = 7,
            aaa = 8,
            b = 9,
            bb = 10,
            c = 11,
            s = 12,
            is = 13,
            bs = 14,
            d = 15,
            ds = 16,
            ss = 17,
            sss = 18,
            cc = 19,
            //fn dummy() void {}
        };
    };

    var buf: [100]u8 = undefined;
    const w: std.Io.Writer = .fixed(&buf);

    var bs: [2]B = .{ .{ .x = 123 }, .{ .x = 456 } };
    var ds: [3]D = .{ .LEFT, .LEFT, .RIGHT };
    var bsa = std.ArrayList(B).initBuffer(&bs);
    var dsa = std.ArrayList(D).initBuffer(&ds);
    bsa.items.len = 2;
    dsa.items.len = 3;

    const a = A{
        .x = 10,
        .y = true,
        .z = 0,
        .w = -1,
        .v = 100,
        .a = 1232725,
        .aa = null,
        .aaa = 172,
        .b = .{ .x = 100 },
        .bb = null,
        .c = .{ .x = 0 },
        .cc = .{ .z = "hello, world" },
        //.cc = .{ .x = 0 },
        .s = "hellaoeu atoehuas o",
        .ss = null,
        .sss = "",

        .is = std.ArrayList(i32).empty,
        .bs = bsa,
        .d = .RIGHT,
        .ds = dsa,
    };
    var wr: TCompactProtocol.Writer = undefined;
    wr.init(w);

    try structWrite(A, a, &wr);
    std.debug.print("bytes written: {}, byte contents: {x}\n", .{ wr.writer.buffered().len, wr.writer.buffered() });

    const io_reader: std.Io.Reader = .fixed(wr.writer.buffered());
    var reader: Reader = undefined;
    reader.init(io_reader);
    const read_a = try structRead(A, std.testing.allocator, &reader);
    defer deinit(A, read_a, std.testing.allocator);
    std.debug.print("read a: {any}\n", .{read_a});

    try std.testing.expectEqualDeep(a, read_a);
}
