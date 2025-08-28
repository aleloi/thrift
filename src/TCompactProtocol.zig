const std = @import("std");

const check_states: bool = std.debug.runtime_safety;

const State = enum {
    CLEAR,
    FIELD,
    VALUE,
    CONTAINER,
    BOOL,
    DUMMY_STATE, // not a real state

    pub fn transition(self: *State, comptime mask: States, new_state: State) error{InvalidState}!void {
        if (check_states) {
            try self.check(mask);
            self.* = new_state;
        }
    }
    pub fn check(self: *State, comptime mask: States) error{InvalidState}!void {
        if (check_states) {
            if (!mask.contains(self.*)) {
                return error.InvalidState;
            }
        }
    }
};
const States = std.EnumSet(State);

const Stacks = struct {
    _last_fid_buf: [64]i16 = undefined,
    last_fids: std.ArrayListUnmanaged(i16) = .{},
    _struct_states_buf: [if (check_states) 64 else 0]State = undefined,
    _container_states_buf: [if (check_states) 64 else 0]State = undefined,
    struct_states: std.ArrayListUnmanaged(State) = .{},
    container_states: std.ArrayListUnmanaged(State) = .{},

    pub fn init(self: *@This()) void {
        self.last_fids = std.ArrayListUnmanaged(i16).initBuffer(&self._last_fid_buf);
        if (check_states) {
            self.struct_states = std.ArrayListUnmanaged(State).initBuffer(&self._struct_states_buf);
            self.container_states = std.ArrayListUnmanaged(State).initBuffer(&self._container_states_buf);
        }
    }
};

/// Logical Thrift types. Check CType.fromTType to see which are supported by the protocol.
pub const TType = enum {
    STOP,
    VOID,
    BOOL,
    BYTE,
    I08,
    DOUBLE,
    I16,
    I32,
    I64,
    STRING,
    UTF7,
    STRUCT,
    MAP,
    SET,
    LIST,
    UTF8,
    UTF16,
};

/// Compact Type - types to bytes on wire.
const CType = enum(u4) {
    STOP = 0x00,
    TRUE = 0x01,
    FALSE = 0x02,
    BYTE = 0x03,
    I16 = 0x04,
    I32 = 0x05,
    I64 = 0x06,
    DOUBLE = 0x07,
    BINARY = 0x08,
    LIST = 0x09,
    SET = 0x0A,
    MAP = 0x0B,
    STRUCT = 0x0C,
    _, // Covers 0xd..0xf to be able to use @enumFromInt. Invalid values are caught later

    pub fn fromTType(t: TType) CType {
        return switch (t) {
            .STOP, .VOID => unreachable, // We trust the incoming TTypes and they are never .STOP / .VOID
            .UTF7, .UTF8, .UTF16 => unreachable, // use .STRING in ser/de code
            .BOOL => .TRUE, // for collection (list)
            .BYTE, .I08 => .BYTE,
            .DOUBLE => .DOUBLE,
            .I16 => .I16,
            .I32 => .I32,
            .I64 => .I64,
            .STRING => .BINARY,
            .STRUCT => .STRUCT,
            .MAP => .MAP,
            .SET => .SET,
            .LIST => .LIST,
        };
    }
    pub fn toTType(t: CType) error{InvalidCType}!TType {
        return switch (t) {
            .STOP => .STOP, // We don't trust types from wire. They can be anything.
            .TRUE, .FALSE => .BOOL, // for collection (list)
            .BYTE => .BYTE,
            .DOUBLE => .DOUBLE,
            .I16 => .I16,
            .I32 => .I32,
            .I64 => .I64,
            .BINARY => .STRING,
            .STRUCT => .STRUCT,
            .MAP => .MAP,
            .SET => .SET,
            .LIST => .LIST,
            else => return error.InvalidCType,
        };
    }
};

pub const ListBeginMeta = struct {
    elem_type: TType,
    size: u32,
};

pub const FieldMeta = struct { fid: i16, tp: TType };

pub const Reader = struct {
    state: State = .CLEAR,
    last_fid: i16 = 0,
    bool_value: ?bool = null,

    stacks: Stacks = .{},

    reader: std.Io.Reader,

    pub fn init(self: *Reader, r: std.Io.Reader) void {
        self.* = .{ .reader = r };
        self.stacks.init();
    }

    pub const TransportError = error{ ReadFailed, EndOfStream }; // TransportError, can't read or EOF
    pub const TransportStateError = error{InvalidState} || TransportError; // above or using this wrong
    pub const TransportStateAllocError = error{OutOfMemory} || TransportStateError; // ... or allocation trouble
    pub const CompactProtocolError = error{ InvalidCType, Overflow } || TransportStateAllocError; // ... or invalid CType u4

    /// Used by layer above the protocol
    pub const ThriftError = error{ CantParseUnion, RequiredFieldMissing };

    pub fn readStructBegin(self: *Reader) error{ OutOfMemory, InvalidState }!void {
        const old_state = self.state;
        try self.state.transition(.initMany(&[_]State{ .CLEAR, .CONTAINER, .VALUE }), .FIELD);
        try self.stacks.last_fids.appendBounded(self.last_fid);
        if (check_states) try self.stacks.struct_states.appendBounded(old_state);
        self.last_fid = 0;
    }

    // The whitepaper and impls in https://github.com/apache/thrift/tree/master/lib and the protocol base class
    // has readFieldBegin also return a field name, but it's always empty for compact protocol.
    pub fn readFieldBegin(self: *Reader) (error{ InvalidCType, Overflow } || TransportStateError)!FieldMeta {
        try self.state.check(.initOne(.FIELD));
        const byte: u8 = try self.reader.takeByte();
        const tp_byte: u8 = byte & 0xF;
        const tp: CType = @enumFromInt(tp_byte); // TODO unsafe!

        if (tp == .STOP) {
            return .{ .fid = 0, .tp = TType.STOP };
        }

        const delta: u8 = byte >> 4;
        const fid: i16 = if (delta == 0) try self.readI16() else self.last_fid + delta;
        self.last_fid = fid;

        if (tp == CType.TRUE or tp == CType.FALSE) {
            self.bool_value = (tp == .TRUE);
            try self.state.transition(.initOne(.FIELD), .BOOL);
        } else {
            try self.state.transition(.initOne(.FIELD), .VALUE);
        }

        return .{ .fid = fid, .tp = try tp.toTType() };
    }

    pub fn readFieldEnd(self: *Reader) error{InvalidState}!void {
        try self.state.transition(.initMany(&[_]State{ .VALUE, .BOOL }), .FIELD);
    }

    pub fn readBinary(self: *Reader, alloc: std.mem.Allocator) (error{Overflow} || TransportStateAllocError)![]const u8 {
        try self.state.check(.initMany(&[_]State{ .VALUE, .CONTAINER }));
        const len = try self.reader.takeLeb128(u64);
        const res = try self.reader.readAlloc(alloc, len);
        std.debug.assert(res.len == len);
        return res;
    }

    pub fn readByte(self: *Reader) TransportStateAllocError!u8 {
        //std.debug.print("reading byte\n", {});
        try self.state.check(.initMany(&[_]State{ .VALUE, .CONTAINER }));
        return try self.reader.takeByte();
    }

    pub fn readI08(self: *Reader) TransportStateAllocError!i8 {
        return @bitCast(try self.readByte());
    }

    fn decodeZigZag(comptime SignedT: type, n: anytype) SignedT {
        const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
        const val: UnsignedT = @intCast(n);
        const sign_mask = -@as(SignedT, @intCast(val & 1));
        return @as(SignedT, @intCast(val >> 1)) ^ sign_mask;
    }

    pub fn readI16(self: *Reader) (error{Overflow} || TransportStateError)!i16 {
        const v = try self.reader.takeLeb128(u16);
        return decodeZigZag(i16, v);
    }

    pub fn readI32(self: *Reader) (error{Overflow} || TransportStateError)!i32 {
        const v = try self.reader.takeLeb128(u32);
        return decodeZigZag(i32, v);
    }

    pub fn readI64(self: *Reader) (error{Overflow} || TransportStateError)!i64 {
        const vv = try self.reader.takeLeb128(u64);
        return decodeZigZag(i64, vv);
    }

    pub fn readDouble(self: *Reader) TransportStateError!f64 {
        const u: u64 = try self.reader.takeInt(u64, .little);
        return @bitCast(u);
    }

    pub fn readListBegin(self: *Reader) CompactProtocolError!ListBeginMeta {
        try self.state.check(.initMany(&[_]State{ .VALUE, .CONTAINER }));
        if (check_states) {
            try self.stacks.container_states.appendBounded(self.state);
        }
        const size_type = try self.reader.takeByte();
        var size: u32 = size_type >> 4;
        const list_type: CType = @enumFromInt(size_type & 0x0f);
        if (size == 15) {
            size = try self.reader.takeLeb128(u32);
        }
        try self.state.transition(.initMany(&[_]State{ .VALUE, .CONTAINER }), .CONTAINER);
        return .{ .elem_type = try list_type.toTType(), .size = size };
    }

    pub fn readListEnd(self: *Reader) error{InvalidState}!void {
        try self.state.check(.initMany(&[_]State{ .VALUE, .CONTAINER }));
        if (check_states) {
            self.state = self.stacks.container_states.pop().?;
        }
    }

    fn skipBytes(self: *Reader, count: u64) TransportError!void {
        const discarded = try self.reader.discard(std.Io.Limit.limited64(count));
        if (discarded < count) {
            return error.EndOfStream;
        }
        std.debug.assert(discarded == count);
    }

    pub fn readBool(self: *Reader) TransportStateError!bool {
        if (self.bool_value) |b| {
            try self.state.check(.initOne(.BOOL));
            self.bool_value = null;
            return b;
        }
        try self.state.check(.initOne(.CONTAINER));
        const byte = try self.reader.takeByte();
        return byte == 1;
    }

    pub fn skip(self: *Reader, field_type: TType) (CompactProtocolError || error{NotImplemented})!void {
        try self.state.check(.initMany(&[_]State{ .VALUE, .CONTAINER, .BOOL }));

        switch (field_type) {
            // TODO exhaustive
            .STOP => {},
            .BOOL => {
                _ = try self.readBool();
            },
            .BYTE => {
                _ = try self.reader.takeByte();
            },
            .DOUBLE => {
                try self.skipBytes(8);
            },
            .I16 => {
                _ = try self.readI16();
            },
            .I32 => {
                _ = try self.reader.takeLeb128(u32);
            },
            .I64 => {
                _ = try self.readI64();
            },
            .STRING => {
                const len = try self.reader.takeLeb128(u64);
                try self.skipBytes(len);
            },
            .LIST, .SET => {
                const list_meta = try self.readListBegin();
                for (0..list_meta.size) |_| {
                    try self.skip(list_meta.elem_type);
                }
                try self.readListEnd();
            },
            .MAP => return error.NotImplemented, // Not needed for parquet footer
            .STRUCT => {
                try self.readStructBegin();
                while (true) {
                    const field = try self.readFieldBegin();
                    if (field.tp == .STOP) {
                        break;
                    }
                    try self.skip(field.tp);
                    try self.readFieldEnd();
                }
                try self.readStructEnd();
            },
            else => unreachable,
        }
    }

    pub fn readStructEnd(self: *Reader) error{InvalidState}!void {
        try self.state.check(.initOne(.FIELD));
        if (check_states) self.state = self.stacks.struct_states.pop().?;
        self.last_fid = self.stacks.last_fids.pop().?;
    }

    test "fuzz TCompactProtocol" {
        const Context = struct {
            const ApiFn = enum(u4) {
                readStructBegin,
                readFieldBegin,
                readBinary,
                readI64,
                readListBegin,
                readListEnd,
                readStructEnd,
                skip,
                _,
            };

            fn oneInstr(parser: *Reader, alloc: std.mem.Allocator, fn_to_call: ApiFn) !void {
                switch (fn_to_call) {
                    .readStructBegin => {
                        _ = try parser.readStructBegin();
                    },
                    .readFieldBegin => {
                        _ = try parser.readFieldBegin();
                    },
                    .readBinary => {
                        _ = try parser.readBinary(alloc);
                    },
                    .readI64 => {
                        _ = try parser.readI64();
                    },
                    .readListBegin => {
                        _ = try parser.readListBegin();
                    },
                    .readListEnd => {
                        _ = try parser.readListEnd();
                    },
                    .readStructEnd => {
                        _ = try parser.readStructEnd();
                    },
                    .skip => {
                        const types = [_]TType{ .STOP, .BYTE, .DOUBLE, .I16, .I32, .I64, .STRING, .LIST, .SET, .MAP, .STRUCT, .BOOL };
                        for (types) |t| {
                            parser.skip(t) catch {};
                        }
                    },
                    else => {},
                }
            }

            fn testOne(context: @This(), input: []const u8) !void {
                _ = context;
                if (input.len < 2) return;

                var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                var parser: Reader = undefined;
                parser.init(std.Io.Reader.fixed(input[2..]));

                var instructions: [4]u4 = undefined;
                for (&instructions, 0..) |*ip, i| {
                    ip.* = std.mem.readPackedInt(u4, input[0..2], i * @bitSizeOf(u4), @import("builtin").cpu.arch.endian());
                }

                for (instructions) |instruction| {
                    oneInstr(&parser, allocator, @enumFromInt(instruction)) catch {};
                }
            }
        };
        try std.testing.fuzz(Context{}, Context.testOne, .{});
    }

    test "readBinary" {
        var data = [_]u8{ 0x03, 'f', 'o', 'o' };
        var parser: Reader = undefined;
        parser.init(std.Io.Reader.fixed(&data));
        parser.state = .VALUE;
        const alloc = std.testing.allocator;
        const str = try parser.readBinary(alloc);
        defer alloc.free(str);
        try std.testing.expectEqualSlices(u8, "foo", str);
    }

    test "readListBegin small" {
        // list<string> size 3
        var data = [_]u8{0x38};
        var parser: Reader = undefined;
        parser.init(std.Io.Reader.fixed(&data));
        parser.state = .VALUE;
        const res = try parser.readListBegin();
        try std.testing.expectEqual(res.elem_type, TType.STRING);
        try std.testing.expectEqual(res.size, 3);
    }

    test "readListBegin large" {
        // list<i64> size 20
        var data = [_]u8{ 0xf6, 0x14 };
        var parser: Reader = undefined;
        parser.init(std.Io.Reader.fixed(&data));
        parser.state = .VALUE;

        const res = try parser.readListBegin();
        try std.testing.expectEqual(res.elem_type, TType.I64);
        try std.testing.expectEqual(res.size, 20);
    }

    test "readI64" {
        const data = &[_]u8{
            0x16,
            0xa4,
            0x8b,
            0xb0,
            0x99,
            0x09,
            0x00,
        };
        var parser: Reader = undefined;
        parser.init(std.Io.Reader.fixed(data));
        try parser.readStructBegin();
        const field = try parser.readFieldBegin();
        try std.testing.expectEqual(field.fid, 1);
        try std.testing.expectEqual(field.tp, TType.I64);
        const value = try parser.readI64();
        try std.testing.expectEqual(value, 1234567890);
        try parser.readFieldEnd();
        try parser.readStructEnd();
    }

    test "readBools" {
        const expectEqual = std.testing.expectEqual;

        // var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        // const alloc = arena.allocator();
        // defer arena.deinit();

        var buf: [255]u8 = undefined;
        var writer: Writer = undefined;
        writer.init(.fixed(&buf));
        writer.state = .FIELD;
        try writer.writeMany(&[_]Writer.ApiCall{
            .{ .FieldBegin = .{ .tp = .BOOL, .fid = 14 } },
            .{ .Bool = true },
            .FieldEnd,
            .{ .FieldBegin = .{ .tp = .BOOL, .fid = 75 } },
            .{ .Bool = false },
            .FieldEnd,
            .{ .FieldBegin = .{ .tp = .I16, .fid = 1 } },
            .{ .I16 = -5 },
            .FieldEnd,
            .{ .FieldBegin = .{ .tp = .LIST, .fid = 100 } },
            .{ .ListBegin = .{ .elem_type = .BOOL, .size = 3 } },
            .{ .Bool = true },
            .{ .Bool = true },
            .{ .Bool = false },
            .ListEnd,
            .FieldEnd,
            .{ .FieldBegin = .{ .tp = .I16, .fid = 10 } },
            .{ .I16 = 5 },
        });

        var reader: Reader = undefined;
        reader.init(.fixed(writer.writer.buffered()));
        reader.state = .FIELD;
        const fb1 = try reader.readFieldBegin();
        try expectEqual(fb1, FieldMeta{ .tp = .BOOL, .fid = 14 });

        const b1 = try reader.readBool();
        try expectEqual(b1, true);
        try reader.readFieldEnd();

        const fb2 = try reader.readFieldBegin();
        try expectEqual(fb2, FieldMeta{ .tp = .BOOL, .fid = 75 });
        const b2 = try reader.readBool();
        try expectEqual(b2, false);
        try reader.readFieldEnd();

        const fb3 = try reader.readFieldBegin();
        try expectEqual(fb3, FieldMeta{ .tp = .I16, .fid = 1 });
        const int1 = try reader.readI16();
        try expectEqual(int1, -5);
        try reader.readFieldEnd();

        const fb4 = try reader.readFieldBegin();
        try expectEqual(fb4, FieldMeta{ .tp = .LIST, .fid = 100 });
        const lm1 = try reader.readListBegin();
        try expectEqual(lm1, ListBeginMeta{ .elem_type = .BOOL, .size = 3 });

        const b3 = try reader.readBool();
        const b4: bool = try reader.readBool();
        const b5: bool = try reader.readBool();

        try reader.readListEnd();
        try reader.readFieldEnd();

        const fb5 = try reader.readFieldBegin();
        try expectEqual(b3, true);
        try expectEqual(b4, true);
        try expectEqual(b5, false);
        try expectEqual(fb5, FieldMeta{ .tp = .I16, .fid = 10 });

        const int2 = try reader.readI16();
        try expectEqual(int2, 5);
    }
};

pub const Writer = struct {
    writer: std.Io.Writer,
    last_fid: i16 = 0,
    bool_fid: ?i16 = null,
    state: State = .CLEAR,

    stacks: Stacks = .{},

    pub fn init(self: *Writer, w: std.Io.Writer) void {
        self.* = .{ .writer = w };
        self.stacks.init();
    }

    pub const WriterError = std.Io.Writer.Error || error{ InvalidState, OutOfMemory };

    fn encodeZigZag(comptime SignedT: type, n: SignedT) std.meta.Int(.unsigned, @bitSizeOf(SignedT)) {
        const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
        const nu: UnsignedT = @bitCast(n);
        const sign_mask: UnsignedT = if (n < 0) @as(UnsignedT, @bitCast(@as(SignedT, -1))) else 0;
        return (nu << 1) ^ sign_mask;
    }

    pub const ApiCall = union(enum) {
        StructBegin,
        StructEnd,
        FieldBegin: FieldMeta,
        FieldEnd,
        FieldStop,
        Binary: []const u8,
        Bool: bool,
        Byte: u8,
        I08: i8,
        I16: i16,
        I32: i32,
        I64: i64,
        Double: f64,
        ListBegin: ListBeginMeta,
        ListEnd,
    };

    fn writeFieldHeader(self: *Writer, field: struct { ctype: CType, fid: i16 }) (std.Io.Writer.Error || error{InvalidState})!void {
        try self.state.check(.initMany(&[_]State{ .BOOL, .VALUE }));
        const delta = field.fid - self.last_fid;
        const ctype: u8 = @intFromEnum(field.ctype);
        if (delta > 0 and delta <= 15) {
            const delta8: u8 = @intCast(delta);
            const b: u8 = @as(u8, delta8 << 4) | ctype;
            try self.writer.writeByte(b);
        } else {
            try self.writer.writeByte(ctype);
            try self.writer.writeUleb128(@as(u16, encodeZigZag(i16, field.fid)));
        }
        self.last_fid = field.fid;
    }

    pub fn write(self: *Writer, api_call: ApiCall) WriterError!void {
        switch (api_call) {
            .StructBegin => {
                try self.stacks.last_fids.appendBounded(self.last_fid);
                if (check_states) try self.stacks.struct_states.appendBounded(self.state);
                try self.state.transition(.initMany(&[_]State{ .CLEAR, .CONTAINER, .VALUE }), .FIELD);
                self.last_fid = 0;
            },
            .StructEnd => {
                try self.state.check(.initOne(.FIELD));
                if (check_states) {
                    self.state = self.stacks.struct_states.pop().?;
                }
                self.last_fid = self.stacks.last_fids.pop().?;
            },
            .ListEnd => {
                try self.state.check(.initOne(.CONTAINER));
                if (check_states) self.state = self.stacks.container_states.pop().?;
            },
            .FieldBegin => |field| {
                // TODO: this is for non-bool. Handle BOOL finally?
                // These are the Reader type structs, not the writer.
                // TODO: it's not .FALSE / .TRUE, it's .BOOL (TType.BOOL==2 in python).
                // This maybe works because .FALSE == 2. But .TRUE == 1 and TType.VOID == 1??
                if (field.tp == .BOOL) {
                    self.bool_fid = field.fid;
                    try self.state.transition(.initOne(.FIELD), .BOOL);
                    return;
                }
                try self.state.transition(.initOne(.FIELD), .VALUE);
                try self.writeFieldHeader(.{ .ctype = CType.fromTType(field.tp), .fid = field.fid });
            },
            .FieldEnd => {
                try self.state.transition(.initMany(&[_]State{ .VALUE, .BOOL }), .FIELD);
            },
            .FieldStop => {
                try self.state.transition(.initOne(.FIELD), .FIELD);
                try self.writer.writeByte(0);
            },
            .Binary => |s| {
                try self.writer.writeUleb128(s.len);
                try self.writer.writeAll(s);
            },
            .Bool => |b| {
                if (self.bool_fid) |fid| {
                    try self.state.check(.initOne(.BOOL));
                    try self.writeFieldHeader(.{ .ctype = (if (b) .TRUE else .FALSE), .fid = fid });
                    self.bool_fid = null;
                    return;
                }
                try self.state.check(.initOne(.CONTAINER));
                try self.writer.writeByte(if (b) 1 else 0);
            },
            .Byte => |b| {
                try self.writer.writeByte(b);
            },
            .I08 => |i| {
                try self.writer.writeByte(@bitCast(i));
            },
            .I16 => |i| {
                try self.writer.writeUleb128(encodeZigZag(i16, i));
            },
            .I32 => |i| {
                try self.writer.writeUleb128(encodeZigZag(i32, i));
            },
            .I64 => |i| {
                try self.writer.writeUleb128(encodeZigZag(i64, i));
            },
            .Double => |f| {
                try self.writer.writeInt(u64, @bitCast(f), .little);
            },
            .ListBegin => |meta| {
                if (check_states) try self.stacks.container_states.appendBounded(self.state);
                try self.state.transition(.initMany(&[_]State{ .VALUE, .CONTAINER }), .CONTAINER);
                const ctype: u8 = @intFromEnum(CType.fromTType(meta.elem_type));
                if (meta.size <= 14) {
                    const size8: u8 = @intCast(meta.size);
                    try self.writer.writeByte(@as(u8, size8 << 4) | ctype);
                } else {
                    try self.writer.writeByte(0xf0 | ctype);
                    try self.writer.writeUleb128(meta.size);
                }
            },
        }
    }

    pub fn writeMany(self: *Writer, api_calls: []const ApiCall) WriterError!void {
        for (api_calls) |api_call| {
            try self.write(api_call);
        }
    }

    test "api functions" {
        var buf: [255]u8 = undefined;
        var tw: Writer = undefined;
        tw.init(.fixed(&buf));

        // Test struct with some fields
        const struct_calls = [_]ApiCall{
            .StructBegin,
            .{ .FieldBegin = .{ .tp = .STRING, .fid = 1 } },
            .{ .Binary = "hej" },
            .FieldEnd,
            .{ .FieldBegin = .{ .tp = .I16, .fid = 3 } },
            .{ .I16 = -123 },
            .FieldEnd,
            .FieldStop,
        };
        try tw.writeMany(&struct_calls);

        const expected_struct_bytes = [_]u8{
            @as(u8, 1 << 4) | @as(u8, @intFromEnum(CType.BINARY)),
            3,
            'h',
            'e',
            'j',
            @as(u8, 2 << 4) | @as(u8, @intFromEnum(CType.I16)),
            0xF5,
            0x01,
            0,
        };
        try std.testing.expectEqualSlices(u8, &expected_struct_bytes, tw.writer.buffered());

        // Test list of bools
        tw.writer.end = 0;
        tw.last_fid = 0; // reset state
        tw.state = .VALUE;
        const list_calls = [_]ApiCall{
            .{ .ListBegin = .{ .elem_type = .BOOL, .size = 2 } },
            .{ .Bool = true },
            .{ .Bool = false },
            .ListEnd,
        };
        try tw.writeMany(&list_calls);

        const expected_list_bytes = [_]u8{
            @as(u8, 2 << 4) | @as(u8, @intFromEnum(CType.TRUE)),
            1,
            0,
        };

        try std.testing.expectEqualSlices(u8, &expected_list_bytes, tw.writer.buffered());

        // bool fields
        tw.writer.end = 0;
        tw.last_fid = 0; // reset state
        tw.state = .FIELD;
        const bool_fields_calls = [_]ApiCall{
            .{ .FieldBegin = .{ .tp = .BOOL, .fid = 14 } },
            .{ .Bool = true },
            .FieldEnd,
            .{ .FieldBegin = .{ .tp = .BOOL, .fid = 75 } },
            .{ .Bool = false },
            .FieldEnd,
        };
        try tw.writeMany(&bool_fields_calls);

        const expected_delta: u16 = encodeZigZag(i16, 75);

        const expected_bool_fields_bytes = [_]u8{ @as(u8, 14 << 4) | @as(u8, @intFromEnum(CType.TRUE)), 0 | @as(u8, @intFromEnum(CType.FALSE)), @intCast((expected_delta & 0x7F) | 0x80), @intCast(expected_delta >> 7) };

        try std.testing.expectEqualSlices(u8, &expected_bool_fields_bytes, tw.writer.buffered());
    }

    test "encodeZigZag" {
        try std.testing.expectEqual(@as(u16, 0), encodeZigZag(i16, 0));
        try std.testing.expectEqual(@as(u16, 1), encodeZigZag(i16, -1));
        try std.testing.expectEqual(@as(u16, 2), encodeZigZag(i16, 1));
        try std.testing.expectEqual(@as(u16, 3), encodeZigZag(i16, -2));
        try std.testing.expectEqual(@as(u32, 4), encodeZigZag(i32, 2));
        try std.testing.expectEqual(@as(u32, 0xffffffff), encodeZigZag(i32, -2147483648));
        try std.testing.expectEqual(@as(u64, 0xffffffffffffffff), encodeZigZag(i64, -9223372036854775808));
    }
};

test {
    std.testing.refAllDeclsRecursive(@This());
}
