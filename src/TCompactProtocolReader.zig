const std = @import("std");
//const State = enum { CLEAR, FIELD_READ, VALUE_READ };

const check_states: bool = @import("builtin").mode == .Debug;


const State = enum {
    CLEAR ,
    FIELD_READ ,
    VALUE_READ ,
    CONTAINER_READ,
    BOOL_READ ,
    DUMMY_STATE , // not a real state

    pub fn transition(self: *State, comptime mask: States, new_state: State) error{InvalidState}!void {
        if (check_states) {
            if (!mask.contains(self.*)) {
                return error.InvalidState;
            }
            self.* = new_state;
        }
    }
};
const States = std.EnumSet(State);


const Self = @This();

state: State = State.CLEAR,

// also part of state?
last_fid: i16 = 0,
bool_value: bool = false,

// Thrift structs can maybe be recursive. Python codegen seems to support it.
// C++ tries to put a non-pointer self inside self. We do not support it, and limit
// nesting to 64.
// The arrays are dependent of the buffers, and need to be initialized
_last_fid_buf: [64]i16 = undefined,
last_fids: std.ArrayListUnmanaged(i16),
_struct_states_buf: [if (check_states) 64 else 0]State = undefined,
_container_states_buf: [if (check_states) 64 else 0]State = undefined,
struct_states: std.ArrayListUnmanaged(State),
container_states: std.ArrayListUnmanaged(State),


reader: std.Io.Reader,

pub fn init(r: std.Io.Reader) Self {
    var self = Self { 
        .reader = r,
        .last_fids = .{},
        .struct_states = .{},
        .container_states = .{}
    };

    self.last_fids = std.ArrayListUnmanaged(i16).initBuffer(&self._last_fid_buf);
    self.struct_states = std.ArrayListUnmanaged(State).initBuffer(&self._struct_states_buf);
    self.container_states = std.ArrayListUnmanaged(State).initBuffer(&self._container_states_buf);
    return self;
}

pub const ParseError = std.Io.Reader.Error ||
    std.mem.Allocator.Error ||
    error{ InvalidState, NotImplemented, EndOfStream, CantParseUnion, RequiredFieldMissing };

pub const Type = enum(u4) {
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
};

pub fn readStructBegin(self: *Self) ParseError!void {
    const old_state = self.state;
    try self.state.transition(States.initMany(&[_]State{.CLEAR,
        .CONTAINER_READ, .VALUE_READ
    }), .FIELD_READ);
    try self.last_fids.appendBounded(self.last_fid);
    if (check_states) try self.struct_states.appendBounded(old_state);
    self.last_fid = 0;
}

pub const FieldMeta = struct {
    fid: i16,
    tp: Type
};

// The whitepaper and impls in https://github.com/apache/thrift/tree/master/lib and the protocol base class
// has readFieldBegin also return a field name, but it's always empty for compact protocol.
pub fn readFieldBegin(self: *Self) ParseError!FieldMeta {
    try self.state.transition(States.initOne(.FIELD_READ), .FIELD_READ);
    const byte: u8 = try self.reader.takeByte();
    if (byte == @intFromEnum(Type.STOP)) {
        return .{ .fid = 0, .tp = Type.STOP };
    }

    const tp_byte: u8 = byte & 0xF;
    const tp: Type = switch (tp_byte) {
        @intFromEnum(Type.TRUE) => Type.TRUE,
        @intFromEnum(Type.FALSE) => Type.FALSE,
        else => @enumFromInt(tp_byte),
    };
    if (tp == Type.TRUE or tp == Type.FALSE) {
        self.bool_value = (tp_byte == @intFromEnum(Type.TRUE));
        try self.state.transition(States.initFull(), .BOOL_READ);
    }

    const delta: u8 = byte >> 4;
    var fid: i16 = undefined;
    if (delta == 0) {
        fid = try self.readI16();
    } else {
        fid = self.last_fid + delta;
        self.last_fid = fid;
    }

    try self.state.transition(States.initFull(), .VALUE_READ);

    return .{ .fid = fid, .tp = tp };
}

pub fn readFieldEnd(self: *Self) ParseError!void {
    try self.state.transition(States.initMany(&[_]State{.VALUE_READ, .BOOL_READ}), 
    .FIELD_READ);
}


pub fn readBinary(self: *Self, alloc: std.mem.Allocator) ParseError![]const u8 {
    try self.state.transition(States.initMany(&[_]State{.VALUE_READ, .CONTAINER_READ}), 
            self.state);
    const len = try self.readVarint(u64);
    //const buf = try alloc.alloc(u8, len);
    const res = try self.reader.readAlloc(alloc, len);
    std.debug.assert(res.len == len);
    return res;
}

fn readVarint(self: *Self, comptime T: type) ParseError!T {
    try self.state.transition(States.initMany(&[_]State{.VALUE_READ, .CONTAINER_READ, .FIELD_READ}),
        self.state);
    var res: T = 0;
    var shift: u8 = 0;
    while (true) {
        const byte = try self.reader.takeByte();
        res |= @as(T, byte & 0x7f) << @intCast(shift);
        if (byte & 0x80 == 0) {
            return res;
        }
        shift += 7;
    }
}

fn decodeZigZag(comptime SignedT: type, n: anytype) SignedT {
    const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
    const val: UnsignedT = @intCast(n);
    const sign_mask = -@as(SignedT, @intCast(val & 1));
    return @as(SignedT, @intCast(val >> 1)) ^ sign_mask;
}

pub fn readI16(self: *Self) ParseError!i16 {
    const v = try self.readVarint(u16);
    return decodeZigZag(i16, v);
}

pub fn readI32(self: *Self) ParseError!i32 {
    const v = try self.readVarint(u32);
    return decodeZigZag(i16, v);
}

pub fn readI64(self: *Self) ParseError!i64 {
    const v = try self.readVarint(u64);
    return decodeZigZag(i64, v);
}

pub fn readListBegin(self: *Self) ParseError!struct { type: Self.Type, size: u32 } {
    try self.state.transition(States.initMany(&[_]State{.VALUE_READ, .CONTAINER_READ}), 
            self.state);
    if (check_states) {
        try self.container_states.appendBounded(self.state);
    }
    const size_type = try self.reader.takeByte();
    var size: u32 = size_type >> 4;
    const list_type: Type = @enumFromInt(size_type & 0x0f);
    if (size == 15) {
        size = try self.readVarint(u32);
    }
    try self.state.transition(States.initMany(&[_]State{.VALUE_READ, .CONTAINER_READ}), 
        .CONTAINER_READ);
    return .{ .type = list_type, .size = size };
}

pub fn readListEnd(self: *Self) ParseError!void {
    try self.state.transition(States.initMany(&[_]State{.VALUE_READ, .CONTAINER_READ}), 
        .DUMMY_STATE);
    if (check_states) {
        self.state = self.container_states.pop().?;
    }

}

fn skipBytes(self: *Self, count: u64) ParseError!void {
    const discarded = try self.reader.discard(std.Io.Limit.limited64(count));
    if (discarded < count) {
        return ParseError.EndOfStream;
    }
    std.debug.assert(discarded == count);
}

pub fn readBool(self: *Self) ParseError!bool {
    // TODO seems to not handle list<bool>
    try self.state.transition(States.initMany(&[_]State{.BOOL_READ, .CONTAINER_READ}), 
        self.state);
    return self.bool_value;
}

pub fn skip(self: *Self, field_type: Type) ParseError!void {
    try self.state.transition(States.initMany(&[_]State{.VALUE_READ, .CONTAINER_READ,
        .BOOL_READ}), self.state);

    switch (field_type) {
        .STOP => {},
        .TRUE, .FALSE => {
            _ = try self.readBool();
        },
        .BYTE => {
            _ = try self.reader.takeByte();
        },
        .DOUBLE => {
            try self.skipBytes(8);
        }, // 8 bytes for double
        .I16 => {
            _ = try self.readI16();
        },
        .I32 => {
            _ = try self.readVarint(u32);
        },
        .I64 => {
            _ = try self.readI64();
        },
        .BINARY => {
            const len = try self.readVarint(u64);
            try self.skipBytes(len);
        },
        .LIST, .SET => {
            const list_meta = try self.readListBegin();
            for (0..list_meta.size) |_| {
                try self.skip(list_meta.type);
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
            }
            try self.readStructEnd();
        },
    }
}

pub fn readStructEnd(self: *Self) ParseError!void {
    try self.state.transition(States.initOne(.FIELD_READ), .DUMMY_STATE);
    if (check_states) self.state = self.struct_states.pop().?;
    self.last_fid = self.last_fids.pop().?;
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

        fn oneInstr(parser: *Self, alloc: std.mem.Allocator, fn_to_call: ApiFn) !void {
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
                    const types = [_]Type{ .STOP, .BYTE, .DOUBLE, .I16, .I32, .I64, .BINARY, .LIST, .SET, .MAP, .STRUCT };
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

            var parser = Self.init( std.Io.Reader.fixed(input[2..]) );

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

test "readVarint" {
    var data = [_]u8{0x01};
    var parser = Self.init( std.Io.Reader.fixed(&data) );
    parser.state = .VALUE_READ;
    try std.testing.expectEqual(@as(u64, 1), try parser.readVarint(u64));

    var data2 = [_]u8{ 0x81, 0x01 };
    parser = Self.init( std.Io.Reader.fixed(&data2) );
    parser.state = .VALUE_READ;
    try std.testing.expectEqual(@as(u64, 129), try parser.readVarint(u64));
}

test "readBinary" {
    var data = [_]u8{ 0x03, 'f', 'o', 'o' };
    var parser = Self.init( std.Io.Reader.fixed(&data) );
    parser.state = .VALUE_READ;
    const alloc = std.testing.allocator;
    const str = try parser.readBinary(alloc);
    defer alloc.free(str);
    try std.testing.expectEqualSlices(u8, "foo", str);
}

test "readListBegin small" {
    // list<string> size 3
    var data = [_]u8{0x38};
    var parser = Self.init( std.Io.Reader.fixed(&data) );
    parser.state = .VALUE_READ;
    const res = try parser.readListBegin();
    try std.testing.expectEqual(res.type, Type.BINARY);
    try std.testing.expectEqual(res.size, 3);
}

test "readListBegin large" {
    // list<i64> size 20
    var data = [_]u8{ 0xf6, 0x14 };
    var parser = Self.init( std.Io.Reader.fixed(&data) );
    parser.state = .VALUE_READ;

    const res = try parser.readListBegin();
    try std.testing.expectEqual(res.type, Type.I64);
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
    var parser = Self.init( std.Io.Reader.fixed(data) );
    try parser.readStructBegin();
    const field = try parser.readFieldBegin();
    try std.testing.expectEqual(field.fid, 1);
    try std.testing.expectEqual(field.tp, Type.I64);
    const value = try parser.readI64();
    try std.testing.expectEqual(value, 1234567890);
    try parser.readFieldEnd();
    try parser.readStructEnd();
}


// test "fail" {
//     std.debug.assert(false);
// }