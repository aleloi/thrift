const std = @import("std");
const State = enum { CLEAR, FIELD_READ, VALUE_READ };

const Self = @This();
state: State = State.CLEAR,

// also part of state?
last_fid: i16 = 0,

reader: std.Io.Reader,

pub const ParseError = std.Io.Reader.Error ||
    std.mem.Allocator.Error ||
    error{ InvalidState, NotImplemented, EndOfStream };

const Type = enum(u4) {
    STOP = 0,
    TRUE = 0x01,
    FALSE = 0x02,
    I64 = 6,
    STRING = 8,
    LIST = 9,
};

pub fn readStructBegin(self: *Self) ParseError!void {
    if (self.state != State.CLEAR)
        return ParseError.InvalidState;

    // TODO save last_fid?
    self.state = State.FIELD_READ;
    self.last_fid = 0;
}

// The whitepaper and impls in https://github.com/apache/thrift/tree/master/lib and the protocol base class
// has readFieldBegin also return a field name, but it's always empty for compact protocol.
pub fn readFieldBegin(self: *Self) ParseError!struct { fid: i16, tp: Type } {
    const byte: u8 = try self.reader.takeByte();
    if (byte == @intFromEnum(Type.STOP)) {
        return .{ .fid = 0, .tp = Type.STOP };
    }

    const tp: Type = @enumFromInt(byte & 0xF);
    const delta: u8 = byte >> 4;
    var fid: i16 = undefined;
    if (delta == 0) {
        fid = try self.readI16();
    } else {
        fid = self.last_fid + delta;
        self.last_fid = fid;
    }

    if ((tp == Type.TRUE) or (tp == Type.FALSE)) {
        return ParseError.NotImplemented;
    } else {
        self.state = State.VALUE_READ;
    }

    return .{ .fid = fid, .tp = tp };
}

pub fn readString(self: *Self, alloc: std.mem.Allocator) ParseError![]const u8 {
    const len = try self.readVarint(u64);
    //const buf = try alloc.alloc(u8, len);
    const res = try self.reader.readAlloc(alloc, len);
    std.debug.assert(res.len == len);
    return res;
}

fn readVarint(self: *Self, comptime T: type) ParseError!T {
    var res: T = 0;
    var shift: u4 = 0;
    while (true) {
        const byte = try self.reader.takeByte();
        res |= @as(T, byte & 0x7f) << @as(u4, shift);
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

fn readI16(self: *Self) ParseError!i16 {
    const v = try self.readVarint(u16);
    return decodeZigZag(i16, v);
}
pub fn readI64(self: *Self) ParseError!i64 {
    const v = try self.readVarint(u64);
    return decodeZigZag(i64, v);
}

pub fn readListBegin(self: *Self) ParseError!struct { type: Self.Type, size: u32 } {
    const size_type = try self.reader.takeByte();
    var size: u32 = size_type >> 4;
    const list_type: Type = @enumFromInt(size_type & 0x0f);
    if (size == 15) {
        size = try self.readVarint(u32);
    }
    return .{ .type = list_type, .size = size };
}

pub fn readListEnd(_: *Self) ParseError!void {}

test "fuzz TCompactProtocol" {
    const Context = struct {
        const ApiFn = enum(u4) {
            readStructBegin,
            readFieldBegin,
            readString,
            readI64,
            readListBegin,
            readListEnd,
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
                .readString => {
                    _ = try parser.readString(alloc);
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
                else => {},
            }
        }

        fn testOne(context: @This(), input: []const u8) !void {
            _ = context;
            if (input.len < 2) return;

            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            var parser = Self{ .reader = std.Io.Reader.fixed(input[2..]) };

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
    var parser = Self{ .reader = std.Io.Reader.fixed(&data) };
    try std.testing.expectEqual(@as(u64, 1), try parser.readVarint(u64));

    var data2 = [_]u8{ 0x81, 0x01 };
    parser = Self{ .reader = std.Io.Reader.fixed(&data2) };
    try std.testing.expectEqual(@as(u64, 129), try parser.readVarint(u64));
}

test "readString" {
    var data = [_]u8{ 0x03, 'f', 'o', 'o' };
    var parser = Self{ .reader = std.Io.Reader.fixed(&data) };
    const alloc = std.testing.allocator;
    const str = try parser.readString(alloc);
    defer alloc.free(str);
    try std.testing.expectEqualSlices(u8, "foo", str);
}

test "readListBegin small" {
    // list<string> size 3
    var data = [_]u8{0x38};
    var parser = Self{ .reader = std.Io.Reader.fixed(&data) };
    const res = try parser.readListBegin();
    try std.testing.expectEqual(res.type, Type.STRING);
    try std.testing.expectEqual(res.size, 3);
}

test "readListBegin large" {
    // list<i64> size 20
    var data = [_]u8{ 0xf6, 0x14 };
    var parser = Self{ .reader = std.Io.Reader.fixed(&data) };
    const res = try parser.readListBegin();
    try std.testing.expectEqual(res.type, Type.I64);
    try std.testing.expectEqual(res.size, 20);
}

test "readListEnd" {
    var data = [_]u8{};
    var parser = Self{ .reader = std.Io.Reader.fixed(&data) };
    try parser.readListEnd();
}
