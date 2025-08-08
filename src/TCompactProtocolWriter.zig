const std = @import("std");
const Reader = @import("TCompactProtocolReader.zig");
const builtin = @import("builtin");


const Self = @This();

const State = enum {
    CLEAR,
    FIELD_WRITE,
    VALUE_WRITE,
    CONTAINER_WRITE,
    BOOL_WRITE,
    // FIELD_READ = 5
    // CONTAINER_READ = 6
    // VALUE_READ = 7
    // BOOL_READ = 8
};

writer: std.Io.Writer,
last_fid: i16 = 0,
bool_fid: i16 = -1,
state: State = .CLEAR,

fn transition(self: *Self, allowed: []const State, next: State) WriterError ! void {
    if (builtin.mode == .Debug) {
        if (std.mem.indexOfScalar(State, allowed, self.state) == null) {
            return error.InvalidState;
        }
    }
    self.state = next;
}

pub const WriterError = std.Io.Writer.Error || error {InvalidState};

fn encodeZigZag(comptime SignedT: type, n: SignedT) std.meta.Int(.unsigned, @bitSizeOf(SignedT)) {
    const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
    const nu: UnsignedT = @bitCast(n);
    const sign_mask: UnsignedT = if (n < 0) @as(UnsignedT, @bitCast(@as(SignedT, -1))) else 0;
    return (nu << 1) ^ sign_mask;
}

fn writeVarint(self: *Self, comptime T: type, n: T) !void {
    var val = n;
    std.debug.print("Writing varint: {}, hex: ", .{n});
    while (true) {
        if ((val & ~@as(T, 0x7F)) == 0) {
            const b: u8 = @intCast(val);
            std.debug.print("0x{x} ", .{b});
            try self.writer.writeByte(b);
            break;
        } else {
            const b: u8 = @intCast((val & 0x7F) | 0x80);
            std.debug.print("0x{x} ", .{b});
            try self.writer.writeByte(b);
            val = val >> 7;
        }
    }
    std.debug.print("\n", .{});
}

pub const ListBeginMeta = struct {
    elem_type: Reader.Type,
    size: u32,
};

pub const ApiCall = union(enum) {
    StructBegin,
    StructEnd,
    FieldBegin: Reader.FieldMeta,
    FieldEnd,
    FieldStop,
    Binary: []const u8,
    Bool: bool,
    I16: i16,
    I32: i32,
    I64: i64,
    ListBegin: ListBeginMeta,
    ListEnd,
};

pub fn write(self: *Self, api_call: ApiCall) WriterError!void {
    switch (api_call) {
        .StructBegin => {
            self.last_fid = 0;
        },
        .StructEnd, .FieldEnd, .ListEnd => {},
        .FieldBegin => |field| {
            const delta = field.fid - self.last_fid;
            if (delta > 0 and delta <= 15) {
                const delta8: u8 = @intCast(delta);
                const b: u8 = @as(u8, delta8 << 4) | @as(u8, @intFromEnum(field.tp));
                std.debug.print("Field begin, writing 0x{x}, {}\n", .{b, field});
                try self.writer.writeByte(b);
            } else {
                try self.writer.writeByte(@intFromEnum(field.tp));
                try self.writeVarint(u16, encodeZigZag(i16, field.fid));
            }
            self.last_fid = field.fid;
        },
        .FieldStop => try self.writer.writeByte(0),
        .Binary => |s| {
            try self.writeVarint(u64, s.len);
            try self.writer.writeAll(s);
        },
        .Bool => |b| {
            try self.writer.writeByte(if (b) 1 else 0);
        },
        .I16 => |i| {
            try self.writeVarint(u16, encodeZigZag(i16, i));
        },
        .I32 => |i| {
            try self.writeVarint(u32, encodeZigZag(i32, i));
        },
        .I64 => |i| {
            try self.writeVarint(u64, encodeZigZag(i64, i));
        },
        .ListBegin => |meta| {
            if (meta.size <= 14) {
                const size8: u8 = @intCast(meta.size);
                try self.writer.writeByte(@as(u8, size8 << 4) | @as(u8, @intFromEnum(meta.elem_type)));
            } else {
                try self.writer.writeByte(@as(u8, 0xf0) | @as(u8, @intFromEnum(meta.elem_type)));
                try self.writeVarint(u32, meta.size);
            }
        },
    }
}

pub fn writeMany(self: *Self, api_calls: []const ApiCall) WriterError!void {
    for (api_calls) |api_call| {
        try self.write(api_call);
    }
}

test "api functions" {
    var buf: [255]u8 = undefined;
    //var fbs = ;
    var tw = Self{.writer=.fixed(&buf)};

    // Test struct with some fields
    const struct_calls = [_]ApiCall{
        .StructBegin,
        .{ .FieldBegin = .{ .tp = .BINARY, .fid = 1 } },
        .{ .Binary = "hej" },
        .FieldEnd,
        .{ .FieldBegin = .{ .tp = .I16, .fid = 3 } },
        .{ .I16 = -123 },
        .FieldEnd,
        .FieldStop,
    };
    try tw.writeMany(&struct_calls);

    const expected_struct_bytes = [_]u8{
        @as(u8, 1 << 4) | @as(u8, @intFromEnum(Reader.Type.BINARY)),
        3,
        'h', 'e', 'j',
        @as(u8, 2 << 4) | @as(u8, @intFromEnum(Reader.Type.I16)),
        0xF5,
        0x01,
        0,
    };
    try std.testing.expectEqualSlices(u8, &expected_struct_bytes, tw.writer.buffered());

    // Test list of bools
    tw.writer.end = 0;
    tw.last_fid = 0; // reset state
    const list_calls = [_]ApiCall{
        .{ .ListBegin = .{ .elem_type = .BYTE, .size = 2 } },
        .{ .Bool = true },
        .{ .Bool = false },
        .ListEnd,
    };
    try tw.writeMany(&list_calls);

    const expected_list_bytes = [_]u8{
        @as(u8, 2 << 4) | @as(u8, @intFromEnum(Reader.Type.BYTE)),
        1,
        0,
    };

    try std.testing.expectEqualSlices(u8, &expected_list_bytes, tw.writer.buffered());
}