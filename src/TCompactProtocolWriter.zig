const std = @import("std");
const Reader = @import("TCompactProtocolReader.zig");

const Self = @This();

writer: std.Io.Writer,
last_fid: i16 = 0,
bool_fid: i16 = -1,

pub const WriteError = std.Io.Writer.Error;

pub fn writeStructBegin(self: *Self) WriteError!void {
    self.last_fid = 0;
}

pub fn writeStructEnd(_: *Self) WriteError!void {}

pub fn writeFieldBegin(self: *Self, field: Reader.FieldMeta) WriteError!void {
    const delta = field.fid - self.last_fid;
    if (delta > 0 and delta <= 15) {
        const delta8: u8 = @intCast(delta);
        try self.writer.writeByte((delta8 << 4) | @intFromEnum(field.tp));
    } else {
        try self.writer.writeByte(@intFromEnum(field.tp));
        try self.writeI16(field.fid);
    }
    self.last_fid = field.fid;
}

pub fn writeFieldEnd(self: *Self) WriteError!void {
    _ = self;
}

pub fn writeFieldStop(self: *Self) WriteError!void {
    try self.writer.writeByte(0);
}

fn encodeZigZag(comptime SignedT: type, n: SignedT) std.meta.Int(.unsigned, @bitSizeOf(SignedT)) {
    const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
    const nu: UnsignedT = @bitCast(n);
    const sign_mask: UnsignedT = switch (n < 0) {
        true => @bitCast(@as(SignedT, -1)),
        false => 0
    };
    
    return (nu << 1) ^ sign_mask;
}

fn writeVarint(self: *Self, comptime T: type, n: T) !void {
    var val = n;
    while (true) {
        if ((val & ~@as(T, 0x7F)) == 0) {
            try self.writer.writeByte(@intCast(val));
            break;
        } else {
            try self.writer.writeByte(@intCast((val & 0x7F) | 0x80));
            val = val >> 7;
        }
    }
}

pub fn writeI16(self: *Self, v: i16) WriteError!void {
    try self.writeVarint(u16, encodeZigZag(i16, v));
}

pub fn writeI32(self: *Self, v: i32) WriteError!void {
    try self.writeVarint(u32, encodeZigZag(i32, v));
}

pub fn writeI64(self: *Self, v: i64) WriteError!void {
    try self.writeVarint(u64, encodeZigZag(i64, v));
}

pub fn writeBinary(self: *Self, s: []const u8) WriteError!void {
    try self.writeVarint(u64, s.len);
    try self.writer.writeAll(s);
}

pub fn writeListBegin(self: *Self, elem_type: Reader.Type, size: u32) WriteError!void {
    if (size <= 14) {
        const size8: u8 = @intCast(size);
        try self.writer.writeByte((size8 << 4) | @intFromEnum(elem_type));
    } else {
        try self.writer.writeByte(0xf0 | @intFromEnum(elem_type));
        try self.writeVarint(u32, size);
    }
}

pub fn writeListEnd(_: *Self) WriteError!void {}

pub fn writeBool(_: *Self, _: bool) WriteError!void {
    unreachable;
}

pub const ApiFn = enum(u4) {
    StructBegin,
    StructEnd,
    FieldBegin,
    FieldEnd,
    Binary,
    Bool,
    I16,
    I32,
    I64,
    ListBegin,
    ListEnd
    // and more ...
};

pub const ApiCall = union(ApiFn) {
    StructBegin,
    StructEnd,
    FieldBegin: Reader.FieldMeta,
    FieldEnd,
    Binary: []const u8,
    Bool: bool,
    I16: i16,
    I32: i32,
    I64: i64,
    ListBegin,
    ListEnd,
};

pub fn write(self: *@This(), api_call: ApiCall) WriteError!void {
    switch (api_call) {
        .StructBegin => try self.writeStructBegin(),
        .StructEnd => try self.writeStructEnd(),
        .FieldBegin => |field| try self.writeFieldBegin(field),
        .FieldEnd => try self.writeFieldEnd(),
        .Binary => |s| try self.writeBinary(s),
        .Bool => |b| try self.writeBool(b),
        .I16 => |i| try self.writeI16(i),
        .I32 => |i| try self.writeI32(i),
        .I64 => |i| try self.writeI64(i),
        .ListBegin => try self.writeListBegin(),
        .ListEnd => try self.writeListEnd()
    }
}

pub fn writeMany(self: *@This(), api_calls: []const ApiCall) WriteError!void {
    for (api_calls) |api_call| {
        try self.write(api_call);
    }

}


test "api functions (OLD)" {
    var buf: [255]u8 = undefined;
    const io_writer = std.Io.Writer.fixed(&buf);
    var tw = @This(){.writer=io_writer};
    try tw.writeStructBegin();
    try tw.writeFieldBegin(.{.tp = .BINARY, .fid = 1});
    try tw.writeBinary("hej");
    try tw.writeFieldEnd();
    //try tw.writeFieldBegin(.{.BYTE, 2});
    //try tw.writeFieldEnd();
    
    try tw.writeFieldBegin(.{.tp = .I16, .fid = 3});
    try tw.writeI16(-123);
    try tw.writeFieldEnd();
}


test "api functions (NEW)" {
    var buf: [255]u8 = undefined;
    const io_writer = std.Io.Writer.fixed(&buf);
    var tw = @This(){.writer=io_writer};
    const arr  = [_]ApiCall {
        .StructBegin,
        .{.FieldBegin = .{.tp = .BINARY, .fid = 1}}
    };
    try tw.writeMany(&arr);
    // try tw.writeStructBegin();
    // try tw.writeFieldBegin(.{.BINARY, 1});
    // try tw.writeBinary("hej");
    // try tw.writeFieldEnd();
    // //try tw.writeFieldBegin(.{.BYTE, 2});
    // //try tw.writeFieldEnd();
    
    // try tw.writeFieldBegin(.{.I16, 3});
    // try tw.writeI16(-123);
    // try tw.writeFieldEnd();
}