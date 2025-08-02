const std = @import("std");
const State = enum { CLEAR, FIELD_READ, VALUE_READ };
const Parser = @This();

state: State = State.CLEAR,

// also part of state?
last_fid: i16 = 0,

//data: []const u8,

reader: std.Io.Reader,

pub const ParseError = std.Io.Reader.Error || error{ InvalidState, NotImplemented };

const Type = enum(u4) {
    STOP = 0,
    TRUE = 0x01,
    FALSE = 0x02,
    I64 = 6,
    STRING = 8,
    LIST = 9,
};

pub fn readStructBegin(self: *Parser) ParseError!void {
    if (self.state != State.CLEAR)
        return ParseError.InvalidState;

    // TODO save last_fid?
    self.state = State.FIELD_READ;
    self.last_fid = 0;
}

// The whitepaper and impls in https://github.com/apache/thrift/tree/master/lib and the protocol base class
// has readFieldBegin also return a field name, but it's always empty for compact protocol.
pub fn readFieldBegin(self: *Parser) ParseError!struct { fid: i16, tp: Type } {
    const byte: u8 = try self.reader.takeByte();
    if (byte == @intFromEnum(Type.STOP)) {
        // Should the whole Parser enter a stop state here?
        return .{ .fid = 0, .tp = Type.STOP };
    }

    const tp: Type = @enumFromInt(byte & 0xF);
    const delta: u8 = byte >> 4;
    var fid: i16 = undefined;
    if (delta == 0) {
        fid = try readI16(self);
    } else {
        fid = self.last_fid + delta;
        self.last_fid = fid;
    }

    if ((tp == Type.TRUE) or (tp == Type.FALSE)) {
        return ParseError.NotImplemented;
        // if type == CompactType.TRUE:
        //     self.state = BOOL_READ
        //     self.__bool_value = True
        // elif type == CompactType.FALSE:
        //     self.state = BOOL_READ
        //     self.__bool_value = False
    } else {
        self.state = State.VALUE_READ;
    }

    //std.debug.print("tp: {}, tag: {}\n", .{ tp, tag });
    return .{ .fid = fid, .tp = tp };
}

pub fn readString(self: *Parser) ParseError![]const u8 {
    _ = self;
    return ParseError.NotImplemented;
}

// fn eatByte(self: *Parser) ParseError!u8 {
//     if (self.data.len == 0)
//         return ParseError.UnexpectedEOF;

//     const byte = self.data[0];
//     self.data = self.data[1..];
//     return byte;
// }

fn readI16(self: *Parser) ParseError!i16 {
    _ = self;
    return ParseError.NotImplemented;
}
