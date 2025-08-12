const std = @import("std");
const Reader = @import("TCompactProtocolReader.zig");
//const builtin = @import("builtin");

const check_states: bool = @import("builtin").mode == .Debug;

const Self = @This();

const State = enum {
    CLEAR ,
    FIELD_WRITE ,
    VALUE_WRITE ,
    CONTAINER_WRITE ,
    BOOL_WRITE ,
    DUMMY_STATE , // not a real state

    pub fn transition(self: *State, comptime mask: States, new_state: State) error{InvalidState}!void {
        if (check_states) {
            if (!mask.contains(self.*)) return error.InvalidState;
            self.* = new_state;
        }
    }
};
const States = std.EnumSet(State);

writer: std.Io.Writer,
last_fid: i16 = 0,
bool_fid: i16 = -1,
state: State = .CLEAR,
//alloc: std.mem.Allocator,

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

pub fn init(w: std.Io.Writer) Self {
    var self = Self { 
        .writer = w,
        .last_fids = .{},
        .struct_states = .{},
        .container_states = .{}
    };

    self.last_fids = std.ArrayListUnmanaged(i16).initBuffer(&self._last_fid_buf);
    self.struct_states = std.ArrayListUnmanaged(State).initBuffer(&self._struct_states_buf);
    self.container_states = std.ArrayListUnmanaged(State).initBuffer(&self._container_states_buf);
    return self;
}


pub const WriterError = std.Io.Writer.Error || error {InvalidState, Overflow, OutOfMemory, NotImplemented};

fn encodeZigZag(comptime SignedT: type, n: SignedT) std.meta.Int(.unsigned, @bitSizeOf(SignedT)) {
    const UnsignedT = std.meta.Int(.unsigned, @bitSizeOf(SignedT));
    const nu: UnsignedT = @bitCast(n);
    const sign_mask: UnsignedT = if (n < 0) @as(UnsignedT, @bitCast(@as(SignedT, -1))) else 0;
    return (nu << 1) ^ sign_mask;
}

fn writeVarint(self: *Self, comptime T: type, n: T) !void {
    var val = n;
    while (true) {
        if ((val & ~@as(T, 0x7F)) == 0) {
            const b: u8 = @intCast(val);
            try self.writer.writeByte(b);
            break;
        } else {
            const b: u8 = @intCast((val & 0x7F) | 0x80);
            try self.writer.writeByte(b);
            val = val >> 7;
        }
    }
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
            try self.last_fids.appendBounded(self.last_fid);
            std.debug.print("check states is: {}\n", .{check_states});
            std.debug.print("builtin is: {any}\n", .{@import("builtin")});
            std.debug.print("builtin mode is: {any}\n", .{@import("builtin").mode});
            if (check_states) try self.struct_states.appendBounded(self.state);
            //try self.structs.append(.{.state = self.state, .fid = self.last_fid});
            try self.state.transition(
                States.initMany(&[_]State{.CLEAR, .CONTAINER_WRITE, .VALUE_WRITE}),
                .FIELD_WRITE);
            self.last_fid = 0;
        },
        .StructEnd => {
            try self.state.transition(
                States.initMany(&[_]State{.FIELD_WRITE}), 
                .DUMMY_STATE);
            if (check_states) {
                self.state = self.struct_states.pop().?;
            }
            self.last_fid = self.last_fids.pop().?;
        },
        .ListEnd => {
            try self.state.transition(
                States.initOne(.CONTAINER_WRITE), .DUMMY_STATE
            );
            if (check_states) self.state = self.container_states.pop().?;

        },
        .FieldBegin => |field| {
            // TODO: this is for non-bool. Handle BOOL finally?
            // These are the Reader type structs, not the writer.
            if (field.tp == .FALSE or field.tp == .TRUE) {
                return error.NotImplemented;
            }
            try self.state.transition(
                States.initOne(.FIELD_WRITE), .VALUE_WRITE
            );
            const delta = field.fid - self.last_fid;
            if (delta > 0 and delta <= 15) {
                const delta8: u8 = @intCast(delta);
                const b: u8 = @as(u8, delta8 << 4) | @as(u8, @intFromEnum(field.tp));
                try self.writer.writeByte(b);
            } else {
                try self.writer.writeByte(@intFromEnum(field.tp));
                try self.writeVarint(u16, encodeZigZag(i16, field.fid));
            }
            self.last_fid = field.fid;
        },
        .FieldEnd => {
            try self.state.transition(
                States.initMany(&[_]State{.VALUE_WRITE, .BOOL_WRITE}),
                .FIELD_WRITE
            );
        },
        .FieldStop => {
            try self.state.transition(
                States.initOne(.FIELD_WRITE), .FIELD_WRITE
            );
            try self.writer.writeByte(0);
            },
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
            if (check_states) try self.container_states.appendBounded(self.state);
            try self.state.transition(
                States.initMany(&[_]State{.VALUE_WRITE, .CONTAINER_WRITE}),
                .CONTAINER_WRITE
            );
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
    //var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    //defer arena.deinit();
    //const alloc = arena.allocator();
    var buf: [255]u8 = undefined;
    //var fbs = ;
    var tw = Self.init(.fixed(&buf));
    

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

    // // Test list of bools
    // tw.writer.end = 0;
    // tw.last_fid = 0; // reset state
    // tw.state = .FIELD_WRITE;
    // const list_calls = [_]ApiCall{
    //     .{ .ListBegin = .{ .elem_type = .BYTE, .size = 2 } },
    //     .{ .Bool = true },
    //     .{ .Bool = false },
    //     .ListEnd,
    // };
    // try tw.writeMany(&list_calls);

    // const expected_list_bytes = [_]u8{
    //     @as(u8, 2 << 4) | @as(u8, @intFromEnum(Reader.Type.BYTE)),
    //     1,
    //     0,
    // };

    // try std.testing.expectEqualSlices(u8, &expected_list_bytes, tw.writer.buffered());
}