const std = @import("std");
const Parser = @import("TCompactProtocol.zig");



const Person = struct { userName: []const u8, favoriteNumber: i64, interests: std.ArrayList([]const u8) };

//const SomeErrorDunno = error{UnexpectedEndOfData};

fn parsePerson(p: anytype, alloc: std.mem.Allocator) !Person {
    var person = Person{
        .userName = undefined,
        .favoriteNumber = undefined,
        .interests = std.ArrayList([]const u8).init(alloc),
    };

    try p.readStructBegin();
    while (true) {
        const field = try p.readFieldBegin();
        if (field.tp == .STOP) {
            break;
        }

        switch (field.fid) {
            1 => {
                if (field.tp == .STRING) {
                    person.userName = try p.readString(alloc);
                } else {
                    return error.InvalidType;
                }
            },
            2 => {
                if (field.tp == .I64) {
                    person.favoriteNumber = try p.readI64();
                } else {
                    return error.InvalidType;
                }
            },
            3 => {
                if (field.tp == .LIST) {
                    const list_meta = try p.readListBegin();
                    try person.interests.ensureTotalCapacity(list_meta.size);
                    for (0..list_meta.size) |_| {
                        const item = try p.readString(alloc);
                        try person.interests.append(item);
                    }
                    try p.readListEnd();
                } else {
                    return error.InvalidType;
                }
            },
            else => return error.UnknownField,
        }
    }

    return person;
}

test "parsePerson" {
    const data = &[_]u8{
        24,
        8,
        74,
        111,
        104,
        110,
        32,
        68,
        111,
        101,
        22,
        84,
        25,
        56,
        6,
        99,
        111,
        100,
        105,
        110,
        103,
        7,
        114,
        101,
        97,
        100,
        105,
        110,
        103,
        6,
        104,
        105,
        107,
        105,
        110,
        103,
        0,
    };
    //const P = 
    var parser = Parser{.reader = std.Io.Reader.fixed(data)};

    //var parser = Parser(std.io.FixedBufferStream([]const u8).Reader).{ .reader = stream.reader() };
    const person = try parsePerson(&parser, std.testing.allocator);

    try std.testing.expectEqualStrings(person.userName, "John Doe");
    try std.testing.expectEqual(person.favoriteNumber, 42);
    try std.testing.expectEqual(person.interests.items.len, 3);
    try std.testing.expectEqualStrings(person.interests.items[0], "coding");
    try std.testing.expectEqualStrings(person.interests.items[1], "reading");
    try std.testing.expectEqualStrings(person.interests.items[2], "hiking");
}

pub fn main() !void {
    const Alloc: type = std.heap.DebugAllocator(.{});
    var alloc: Alloc = Alloc.init;
    const alist = std.ArrayList([]const u8).init(alloc.allocator());
    _ = Person{ .userName = "hej", .favoriteNumber = 0, .interests = alist };
    const data = &[_]u8{
        24,
        8,
        74,
        111,
        104,
        110,
        32,
        68,
        111,
        101,
        22,
        84,
        25,
        56,
        6,
        99,
        111,
        100,
        105,
        110,
        103,
        7,
        114,
        101,
        97,
        100,
        105,
        110,
        103,
        6,
        104,
        105,
        107,
        105,
        110,
        103,
        0,
    };
    //var stream = std.io.fixedBufferStream(data);
    var parser = Parser{.reader = std.Io.Reader.fixed(data)};
    const p = try parsePerson(&parser, alloc.allocator());
    std.debug.print(" {}\n", .{ p});
    if (false) {
        //parsePerson.wtf_functions_cant_have_functions();
    }
}

test {
    std.testing.refAllDecls(@This());
}
