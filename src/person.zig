const std = @import("std");
const Parser = @import("TCompactProtocol.zig");

pub const Person = struct { userName: []const u8, favoriteNumber: i64, interests: std.ArrayList([]const u8) };

pub fn parsePerson(p: anytype, alloc: std.mem.Allocator) !Person {
    var person = Person{
        .userName = undefined,
        .favoriteNumber = undefined,
        .interests = std.ArrayList([]const u8).init(alloc),
    };
    var userName_allocated = false;
    errdefer {
        if (userName_allocated) {
            alloc.free(person.userName);
        }
        for (person.interests.items) |item| {
            alloc.free(item);
        }
        person.interests.deinit();
    }

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
                    userName_allocated = true;
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
    var parser = Parser{ .reader = std.Io.Reader.fixed(data) };
    var person = try parsePerson(&parser, std.testing.allocator);
    defer std.testing.allocator.free(person.userName);
    defer {
        for (person.interests.items) |item| {
            std.testing.allocator.free(item);
        }
        person.interests.deinit();
    }

    // catch |err| {
    //     if (err == error.EndOfStream) {
    //         return;
    //     } else {
    //         return err;
    //     }
    // };
    try std.testing.expectEqualStrings(person.userName, "John Doe");

    try std.testing.expectEqual(person.interests.items.len, 3);
    try std.testing.expectEqualStrings(person.interests.items[0], "coding");
    try std.testing.expectEqualStrings(person.interests.items[1], "reading");
    try std.testing.expectEqualStrings(person.interests.items[2], "hiking");
    try std.testing.expectEqual(person.favoriteNumber, 42);
}
