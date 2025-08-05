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
                if (field.tp == .BINARY) {
                    person.userName = try p.readBinary(alloc);
                    userName_allocated = true;
                } else {
                    try p.skip(field.tp);
                }
            },
            2 => {
                if (field.tp == .I64) {
                    person.favoriteNumber = try p.readI64();
                } else {
                    try p.skip(field.tp);
                }
            },
            3 => {
                if (field.tp == .LIST) {
                    const list_meta = try p.readListBegin();
                    try person.interests.ensureTotalCapacity(list_meta.size);
                    for (0..list_meta.size) |_| {
                        const item = try p.readBinary(alloc);
                        try person.interests.append(item);
                    }
                    try p.readListEnd();
                } else {
                    try p.skip(field.tp);
                }
            },
            else => {
                try p.skip(field.tp);
            },
        }
    }

    return person;
}

test "parsePerson" {
        const data = &[_]u8{
        0x18,
        0x05,
        0x41,
        0x6c,
        0x69,
        0x63,
        0x65,
        0x16,
        0xa4,
        0x8b,
        0xb0,
        0x99,
        0x09,
        0x19,
        0x38,
        0x0b,
        0x70,
        0x72,
        0x6f,
        0x67,
        0x72,
        0x61,
        0x6d,
        0x6d,
        0x69,
        0x6e,
        0x67,
        0x05,
        0x6d,
        0x75,
        0x73,
        0x69,
        0x63,
        0x06,
        0x74,
        0x72,
        0x61,
        0x76,
        0x65,
        0x6c,
        0x19,
        0x2c,
        0x14,
        0x0a,
        0x00,
        0x24,
        0x14,
        0x00,
        0x19,
        0x2c,
        0x15,
        0x00,
        0x14,
        0xca,
        0x01,
        0x00,
        0x15,
        0x02,
        0x14,
        0x94,
        0x03,
        0x00,
        0x00,
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
    try std.testing.expectEqualStrings(person.userName, "Alice");

    try std.testing.expectEqual(person.interests.items.len, 3);
    try std.testing.expectEqualStrings(person.interests.items[0], "programming");
    try std.testing.expectEqualStrings(person.interests.items[1], "music");
    try std.testing.expectEqualStrings(person.interests.items[2], "travel");
    try std.testing.expectEqual(person.favoriteNumber, 1234567890);
}
