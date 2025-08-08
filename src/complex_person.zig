const std = @import("std");
const Parser = @import("TCompactProtocolReader.zig");
const Writer = @import("TCompactProtocolWriter.zig");
const ParseError = Parser.ParseError;

pub const SockType = enum(i32) { LEFT = 0, RIGHT = 1, _ };

fn readFieldOrStop(p: *Parser) ParseError!?Parser.FieldMeta {
    const field = try p.readFieldBegin();
    if (field.tp == .STOP) return null;
    return field;
}

pub const Animal = union(enum) {
    age_of_dog: i16,
    number_of_fish: i16,

    pub fn read(p: *Parser) ParseError!Animal {
        var animal: ?Animal = null;
        try p.readStructBegin();
        while (try readFieldOrStop(p))  |field| {
            if (try readAnimalField(p, field)) |new_animal| {
                animal = new_animal;
            }
            try p.readFieldEnd();
        }
        try p.readStructEnd();
        return animal orelse ParseError.CantParseUnion;
    }

    const FieldTag = enum (i16) {
        age_of_dog = 1,
        number_of_fish = 2,
        default = std.math.maxInt(i16),
        _
    };

    fn readAnimalField(p: *Parser, field: Parser.FieldMeta) Parser.ParseError!?Animal {
        sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {
            .age_of_dog => {
                if (field.tp == Parser.Type.I16) {
                    return Animal{ .age_of_dog = try p.readI16() };
                } 
                continue :sw .default;
            },
            .number_of_fish => {
                if (field.tp == Parser.Type.I16) {
                    return Animal{ .number_of_fish = try p.readI16() };
                } 
                continue :sw .default;
            },
            .default => try p.skip(field.tp),
            else => continue :sw .default,
        }
        return null;
    }
};



pub const Sock = struct {
    sock_type: SockType,
    pattern: i16,

    const FieldTag = enum(i16) {
        sock_type = 1,
        pattern = 2,
        default = std.math.maxInt(i16),
        _
    };

    const Isset = struct {
        sock_type: bool = false,
        pattern: bool = false
    };

    pub fn read(p: *Parser) ParseError!Sock {
        var sock: Sock = undefined;
        var isset: Isset = .{};

        try p.readStructBegin();
        while (try readFieldOrStop(p)) |field| {
            try sock.readSockField(p, &isset, field);
            try p.readFieldEnd();
        }
        try p.readStructEnd();

        // Validation
        if (isset.sock_type and isset.pattern) {
            return sock;
        } else {
            return ParseError.RequiredFieldMissing; 
        }
    }

    fn readSockField(self: *Sock, p: *Parser, isset: *Isset, 
                    field: Parser.FieldMeta
    ) ParseError!void {
        sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {
            .sock_type => {
                if (field.tp == Parser.Type.I32) {
                    self.sock_type = @enumFromInt(try p.readI32());
                    isset.sock_type = true;
                    return;
                }
                continue :sw .default;
            },
            .pattern => {
                if (field.tp == Parser.Type.I16) {
                    self.pattern = try p.readI16();
                    isset.pattern = true;
                    return;
                }
                continue :sw .default;
            },
            .default => try p.skip(field.tp),
            else => continue :sw .default,
        }
    }
};

pub const ComplexPerson = struct {
    userName: []const u8,
    favoriteNumber: i64,
    interests: std.ArrayList([]const u8),
    pets: std.ArrayList(Animal),
    socks: std.ArrayList(Sock),
};

pub fn parseComplexPerson(p: *Parser, alloc: std.mem.Allocator) !ComplexPerson {
    var person = ComplexPerson{
        .userName = undefined,
        .favoriteNumber = undefined,
        .interests = std.ArrayList([]const u8).init(alloc),
        .pets = std.ArrayList(Animal).init(alloc),
        .socks = std.ArrayList(Sock).init(alloc),
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
        person.pets.deinit();
        person.socks.deinit();
    }

    try p.readStructBegin();
    while (true) {
        const field = try p.readFieldBegin();
        if (field.tp == Parser.Type.STOP) {
            break;
        }

        switch (field.fid) {
            1 => {
                if (field.tp == Parser.Type.BINARY) {
                    person.userName = try p.readBinary(alloc);
                    userName_allocated = true;
                } else {
                    try p.skip(field.tp);
                }
            },
            2 => {
                if (field.tp == Parser.Type.I64) {
                    person.favoriteNumber = try p.readI64();
                } else {
                    try p.skip(field.tp);
                }
            },
            3 => {
                if (field.tp == Parser.Type.LIST) {
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
            4 => {
                if (field.tp == Parser.Type.LIST) {
                    const list_meta = try p.readListBegin();
                    try person.pets.ensureTotalCapacity(list_meta.size);
                    for (0..list_meta.size) |_| {
                        //Animal.read(p)
                        if (Animal.read(p)) |animal| {
                            try person.pets.append(animal);
                        } else |err| {
                            switch (err) {
                                ParseError.CantParseUnion, ParseError.RequiredFieldMissing => {},
                                else => return err
                            }
                        }
                    }
                    try p.readListEnd();
                } else {
                    try p.skip(field.tp);
                }
            },
            5 => {
                if (field.tp == Parser.Type.LIST) {
                    const list_meta = try p.readListBegin();
                    try person.socks.ensureTotalCapacity(list_meta.size);
                    for (0..list_meta.size) |_| {
                        if (Sock.read(p)) |sock| {
                            try person.socks.append(sock);
                        } else |err| switch (err) {
                            ParseError.CantParseUnion, ParseError.RequiredFieldMissing => {},
                            else => return err,
                        }
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
    try p.readStructEnd();
    return person;
}

fn writeManyToBuffer(buf: []u8, calls: []const Writer.ApiCall) Writer.WriteError![]const u8 {
    var tw = Writer{.writer=.fixed(buf)};
    try tw.writeMany(calls);
    return tw.writer.buffered();
}

test "Animal.read - age_of_dog" {
    var buf: [255]u8 = undefined;
    const data = try writeManyToBuffer(&buf,
        &[_]Writer.ApiCall{
                .StructBegin,
                .{.FieldBegin = .{.tp=.I16, .fid=1} },
                .{.I16 = 42},
                .FieldStop,
                .StructEnd,
            });

    var parser = Parser{ .reader = std.Io.Reader.fixed(data) };
    const animal = try Animal.read(&parser);
    try std.testing.expectEqual(@as(i16, 42), animal.age_of_dog);
}

test "Animal.read - number_of_fish" {
    var buf: [255]u8 = undefined;
    const data = try writeManyToBuffer(&buf,
        &[_]Writer.ApiCall{
                .StructBegin,
                .{.FieldBegin = .{.tp=.I16, .fid=2} },
                .{.I16 = 100},
                .FieldStop,
                .StructEnd,
            });
    var parser = Parser{ .reader = std.Io.Reader.fixed(data) };
    const animal = try Animal.read(&parser);
    try std.testing.expectEqual(@as(i16, 100), animal.number_of_fish);
}

test "Animal.read - latest wins" {
    var buf: [255]u8 = undefined;
    const data = try writeManyToBuffer(&buf,
        &[_]Writer.ApiCall{
                .StructBegin,
                    .{.FieldBegin = .{.tp=.I16, .fid=1} },
                        .{.I16 = 10},
                    .{.FieldBegin = .{.tp=.I16, .fid=2} },
                        .{.I16 = 20},
                    .FieldStop,
                .StructEnd,
            });
    var parser = Parser{ .reader = std.Io.Reader.fixed(data) };
    const animal = try Animal.read(&parser);
    try std.testing.expectEqual(20, animal.number_of_fish);
}

test "Animal.read - unknown field skipped and latest wins" {
    var buf: [255]u8 = undefined;
    // Animal with age_of_dog=1, unknown field (99:I16=500), number_of_fish=2 (number_of_fish should win, unknown skipped)
    const data = try writeManyToBuffer(&buf,
        &[_]Writer.ApiCall{
                .StructBegin,
                    .{.FieldBegin = .{.tp=.I16, .fid=1} },
                        .{.I16 = 1},
                    .{.FieldBegin = .{.tp=.I16, .fid=99} },
                        .{.I16 = 500},
                    .{.FieldBegin = .{.tp=.I16, .fid=2} },
                        .{.I16 = 2},
                    .FieldStop,
                .StructEnd,
            });
    
    var parser = Parser{ .reader = std.Io.Reader.fixed(data) };
    const animal = try Animal.read(&parser);
    try std.testing.expectEqual(@as(i16, 2), animal.number_of_fish);
}

test "parseComplexPerson" {
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
    var person = try parseComplexPerson(&parser, std.testing.allocator);
    defer {
        std.testing.allocator.free(person.userName);
        for (person.interests.items) |item| {
            std.testing.allocator.free(item);
        }
        person.interests.deinit();
        person.pets.deinit();
        person.socks.deinit();
    }

    try std.testing.expectEqualStrings(person.userName, "Alice");
    try std.testing.expectEqual(person.favoriteNumber, 1234567890);
    try std.testing.expectEqual(person.interests.items.len, 3);
    try std.testing.expectEqualStrings(person.interests.items[0], "programming");
    try std.testing.expectEqualStrings(person.interests.items[1], "music");
    try std.testing.expectEqualStrings(person.interests.items[2], "travel");
    try std.testing.expectEqual(person.pets.items.len, 2);
    try std.testing.expectEqual(@as(i16, 5), person.pets.items[0].age_of_dog);
    try std.testing.expectEqual(@as(i16, 10), person.pets.items[1].number_of_fish);
    try std.testing.expectEqual(person.socks.items.len, 2);
    try std.testing.expectEqual(@as(u8, @intFromEnum(SockType.LEFT)), @intFromEnum(person.socks.items[0].sock_type));
    try std.testing.expectEqual(@as(i16, 101), person.socks.items[0].pattern);
    try std.testing.expectEqual(@as(u8, @intFromEnum(SockType.RIGHT)), @intFromEnum(person.socks.items[1].sock_type));
    try std.testing.expectEqual(@as(i16, 202), person.socks.items[1].pattern);
}

// test "fail" {
//     std.debug.assert(false);
// }