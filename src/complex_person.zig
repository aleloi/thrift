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

/// union Animal {
///   1: i16 age_of_dog,
///   2: i16 number_of_fish
/// }
/// unions don't neet IsSet structs. Parsing state is handled by the current union value.
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

    // Every struct or union has a FieldTag construct with the original field tags.
    const FieldTag = enum (i16) {
        age_of_dog = 1,
        number_of_fish = 2,
        default = std.math.maxInt(i16),
        _
    };

    /// Field tags are used in switch statements both in reading and writing functions.
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

    pub fn write(self: *const Animal, w: *Writer) Writer.WriterError!void {
        try w.write(.StructBegin);
        switch (self.*) {
            .age_of_dog => |age| {
                try w.writeMany(&[_]Writer.ApiCall{
                    .{.FieldBegin = .{.tp = .I16, .fid = @intFromEnum(FieldTag.age_of_dog)}},
                    .{.I16 = age},
                    .FieldEnd});
            },
            .number_of_fish => |n| {
                try w.writeMany(&[_]Writer.ApiCall{
                    .{.FieldBegin = .{.tp = .I16, .fid = @intFromEnum(FieldTag.number_of_fish)}},
                    .{.I16 = n},
                    .FieldEnd
                });

            },
        }
        try w.write(.FieldStop);
        try w.write(.StructEnd);
    }
};


/// struct Sock {
///   1: required SockType sock_type,
///   2: required i16 pattern
/// }
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

        // Validation, maybe move to a seperate function in code gen?
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

    /// Both fields are required and primitive (enums are i32 in thrift). Hence no sub calls.
    pub fn write(self: *const Sock, w: *Writer) Writer.WriterError!void {
        try w.writeMany(&[_] Writer.ApiCall{
            .StructBegin,
                .{.FieldBegin = .{.tp = .I32, .fid = @intFromEnum(FieldTag.sock_type)}},
                    .{.I32 = @intFromEnum(self.sock_type)},
                    .FieldEnd,
                .{.FieldBegin = .{.tp = .I16, .fid = @intFromEnum(FieldTag.pattern)}},
                    .{.I16 = self.pattern},
                    .FieldEnd,
                .FieldStop,
            .StructEnd});
    }
};

/// struct Person {
///   1: required string userName,
///   2: optional i64 favoriteNumber,
///   3: optional list<string> interests,
///   4: optional list<Animal> pets,
///   5: optional list<Sock> socks,
///   6: optional i64 field_thats_going_to_not_be_included,
/// }
/// This is actually wrong
pub const ComplexPerson = struct {
    userName: []const u8,
    favoriteNumber: i64,
    interests: std.ArrayList([]const u8),
    pets: std.ArrayList(Animal),
    socks: std.ArrayList(Sock),

    const FieldTag = enum(i16) {
        userName = 1,
        favoriteNumber = 2,
        interests = 3,
        pets = 4,
        socks = 5,
        default = std.math.maxInt(i16),
        _,
    };

    pub fn write(self: *const ComplexPerson, w: *Writer) Writer.WriterError!void {
        try w.writeMany(&[_] Writer.ApiCall{
            .StructBegin,
                .{.FieldBegin = .{.tp = .BINARY, .fid = @intFromEnum(FieldTag.userName)}},
                    .{.Binary = self.userName},
                    .FieldEnd,
                .{.FieldBegin = .{.tp = .I64, .fid = @intFromEnum(FieldTag.favoriteNumber)}},
                    .{.I64 = self.favoriteNumber},
                    .FieldEnd,
                .{.FieldBegin = .{.tp = .LIST, .fid = @intFromEnum(FieldTag.interests)}},
                    .{.ListBegin = .{.elem_type = .BINARY, .size = @intCast(self.interests.items.len)}},

        });
        for (self.interests.items) |item| {
            try w.write(.{.Binary = item});
        }
        try w.write(.ListEnd);
        try w.write(.FieldEnd);
        try w.write(.{.FieldBegin = .{.tp = .LIST, .fid = @intFromEnum(FieldTag.pets)}});
        
// yes, reading the pets list
// .{ .type = .STRUCT, .size = 2 }
// animal field: .{ .fid = 5, .tp = .I16 }
// Skipping 5: i16
// animal : nullq
// animal field: .{ .fid = 7, .tp = .I16 }
// Skipping 10: i16
// animal : null
// Skipping 101: i16
// Skipping 202: i16


    // try person.pets.appendSlice(&[_]Animal{.{
    //     .age_of_dog = 5
    // }, .{
    //     .number_of_fish = 10
    // }});

        const list_meta: Writer.ListBeginMeta = .{.elem_type = .STRUCT, .size = @intCast(self.pets.items.len)};
        try w.write(.{.ListBegin = list_meta});
        for (self.pets.items) |item| {
            try item.write(w);
        }
        try w.write(.ListEnd);
        try w.write(.FieldEnd);
        try w.write(.{.FieldBegin = .{.tp = .LIST, .fid = @intFromEnum(FieldTag.socks)}});
        try w.write(.{.ListBegin = .{.elem_type = .STRUCT, .size = @intCast(self.socks.items.len)}});
        for (self.socks.items) |item| {
            try item.write(w);
        }
        try w.write(.ListEnd);
        try w.write(.FieldEnd);
        try w.write(.FieldStop);
        try w.write(.StructEnd);
    }

    pub fn read(p: *Parser, alloc: std.mem.Allocator) !ComplexPerson {
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
        while (try readFieldOrStop(p)) |field| {
            sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {
                .userName => {
                    if (field.tp == Parser.Type.BINARY) {
                        person.userName = try p.readBinary(alloc);
                        userName_allocated = true;
                    } else {
                        try p.skip(field.tp);
                    }
                },
                .favoriteNumber => {
                    if (field.tp == Parser.Type.I64) {
                        person.favoriteNumber = try p.readI64();
                    } else {
                        try p.skip(field.tp);
                    }
                },
                .interests => {
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
                .pets => {
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
                                    else => return err,
                                }
                            }
                        }
                        try p.readListEnd();
                    } else {
                        try p.skip(field.tp);
                    }
                },
                .socks => {
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
                .default => try p.skip(field.tp),
                else => continue :sw .default,
            }
            try p.readFieldEnd();
        }
        try p.readStructEnd();
        return person;
    }
};

fn writeManyToBuffer(buf: []u8, calls: []const Writer.ApiCall) Writer.WriterError![]const u8 {
    //var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    //defer arena.deinit();

    var tw: Writer = undefined;
    tw.init(.fixed(buf));
    //.alloc = arena.allocator()
    
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
                .FieldEnd,
                .FieldStop,
                .StructEnd,
            });

    var parser: Parser = undefined;
    parser.init( std.Io.Reader.fixed(data) );
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
                .FieldEnd,
                .FieldStop,
                .StructEnd,
            });
    var parser: Parser = undefined;
    parser.init( std.Io.Reader.fixed(data) );
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
                        .FieldEnd,
                    .{.FieldBegin = .{.tp=.I16, .fid=2} },
                        .{.I16 = 20},
                        .FieldEnd,
                    .FieldStop,
                .StructEnd,
            });
    var parser: Parser = undefined;
    parser.init( std.Io.Reader.fixed(data) );
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
                        .FieldEnd,
                    .{.FieldBegin = .{.tp=.I16, .fid=99} },
                        .{.I16 = 500},
                        .FieldEnd,
                    .{.FieldBegin = .{.tp=.I16, .fid=2} },
                        .{.I16 = 2},
                        .FieldEnd,
                    .FieldStop,
                .StructEnd,
            });
    
    var parser: Parser = undefined;
    parser.init( std.Io.Reader.fixed(data) );
    const animal = try Animal.read(&parser);
    try std.testing.expectEqual(@as(i16, 2), animal.number_of_fish);
}

test "Sock.write" {
    //var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    //defer arena.deinit();

    var buf: [255]u8 = undefined;
    //var fbs = std.io.fixedBufferStream(&buf);
    var writer: Writer = undefined;
    writer.init(.fixed(&buf));
        

    const sock = Sock{.sock_type = .LEFT, .pattern = 42};
    try sock.write(&writer);

    var parser: Parser = undefined;
    parser.init( std.Io.Reader.fixed(writer.writer.buffered()) );
    const sock_read = try Sock.read(&parser);
    try std.testing.expectEqual(sock.sock_type, sock_read.sock_type);
    try std.testing.expectEqual(sock.pattern, sock_read.pattern);
}

test "Animal.write" {
    var buf: [255]u8 = undefined;
    var writer: Writer = undefined;
    writer.init(.fixed(&buf));


    const animal = Animal{.age_of_dog = 42};
    try animal.write(&writer);

    var parser: Parser = undefined;
    parser.init( std.Io.Reader.fixed(writer.writer.buffered()) );
    const animal_read = try Animal.read(&parser);
    try std.testing.expectEqual(animal.age_of_dog, animal_read.age_of_dog);
}

test "ComplexPerson.read" {
    //if (true) return error.SkipZigTest;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const alloc = arena.allocator();
    defer arena.deinit();
    var person = ComplexPerson{
        .userName = "Alice",
        .favoriteNumber = 1234567890,
        .interests = std.ArrayList([]const u8).init(alloc),
        .pets = std.ArrayList(Animal).init(alloc),
        .socks = std.ArrayList(Sock).init(alloc),
    };
    // defer {
    //     std.testing.allocator.free(person.userName);
    //     for (person.interests.items) |item| {
    //         std.testing.allocator.free(item);
    //     }
    //     person.interests.deinit();
    //     person.pets.deinit();
    //     person.socks.deinit();
    // }
    try person.interests.appendSlice(&[_][]const u8{"programming", "music", "travel"});
    try person.pets.appendSlice(&[_]Animal{.{
        .age_of_dog = 5
    }, .{
        .number_of_fish = 10
    }});
    try person.socks.appendSlice(&[_]Sock{
        .{
            .sock_type = .LEFT,
            .pattern = 101
        },
        .{
            .sock_type = .RIGHT,
            .pattern = 202
        },
    });

    var buf: [1024]u8 = undefined;
    //var fbs = std.io.fixedBufferStream(&buf);
    var writer: Writer = undefined;
    writer.init(.fixed(&buf));
    try person.write(&writer);

    var parser: Parser = undefined;
    parser.init( std.Io.Reader.fixed(writer.writer.buffered()) );
    const person_read = try ComplexPerson.read(&parser, alloc);
    // defer {
    //     std.testing.allocator.free(person_read.userName);
    //     for (person_read.interests.items) |item| {
    //         std.testing.allocator.free(item);
    //     }
    //     person_read.interests.deinit();
    //     person_read.pets.deinit();
    //     person_read.socks.deinit();
    // }

    try std.testing.expectEqualStrings(person.userName, person_read.userName);
    try std.testing.expectEqual(person.favoriteNumber, person_read.favoriteNumber);
    try std.testing.expectEqual(person.interests.items.len, person_read.interests.items.len);
    for (person.interests.items, person_read.interests.items) |item, other| {
        try std.testing.expectEqualStrings(item, other);
    }
    try std.testing.expectEqualSlices(Animal, person.pets.items, person_read.pets.items);
    try std.testing.expectEqualSlices(Sock, person.socks.items, person_read.socks.items);

}

// test "fail" {
//     std.debug.assert(false);
// }