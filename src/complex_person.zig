const std = @import("std");
const TCompactProtocol = @import("TCompactProtocol.zig");
const Parser = TCompactProtocol.Reader;
const Writer = TCompactProtocol.Writer;
const CompactProtocolError = Parser.CompactProtocolError || error {NotImplemented};
const ThriftError = Parser.ThriftError;
const TType = TCompactProtocol.TType;
const FieldMeta = TCompactProtocol.FieldMeta;
const ListBeginMeta = TCompactProtocol.ListBeginMeta;


pub const SockType = enum(i32) { LEFT = 0, RIGHT = 1, _ };

fn readFieldOrStop(p: *Parser) CompactProtocolError!?FieldMeta {
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

    pub fn read(p: *Parser) (CompactProtocolError || ThriftError)!Animal {
        var animal: ?Animal = null;
        try p.readStructBegin();
        while (try readFieldOrStop(p))  |field| {
            if (try readAnimalField(p, field)) |new_animal| {
                animal = new_animal;
            }
            try p.readFieldEnd();
        }
        try p.readStructEnd();
        return animal orelse error.CantParseUnion;
    }

    // Every struct or union has a FieldTag construct with the original field tags.
    const FieldTag = enum (i16) {
        age_of_dog = 1,
        number_of_fish = 2,
        default = std.math.maxInt(i16),
        _
    };

    /// Field tags are used in switch statements both in reading and writing functions.
    fn readAnimalField(p: *Parser, field: FieldMeta) CompactProtocolError!?Animal {
        sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {
            .age_of_dog => {
                if (field.tp == TType.I16) {
                    return Animal{ .age_of_dog = try p.readI16() };
                } 
                continue :sw .default;
            },
            .number_of_fish => {
                if (field.tp == TType.I16) {
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

    pub fn read(p: *Parser) (CompactProtocolError || ThriftError)!Sock {
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
            return ThriftError.RequiredFieldMissing; 
        }
    }

    fn readSockField(self: *Sock, p: *Parser, isset: *Isset, 
                    field: FieldMeta
    ) CompactProtocolError!void {
        sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {
            .sock_type => {
                if (field.tp == TType.I32) {
                    self.sock_type = @enumFromInt(try p.readI32());
                    isset.sock_type = true;
                    return;
                }
                continue :sw .default;
            },
            .pattern => {
                if (field.tp == TType.I16) {
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
/// }
/// This is wrong; optional fields in thrift should map to optional fields in zig.
pub const ComplexPerson = struct {
    userName: []const u8,
    favoriteNumber: ?i64,
    interests: ?std.ArrayList([]const u8),
    pets: ?std.ArrayList(Animal),
    socks: ?std.ArrayList(Sock),

    const FieldTag = enum(i16) {
        userName = 1,
        favoriteNumber = 2,
        interests = 3,
        pets = 4,
        socks = 5,
        default = std.math.maxInt(i16),
        _,
    };

    const Isset = struct {
        userName: bool = false,
        favoriteNumber: bool = false,
        interests: bool = false,
        pets: bool = false,
        socks: bool = false
    };

    pub fn write(self: *const ComplexPerson, w: *Writer) Writer.WriterError!void {
        try w.writeMany(&[_] Writer.ApiCall{
            .StructBegin,
                .{.FieldBegin = .{.tp = .STRING, .fid = @intFromEnum(FieldTag.userName)}},
                    .{.Binary = self.userName},
                    .FieldEnd
        });
        if (self.favoriteNumber) |nr| {
            try w.writeMany(&[_] Writer.ApiCall{
                .{.FieldBegin = .{.tp = .I64, .fid = @intFromEnum(FieldTag.favoriteNumber)}},
                    .{.I64 = nr},
                    .FieldEnd
            });
        }
        if (self.interests) |interests| {
            try w.writeMany(&[_] Writer.ApiCall{
                .{.FieldBegin = .{.tp = .LIST, .fid = @intFromEnum(FieldTag.interests)}},
                    .{.ListBegin = .{.elem_type = .STRING, .size = @intCast(interests.items.len)}}
                });
            for (interests.items) |item| {
                try w.write(.{.Binary = item});
            }
            try w.write(.ListEnd);
            try w.write(.FieldEnd);
        }
        if (self.pets) | pets| {
            try w.write(.{.FieldBegin = .{.tp = .LIST, .fid = @intFromEnum(FieldTag.pets)}});
            const list_meta: ListBeginMeta = .{.elem_type = .STRUCT, .size = @intCast(pets.items.len)};
            try w.write(.{.ListBegin = list_meta});
            for (pets.items) |item| {
                try item.write(w);
            }
            try w.write(.ListEnd);
            try w.write(.FieldEnd);
        }
        if (self.socks) |socks| {
            try w.write(.{.FieldBegin = .{.tp = .LIST, .fid = @intFromEnum(FieldTag.socks)}});
            try w.write(.{.ListBegin = .{.elem_type = .STRUCT, .size = @intCast(socks.items.len)}});
            for (socks.items) |item| {
                try item.write(w);
            }
            try w.write(.ListEnd);
            try w.write(.FieldEnd);
        }
        try w.write(.FieldStop);
        try w.write(.StructEnd);
    }

    fn readComplexPersonField(person: *ComplexPerson, p: *Parser, isset: *Isset, alloc: std.mem.Allocator, field: FieldMeta) CompactProtocolError!void {
        sw: switch (@as(FieldTag, @enumFromInt(field.fid))) {
            .userName => {
                if (field.tp == TType.STRING) {
                    person.userName = try p.readBinary(alloc);
                    isset.userName = true;
                    return;
                } 
                continue :sw .default;
            },
            .favoriteNumber => {
                if (field.tp == TType.I64) {
                    person.favoriteNumber = try p.readI64();
                    isset.favoriteNumber = true;
                    return;
                } 
                continue :sw .default;
            },
            .interests => {
                if (field.tp == TType.LIST) {
                    const list_meta = try p.readListBegin();
                    // TODO maximal length / byte alloc as the C++ impl
                    person.interests = std.ArrayList([]const u8).init(alloc);
                    isset.interests = true;
                    try person.interests.?.ensureTotalCapacity(list_meta.size);
                    for (0..list_meta.size) |_| {
                        const item = try p.readBinary(alloc);
                        // I guess? if append fails freeing wouldn't happen otherwise.
                        errdefer alloc.free(item);
                        try person.interests.?.append(item);
                    }
                    try p.readListEnd();
                    return;
                } 
                continue :sw .default;
            },
            .pets => {
                if (field.tp == TType.LIST) {
                    const list_meta = try p.readListBegin();
                    person.pets = std.ArrayList(Animal).init(alloc);
                    isset.pets = true;
                    try person.pets.?.ensureTotalCapacity(list_meta.size);
                    for (0..list_meta.size) |_| {
                        if (Animal.read(p)) |animal| {
                            try person.pets.?.append(animal);
                        } else |err| {
                            switch (err) {
                                error.CantParseUnion, error.RequiredFieldMissing => {},
                                else => |err2| return err2,
                            }
                        }
                    }
                    try p.readListEnd();
                    return;
                } 
                continue :sw .default;
            },
            .socks => {
                if (field.tp == TType.LIST) {
                    const list_meta = try p.readListBegin();
                    person.socks = std.ArrayList(Sock).init(alloc);
                    isset.socks = true;
                    try person.socks.?.ensureTotalCapacity(list_meta.size);
                    for (0..list_meta.size) |_| {
                        if (Sock.read(p)) |sock| {
                            try person.socks.?.append(sock);
                        } else |err| switch (err) {
                            ThriftError.CantParseUnion, ThriftError.RequiredFieldMissing => {},
                            else => |err2| return err2,
                        }
                    }
                    try p.readListEnd();
                    return;
                } 
                continue :sw .default;
            },
            .default => try p.skip(field.tp),
            else => continue :sw .default,
        }
    }


    pub fn read(p: *Parser, alloc: std.mem.Allocator) CompactProtocolError!ComplexPerson {
        var person = ComplexPerson{
            .userName = undefined,
            .favoriteNumber = null,
            .interests = null,
            .pets = null,
            .socks = null,
        };
        var isset: Isset = .{};
        errdefer {
            // TODO: do we care about recursively freeing stuff like 'interests'?
            if (isset.userName) alloc.free(person.userName);
            if (isset.interests) {
                for (person.interests.?.items) |item| {
                    alloc.free(item);
                }
                person.interests.?.deinit();
            }
            if (isset.pets) person.pets.?.deinit();
            if (isset.socks) person.socks.?.deinit();
        }
        try p.readStructBegin();
        while (try readFieldOrStop(p)) |field| {
            try person.readComplexPersonField(p, &isset, alloc, field );
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
    try person.interests.?.appendSlice(&[_][]const u8{"programming", "music", "travel"});
    try person.pets.?.appendSlice(&[_]Animal{.{
        .age_of_dog = 5
    }, .{
        .number_of_fish = 10
    }});
    try person.socks.?.appendSlice(&[_]Sock{
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
    try std.testing.expectEqual(person.interests.?.items.len, person_read.interests.?.items.len);
    for (person.interests.?.items, person_read.interests.?.items) |item, other| {
        try std.testing.expectEqualStrings(item, other);
    }
    try std.testing.expectEqualSlices(Animal, person.pets.?.items, person_read.pets.?.items);
    try std.testing.expectEqualSlices(Sock, person.socks.?.items, person_read.socks.?.items);

}

// test "fail" {
//     std.debug.assert(false);
// }