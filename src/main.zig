const std = @import("std");
const Parser = @import("TCompactProtocol.zig");
const PersonModule = @import("person.zig");
const ComplexPersonModule = @import("complex_person.zig");

pub fn main() !void {
    const Alloc: type = std.heap.DebugAllocator(.{});
    var alloc: Alloc = Alloc.init;
    const alist = std.ArrayList([]const u8).init(alloc.allocator());
    _ = PersonModule.Person{ .userName = "hej", .favoriteNumber = 0, .interests = alist };
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
    const p = try PersonModule.parsePerson(&parser, alloc.allocator());
    std.debug.print(" {} ", .{p});
    if (false) {
        //parsePerson.wtf_functions_cant_have_functions();
    }
}

test {
    std.testing.refAllDeclsRecursive(@This());
    //std.debug.assert(false);
}