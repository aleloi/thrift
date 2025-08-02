const std = @import("std");
const Parser = @import("TCompactProtocol.zig");

const Person = struct { userName: []const u8, favoriteNumber: i64, interests: std.ArrayList([]const u8) };

//const SomeErrorDunno = error{UnexpectedEndOfData};

fn parsePerson(p: *Parser, alloc: std.mem.Allocator) Parser.ParseError!Person {
    _ = alloc;
    try p.readStructBegin();
    const tt = try p.readFieldBegin();
    var res: Person = .{};
    if (tt.tp == .STOP) {
        // TODO validation here;
        return res;
    }
    std.debug.print("{}\n", .{tt});
    return Parser.ParseError.NotImplemented;
}

pub fn main() !void {
    const Alloc: type = std.heap.DebugAllocator(.{});
    var alloc: Alloc = Alloc.init;
    const alist = std.ArrayList([]const u8).init(alloc.allocator());
    _ = Person{ .userName = "hej", .favoriteNumber = 0, .interests = alist };
    const reader = std.Io.Reader.fixed("\x18\x06");
    //const data: [2]u8 = .{ 0x18, 0x06 };
    var parser = Parser{ .reader = reader };
    const p = try parsePerson(&parser, alloc.allocator());
    std.debug.print("{}\n", .{p});
    if (false) {
        parsePerson.wtf_functions_cant_have_functions();
    }
}
