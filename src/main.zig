const std = @import("std");
const TCompactProtocol = @import("TCompactProtocol.zig");
const Parser = TCompactProtocol.Reader;
//const Writer = @import("TCompactProtocolWriter.zig");
const ComplexPersonModule = @import("complex_person.zig");

pub fn main() !void {

}

test {
    std.testing.refAllDeclsRecursive(@This());
    //std.debug.assert(false);
}