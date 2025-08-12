const std = @import("std");
const Parser = @import("TCompactProtocolReader.zig");
//const Writer = @import("TCompactProtocolWriter.zig");
const ComplexPersonModule = @import("complex_person.zig");

pub fn main() !void {

}

test {
    std.testing.refAllDeclsRecursive(@This());
    //std.debug.assert(false);
}