const std = @import("std");
const TCompactProtocol = @import("TCompactProtocol.zig");
const Writer = TCompactProtocol.Writer;
const Reader = TCompactProtocol.Reader;
const TType = TCompactProtocol.TType;
const FieldMeta = TCompactProtocol.FieldMeta;
const WriterError = Writer.WriterError;
const CompactProtocolError = Reader.CompactProtocolError || error{ NotImplemented, WriteFailed };
const ThriftError = Reader.ThriftError;
const Meta = @import("Meta.zig");

const p = @import("parquet.zig");

test "fuzz parquet" {
    const Context = struct {
        //arena: *std.heap.ArenaAllocator,

        fn testOneWithErrors(input: []const u8) (CompactProtocolError || ThriftError)!void {
            var buf: [10000000]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buf);
            const alloc = fba.allocator();

            // Read the struct from
            const fmd = blk: {
                var r: Reader = undefined;
                r.init(.fixed(input));

                break :blk try Meta.structRead(p.FileMetaData, alloc, &r);
            };

            const fmd_serialized = blk: {
                var w: Writer = undefined;
                w.init(.fixed(alloc.alloc(u8, 2 * input.len) catch unreachable));

                try Meta.structWrite(p.FileMetaData, fmd, &w);
                break :blk w.writer.buffered();
            };

            const fmd_deserialized = blk: {
                var r: Reader = undefined;
                r.init(.fixed(fmd_serialized));

                // Reading must work since we just serialized it:
                break :blk Meta.structRead(p.FileMetaData, alloc, &r) catch unreachable;
            };

            // TODO capacity...
            std.testing.expectEqualDeep(fmd, fmd_deserialized) catch unreachable;
        }

        fn testOne(context: @This(), input: []const u8) !void {
            _ = context;
            testOneWithErrors(input) catch |err| switch (err) {
                ThriftError.CantParseUnion, ThriftError.RequiredFieldMissing, CompactProtocolError.EndOfStream, CompactProtocolError.InvalidCType, CompactProtocolError.InvalidState, CompactProtocolError.Overflow, CompactProtocolError.ReadFailed => {},
                CompactProtocolError.NotImplemented, CompactProtocolError.WriteFailed => unreachable,
                else => unreachable,
            };
        }
    };

    //var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    std.testing.fuzz(Context{}, Context.testOne, .{}) catch |err| switch (err) {
        ThriftError.CantParseUnion, ThriftError.RequiredFieldMissing, CompactProtocolError.EndOfStream, CompactProtocolError.InvalidCType, CompactProtocolError.InvalidState, CompactProtocolError.Overflow, CompactProtocolError.ReadFailed => {},
        else => |err2| return err2,
    };
}
