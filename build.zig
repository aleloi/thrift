const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "my_thrift",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/Meta.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{},
        }),
    });

    b.installArtifact(exe);

    const check = b.step("check", "Check if it compiles");
    check.dependOn(&exe.step);

    // const exe_tests = b.addTest(.{
    //     .root_module = exe.root_module,
    // });

    const test_step = b.step("test", "Run tests");

    const test_files: []const []const u8 = &.{
        "src/fuzz_parquet.zig",
        //"src/complex_person.zig",
        //"src/main.zig",
        //"src/Meta.zig",
        //"src/TCompactProtocol.zig",
    };
    for (test_files) |test_file| {
        const test_set = b.addTest(.{
            .root_module = b.createModule(.{ .root_source_file = b.path(test_file), .target = target, .optimize = optimize }),
            //.target = target,
            //.optimize = optimize,
        });
        //test_set.root_module.addImport("rope", rope_mod);
        var run_test_set = b.addRunArtifact(test_set);
        run_test_set.has_side_effects = true;
        test_step.dependOn(&run_test_set.step);
    }

    // const run_exe_tests = b.addRunArtifact(exe_tests);
    // test_step.dependOn(&run_exe_tests.step);
}
