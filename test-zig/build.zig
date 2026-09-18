const std = @import("std");

pub fn build(b: *std.Build) void {
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseSmall });
    const target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .wasi });

    const sdk = b.dependency("sdk_zig", .{ .optimize = optimize });

    const exe = b.addExecutable(.{
        .name = "test-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("module.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "mdb", .module = sdk.module("mdb") }},
        }),
    });
    exe.entry = .disabled;
    exe.stack_size = 1024 * 1024;
    exe.rdynamic = true;
    b.installArtifact(exe);
}
