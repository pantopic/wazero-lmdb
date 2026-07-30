const std = @import("std");

pub fn build(b: *std.Build) void {
    const optimize = b.standardOptimizeOption(.{});
    const wasm_target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .wasi });

    const mod = b.addModule("lmdb", .{
        .root_source_file = b.path("src/sdk.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });

    _ = mod;

    // Unit tests run natively (they only exercise host-independent code).
    const native = b.standardTargetOptions(.{});
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/sdk.zig"),
            .target = native,
            .optimize = optimize,
        }),
    });
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);
}
