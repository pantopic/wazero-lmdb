const std = @import("std");
const sdk = @import("sdk.zig");

pub const key_cap_bytes: u32 = 511;
pub const val_cap_bytes: u32 = 1536 * 1024;

pub var meta: [11]u32 = undefined;

pub var key_cap: u32 = key_cap_bytes;
pub var key_len: u32 = 0;
pub var key_buf: [key_cap_bytes]u8 = undefined;

pub var val_cap: u32 = val_cap_bytes;
pub var val_len: u32 = 0;
pub var val_buf: [val_cap_bytes]u8 = undefined;

pub var txn_id: u32 = 0;
pub var exp_dbi: sdk.DBI = 0;
pub var cur_id: u32 = 0;
pub var exp_flg: u32 = 0;
pub var err_code: u32 = 0;

export fn __lmdb() u32 {
    meta[0] = @intCast(@intFromPtr(&key_cap));
    meta[1] = @intCast(@intFromPtr(&key_len));
    meta[2] = @intCast(@intFromPtr(&key_buf[0]));
    meta[3] = @intCast(@intFromPtr(&val_cap));
    meta[4] = @intCast(@intFromPtr(&val_len));
    meta[5] = @intCast(@intFromPtr(&val_buf[0]));
    meta[6] = @intCast(@intFromPtr(&txn_id));
    meta[7] = @intCast(@intFromPtr(&exp_dbi));
    meta[8] = @intCast(@intFromPtr(&cur_id));
    meta[9] = @intCast(@intFromPtr(&exp_flg));
    meta[10] = @intCast(@intFromPtr(&err_code));
    return @intCast(@intFromPtr(&meta[0]));
}

pub fn setKey(k: []const u8) void {
    @memcpy(key_buf[0..k.len], k);
    key_len = @intCast(k.len);
}

pub fn getKey() []const u8 {
    return key_buf[0..key_len];
}

pub fn setVal(v: []const u8) void {
    @memcpy(val_buf[0..v.len], v);
    val_len = @intCast(v.len);
}

pub fn getVal() []const u8 {
    return val_buf[0..val_len];
}

pub fn check() sdk.Error!void {
    return switch (err_code) {
        0 => {},
        1 => sdk.Error.KeyExist,
        2 => sdk.Error.NotFound,
        3 => sdk.Error.PageNotFound,
        4 => sdk.Error.Corrupted,
        5 => sdk.Error.Panic,
        6 => sdk.Error.VersionMismatch,
        7 => sdk.Error.Invalid,
        8 => sdk.Error.MapFull,
        9 => sdk.Error.DBsFull,
        10 => sdk.Error.ReadersFull,
        11 => sdk.Error.TLSFull,
        12 => sdk.Error.TxnFull,
        13 => sdk.Error.CursorFull,
        14 => sdk.Error.PageFull,
        15 => sdk.Error.MapResized,
        16 => sdk.Error.Incompatible,
        17 => sdk.Error.BadRSlot,
        18 => sdk.Error.BadTxn,
        19 => sdk.Error.BadValSize,
        20 => sdk.Error.BadDBI,
        21 => sdk.Error.Exist,
        22 => sdk.Error.NotExist,
        23 => sdk.Error.Permission,
        else => sdk.Error.Unknown,
    };
}

test "stat round trip" {
    const s = sdk.Stat{
        .p_size = 4096,
        .depth = 3,
        .branch_pages = 7,
        .leaf_pages = 42,
        .overflow_pages = 1,
        .entries = 1000,
    };
    var b: [48]u8 = undefined;
    s.toBytes(&b);
    try std.testing.expectEqual(s, sdk.Stat.fromBytes(&b));
}

// Host module imports
pub extern "pantopic/wazero-lmdb" fn __lmdb_begin() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_db_open() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_db_stat() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_db_drop() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_commit() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_abort() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_put() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_get() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_del() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_cursor_open() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_cursor_get() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_cursor_put() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_cursor_del() void;
pub extern "pantopic/wazero-lmdb" fn __lmdb_cursor_close() void;
