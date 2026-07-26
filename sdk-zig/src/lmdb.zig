//! Guest-side SDK for the pantopic/wazero-lmdb host module.
//!
//! Functionally identical to sdk-go: the guest exposes an `__lmdb` export
//! returning a pointer to a table of pointers to static exchange buffers
//! (key, val) and scalar cells (txn, dbi, cursor, flags, error). Host calls
//! communicate exclusively through those buffers.
//!
//! Slices returned by `Txn.get`, `Cursor.get` and `errorMessage` point into
//! the shared static buffers and are only valid until the next SDK call.

const std = @import("std");

// LMDB multiuse flags
// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#pkg-constants
pub const dup_sort: u32 = 0x00004;
pub const current: u32 = 0x00040;
pub const readonly: u32 = 0x20000;
pub const create: u32 = 0x40000;
pub const no_readahead: u32 = 0x800000;

pub const no_dup_data: u32 = 0x20; // Store the key-value pair only if key is not present (DupSort).
pub const no_overwrite: u32 = 0x10; // Store a new key-value pair only if key is not present.
pub const append: u32 = 0x20000; // Append an item to the database.
pub const append_dup: u32 = 0x40000; // Append an item to the database (DupSort).

// LMDB cursor operations
// https://github.com/PowerDNS/lmdb-go/blob/v1.9.3/lmdb/cursor.go#L16
pub const op_first: u32 = 0;
pub const op_first_dup: u32 = 1;
pub const op_get_both: u32 = 2;
pub const op_get_both_range: u32 = 3;
pub const op_get_current: u32 = 4;
pub const op_get_multiple: u32 = 5;
pub const op_last: u32 = 6;
pub const op_last_dup: u32 = 7;
pub const op_next: u32 = 8;
pub const op_next_dup: u32 = 9;
pub const op_next_multiple: u32 = 10;
pub const op_next_no_dup: u32 = 11;
pub const op_prev: u32 = 12;
pub const op_prev_dup: u32 = 13;
pub const op_prev_no_dup: u32 = 14;
pub const op_set: u32 = 15;
pub const op_set_key: u32 = 16;
pub const op_set_range: u32 = 17;

pub const DBI = u32;

/// Error codes written by the host, in the same order as sdk-go's Errno.
pub const Error = error{
    KeyExist,
    NotFound,
    PageNotFound,
    Corrupted,
    Panic,
    VersionMismatch,
    Invalid,
    MapFull,
    DBsFull,
    ReadersFull,
    TLSFull,
    TxnFull,
    CursorFull,
    PageFull,
    MapResized,
    Incompatible,
    BadRSlot,
    BadTxn,
    BadValSize,
    BadDBI,
    Exist,
    NotExist,
    Permission,
    Unknown,
};

/// The host writes the error message into the val buffer alongside the error
/// code. Valid until the next SDK call.
pub fn errorMessage() []const u8 {
    return getVal();
}

pub fn isNotFound(err: anyerror) bool {
    return err == Error.NotFound;
}

pub fn isNotExist(err: anyerror) bool {
    return err == Error.NotExist;
}

pub fn begin(flags: u32) Error!Txn {
    txn_id = 0;
    exp_flg = flags;
    __lmdb_begin();
    try check();
    return .{ .id = txn_id };
}

/// Runs `func` (any callable accepting a `Txn` and returning `!void`) in a
/// read-only transaction which is always aborted afterward.
pub fn view(func: anytype) anyerror!void {
    const txn = try begin(readonly);
    defer txn.abort();
    return func(txn);
}

/// Runs `func` in a read-write transaction, committing on success and
/// aborting on error.
pub fn update(func: anytype) anyerror!void {
    const txn = try begin(0);
    if (func(txn)) |_| {
        try txn.commit();
    } else |err| {
        txn.abort();
        return err;
    }
}

/// Txn represents an LMDB transaction
/// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#Txn
pub const Txn = struct {
    id: u32,

    pub fn createDBI(t: Txn, name: []const u8, flags: u32) Error!DBI {
        return t.openDBI(name, flags | create);
    }

    pub fn openDBI(t: Txn, name: []const u8, flags: u32) Error!DBI {
        txn_id = t.id;
        exp_flg = flags;
        setKey(name);
        __lmdb_db_open();
        try check();
        return exp_dbi;
    }

    pub fn drop(t: Txn, dbi: DBI) Error!void {
        txn_id = t.id;
        exp_dbi = dbi;
        __lmdb_db_drop();
        try check();
    }

    pub fn stat(t: Txn, dbi: DBI) Error!Stat {
        txn_id = t.id;
        exp_dbi = dbi;
        __lmdb_db_stat();
        try check();
        return Stat.fromBytes(getVal()[0..48]);
    }

    pub fn put(t: Txn, dbi: DBI, k: []const u8, v: []const u8, flags: u32) Error!void {
        txn_id = t.id;
        exp_dbi = dbi;
        exp_flg = flags;
        setKey(k);
        setVal(v);
        __lmdb_put();
        try check();
    }

    /// Returns a slice into the shared val buffer, valid until the next SDK call.
    pub fn get(t: Txn, dbi: DBI, k: []const u8) Error![]const u8 {
        txn_id = t.id;
        exp_dbi = dbi;
        setKey(k);
        __lmdb_get();
        try check();
        return getVal();
    }

    pub fn del(t: Txn, dbi: DBI, k: []const u8, v: []const u8) Error!void {
        txn_id = t.id;
        exp_dbi = dbi;
        setKey(k);
        setVal(v);
        __lmdb_del();
        try check();
    }

    pub fn openCursor(t: Txn, dbi: DBI) Error!Cursor {
        txn_id = t.id;
        exp_dbi = dbi;
        __lmdb_cursor_open();
        try check();
        return .{ .id = cur_id };
    }

    pub fn commit(t: Txn) Error!void {
        txn_id = t.id;
        __lmdb_commit();
        defer txn_id = 0;
        try check();
    }

    pub fn abort(t: Txn) void {
        txn_id = t.id;
        __lmdb_abort();
        txn_id = 0;
    }

    /// Runs `func` in a nested transaction, committing on success and
    /// aborting on error.
    pub fn sub(t: Txn, func: anytype) anyerror!void {
        txn_id = t.id;
        __lmdb_begin();
        try check();
        const child = Txn{ .id = txn_id };
        defer txn_id = t.id;
        if (func(child)) |_| {
            try child.commit();
        } else |err| {
            child.abort();
            return err;
        }
    }
};

/// Cursor represents an LMDB cursor
/// See https://pkg.go.dev/github.com/PowerDNS/lmdb-go/lmdb#Cursor
pub const Cursor = struct {
    id: u32,

    pub const Entry = struct {
        key: []const u8,
        val: []const u8,
    };

    /// Returns slices into the shared key/val buffers, valid until the next SDK call.
    pub fn get(c: Cursor, k: []const u8, v: []const u8, flags: u32) Error!Entry {
        cur_id = c.id;
        exp_flg = flags;
        setKey(k);
        setVal(v);
        __lmdb_cursor_get();
        try check();
        return .{ .key = getKey(), .val = getVal() };
    }

    pub fn put(c: Cursor, k: []const u8, v: []const u8, flags: u32) Error!void {
        cur_id = c.id;
        exp_flg = flags;
        setKey(k);
        setVal(v);
        __lmdb_cursor_put();
        try check();
    }

    pub fn del(c: Cursor, flags: u32) Error!void {
        cur_id = c.id;
        exp_flg = flags;
        __lmdb_cursor_del();
        try check();
    }

    pub fn close(c: Cursor) void {
        cur_id = c.id;
        __lmdb_cursor_close();
    }
};

pub const Stat = struct {
    p_size: u64, // Size of a database page. This is currently the same for all databases.
    depth: u64, // Depth (height) of the B-tree
    branch_pages: u64, // Number of internal (non-leaf) pages
    leaf_pages: u64, // Number of leaf pages
    overflow_pages: u64, // Number of overflow pages
    entries: u64, // Number of data items

    pub fn fromBytes(b: *const [48]u8) Stat {
        return .{
            .p_size = std.mem.readInt(u64, b[0..8], .little),
            .depth = std.mem.readInt(u64, b[8..16], .little),
            .branch_pages = std.mem.readInt(u64, b[16..24], .little),
            .leaf_pages = std.mem.readInt(u64, b[24..32], .little),
            .overflow_pages = std.mem.readInt(u64, b[32..40], .little),
            .entries = std.mem.readInt(u64, b[40..48], .little),
        };
    }

    pub fn toBytes(s: Stat, b: *[48]u8) void {
        std.mem.writeInt(u64, b[0..8], s.p_size, .little);
        std.mem.writeInt(u64, b[8..16], s.depth, .little);
        std.mem.writeInt(u64, b[16..24], s.branch_pages, .little);
        std.mem.writeInt(u64, b[24..32], s.leaf_pages, .little);
        std.mem.writeInt(u64, b[32..40], s.overflow_pages, .little);
        std.mem.writeInt(u64, b[40..48], s.entries, .little);
    }
};

// ------------------------------------------------------------------------
// ABI: static exchange buffers shared with the host.

const key_cap_bytes: u32 = 511;
const val_cap_bytes: u32 = 1536 * 1024;

var meta: [11]u32 = undefined;

var key_cap: u32 = key_cap_bytes;
var key_len: u32 = 0;
var key_buf: [key_cap_bytes]u8 = undefined;

var val_cap: u32 = val_cap_bytes;
var val_len: u32 = 0;
var val_buf: [val_cap_bytes]u8 = undefined;

var txn_id: u32 = 0;
var exp_dbi: DBI = 0;
var cur_id: u32 = 0;
var exp_flg: u32 = 0;
var err_code: u32 = 0;

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

fn setKey(k: []const u8) void {
    @memcpy(key_buf[0..k.len], k);
    key_len = @intCast(k.len);
}

fn getKey() []const u8 {
    return key_buf[0..key_len];
}

fn setVal(v: []const u8) void {
    @memcpy(val_buf[0..v.len], v);
    val_len = @intCast(v.len);
}

fn getVal() []const u8 {
    return val_buf[0..val_len];
}

fn check() Error!void {
    return switch (err_code) {
        0 => {},
        1 => Error.KeyExist,
        2 => Error.NotFound,
        3 => Error.PageNotFound,
        4 => Error.Corrupted,
        5 => Error.Panic,
        6 => Error.VersionMismatch,
        7 => Error.Invalid,
        8 => Error.MapFull,
        9 => Error.DBsFull,
        10 => Error.ReadersFull,
        11 => Error.TLSFull,
        12 => Error.TxnFull,
        13 => Error.CursorFull,
        14 => Error.PageFull,
        15 => Error.MapResized,
        16 => Error.Incompatible,
        17 => Error.BadRSlot,
        18 => Error.BadTxn,
        19 => Error.BadValSize,
        20 => Error.BadDBI,
        21 => Error.Exist,
        22 => Error.NotExist,
        23 => Error.Permission,
        else => Error.Unknown,
    };
}

test "stat round trip" {
    const s = Stat{
        .p_size = 4096,
        .depth = 3,
        .branch_pages = 7,
        .leaf_pages = 42,
        .overflow_pages = 1,
        .entries = 1000,
    };
    var b: [48]u8 = undefined;
    s.toBytes(&b);
    try std.testing.expectEqual(s, Stat.fromBytes(&b));
}

// Host module imports
extern "pantopic/wazero-lmdb" fn __lmdb_begin() void;
extern "pantopic/wazero-lmdb" fn __lmdb_db_open() void;
extern "pantopic/wazero-lmdb" fn __lmdb_db_stat() void;
extern "pantopic/wazero-lmdb" fn __lmdb_db_drop() void;
extern "pantopic/wazero-lmdb" fn __lmdb_commit() void;
extern "pantopic/wazero-lmdb" fn __lmdb_abort() void;
extern "pantopic/wazero-lmdb" fn __lmdb_put() void;
extern "pantopic/wazero-lmdb" fn __lmdb_get() void;
extern "pantopic/wazero-lmdb" fn __lmdb_del() void;
extern "pantopic/wazero-lmdb" fn __lmdb_cursor_open() void;
extern "pantopic/wazero-lmdb" fn __lmdb_cursor_get() void;
extern "pantopic/wazero-lmdb" fn __lmdb_cursor_put() void;
extern "pantopic/wazero-lmdb" fn __lmdb_cursor_del() void;
extern "pantopic/wazero-lmdb" fn __lmdb_cursor_close() void;
