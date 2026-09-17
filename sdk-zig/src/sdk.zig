const std = @import("std");
const abi = @import("abi.zig");

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

pub fn errorMessage() []const u8 {
    return abi.getVal();
}

pub fn isNotFound(err: anyerror) bool {
    return err == Error.NotFound;
}

pub fn isNotExist(err: anyerror) bool {
    return err == Error.NotExist;
}

pub fn begin(flags: u32) Error!Txn {
    abi.txn_id = 0;
    abi.exp_flg = flags;
    abi.__lmdb_begin();
    try abi.check();
    return .{ .id = abi.txn_id };
}

pub fn view(func: anytype) anyerror!void {
    const txn = try begin(readonly);
    defer txn.abort();
    return func(txn);
}

pub fn update(func: anytype) anyerror!void {
    const txn = try begin(0);
    if (func(txn)) |_| {
        try txn.commit();
    } else |err| {
        txn.abort();
        return err;
    }
}

pub const Txn = struct {
    id: u32,

    pub fn createDBI(t: Txn, name: []const u8, flags: u32) Error!DBI {
        return t.openDBI(name, flags | create);
    }

    pub fn openDBI(t: Txn, name: []const u8, flags: u32) Error!DBI {
        abi.txn_id = t.id;
        abi.exp_flg = flags;
        abi.setKey(name);
        abi.__lmdb_db_open();
        try abi.check();
        return abi.exp_dbi;
    }

    pub fn drop(t: Txn, dbi: DBI) Error!void {
        abi.txn_id = t.id;
        abi.exp_dbi = dbi;
        abi.__lmdb_db_drop();
        try abi.check();
    }

    pub fn stat(t: Txn, dbi: DBI) Error!Stat {
        abi.txn_id = t.id;
        abi.exp_dbi = dbi;
        abi.__lmdb_db_stat();
        try abi.check();
        return Stat.fromBytes(abi.getVal()[0..48]);
    }

    pub fn put(t: Txn, dbi: DBI, k: []const u8, v: []const u8, flags: u32) Error!void {
        abi.txn_id = t.id;
        abi.exp_dbi = dbi;
        abi.exp_flg = flags;
        abi.setKey(k);
        abi.setVal(v);
        abi.__lmdb_put();
        try abi.check();
    }

    pub fn get(t: Txn, dbi: DBI, k: []const u8) Error![]const u8 {
        abi.txn_id = t.id;
        abi.exp_dbi = dbi;
        abi.setKey(k);
        abi.__lmdb_get();
        try abi.check();
        return abi.getVal();
    }

    pub fn del(t: Txn, dbi: DBI, k: []const u8, v: []const u8) Error!void {
        abi.txn_id = t.id;
        abi.exp_dbi = dbi;
        abi.setKey(k);
        abi.setVal(v);
        abi.__lmdb_del();
        try abi.check();
    }

    pub fn openCursor(t: Txn, dbi: DBI) Error!Cursor {
        abi.txn_id = t.id;
        abi.exp_dbi = dbi;
        abi.__lmdb_cursor_open();
        try abi.check();
        return .{ .id = abi.cur_id };
    }

    pub fn commit(t: Txn) Error!void {
        abi.txn_id = t.id;
        abi.__lmdb_commit();
        defer abi.txn_id = 0;
        try abi.check();
    }

    pub fn abort(t: Txn) void {
        abi.txn_id = t.id;
        abi.__lmdb_abort();
        abi.txn_id = 0;
    }

    pub fn sub(t: Txn, func: anytype) anyerror!void {
        abi.txn_id = t.id;
        abi.__lmdb_begin();
        try abi.check();
        const child = Txn{ .id = abi.txn_id };
        defer abi.txn_id = t.id;
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

    pub fn get(c: Cursor, k: []const u8, v: []const u8, flags: u32) Error!Entry {
        abi.cur_id = c.id;
        abi.exp_flg = flags;
        abi.setKey(k);
        abi.setVal(v);
        abi.__lmdb_cursor_get();
        try abi.check();
        return .{ .key = abi.getKey(), .val = abi.getVal() };
    }

    pub fn put(c: Cursor, k: []const u8, v: []const u8, flags: u32) Error!void {
        abi.cur_id = c.id;
        abi.exp_flg = flags;
        abi.setKey(k);
        abi.setVal(v);
        abi.__lmdb_cursor_put();
        try abi.check();
    }

    pub fn del(c: Cursor, flags: u32) Error!void {
        abi.cur_id = c.id;
        abi.exp_flg = flags;
        abi.__lmdb_cursor_del();
        try abi.check();
    }

    pub fn close(c: Cursor) void {
        abi.cur_id = c.id;
        abi.__lmdb_cursor_close();
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
