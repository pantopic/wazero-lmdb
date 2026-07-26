//! Test guest module for the Zig SDK, mirroring test/module.go.

const std = @import("std");
const lmdb = @import("lmdb");

var txn: lmdb.Txn = .{ .id = 0 };
var cur: lmdb.Cursor = .{ .id = 0 };
var dbi: lmdb.DBI = 0;
var k: [16]u8 = undefined;
var v: [16]u8 = undefined;
var statbuf: [48]u8 = undefined;

fn fatal(err: anyerror) noreturn {
    @panic(@errorName(err));
}

export fn memcheck() u32 {
    // No runtime heap allocation in this module; the SDK uses static buffers.
    return 0;
}

export fn begin() void {
    txn = lmdb.begin(0) catch |err| fatal(err);
}

export fn beginread() void {
    txn = lmdb.begin(lmdb.readonly) catch |err| fatal(err);
}

export fn db() void {
    dbi = txn.openDBI("test", lmdb.create) catch |err| fatal(err);
}

export fn dbstat() u64 {
    const s = txn.stat(dbi) catch |err| fatal(err);
    s.toBytes(&statbuf);
    return (@as(u64, @intCast(@intFromPtr(&statbuf[0]))) << 32) + statbuf.len;
}

export fn dbdrop() void {
    txn.drop(dbi) catch |err| fatal(err);
}

export fn set() void {
    txn.put(dbi, "a", "1", 0) catch |err| fatal(err);
}

export fn get() void {
    const val = txn.get(dbi, "a") catch |err| fatal(err);
    if (val.len > 1 or val[0] != '1') @panic("wrong value");
}

export fn getmissing() void {
    if (txn.get(dbi, "ddd")) |_| {
        @panic("error not returned");
    } else |err| {
        if (!lmdb.isNotFound(err)) fatal(err);
    }
}

export fn del() void {
    txn.del(dbi, "a", "") catch |err| fatal(err);
}

export fn commit() void {
    txn.commit() catch |err| fatal(err);
}

export fn set2() void {
    txn.put(dbi, "b", "2", 0) catch |err| fatal(err);
}

export fn get2() void {
    const val = txn.get(dbi, "b") catch |err| fatal(err);
    if (!std.mem.eql(u8, val, "2")) @panic("wrong value");
}

fn updateFn(t: lmdb.Txn) anyerror!void {
    t.put(dbi, "b", "22", 0) catch {};
}

export fn update() void {
    lmdb.update(updateFn) catch |err| fatal(err);
}

fn updatefailFn(t: lmdb.Txn) anyerror!void {
    t.put(dbi, "b", "222", 0) catch {};
    return error.ICantBelieveYouveDoneThis;
}

export fn updatefail() void {
    if (lmdb.update(updatefailFn)) |_| {
        @panic("Error missing");
    } else |_| {}
}

fn viewFn(t: lmdb.Txn) anyerror!void {
    const val = try t.get(dbi, "b");
    if (!std.mem.eql(u8, val, "22")) return error.WrongValue;
}

export fn view() void {
    lmdb.view(viewFn) catch |err| fatal(err);
}

fn clearFn(t: lmdb.Txn) anyerror!void {
    dbi = t.openDBI("test", lmdb.create) catch |err| fatal(err);
    return t.drop(dbi);
}

export fn clear() void {
    lmdb.update(clearFn) catch |err| fatal(err);
}

fn subPut(t: lmdb.Txn) anyerror!void {
    return t.put(dbi, "sub", "txn", 0);
}

fn subFn(t: lmdb.Txn) anyerror!void {
    return t.sub(subPut);
}

export fn sub() void {
    lmdb.update(subFn) catch |err| fatal(err);
}

fn subPutFail(t: lmdb.Txn) anyerror!void {
    t.put(dbi, "sub", "txn", 0) catch {};
    return error.ICantBelieveYouveDoneThis;
}

fn subabortFn(t: lmdb.Txn) anyerror!void {
    t.sub(subPutFail) catch {};
}

export fn subabort() void {
    lmdb.update(subabortFn) catch |err| fatal(err);
}

fn subDel(t: lmdb.Txn) anyerror!void {
    return t.del(dbi, "sub", "");
}

fn subdelFn(t: lmdb.Txn) anyerror!void {
    return t.sub(subDel);
}

export fn subdel() void {
    lmdb.update(subdelFn) catch |err| fatal(err);
}

export fn stress(limit: u32) void {
    txn = lmdb.begin(0) catch |err| fatal(err);
    dbi = txn.openDBI("test", lmdb.create) catch |err| fatal(err);
    const n: u64 = limit;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        std.mem.writeInt(u64, k[0..8], i + 1_000_000_000_000_000, .little);
        std.mem.writeInt(u64, v[0..8], n - i + 1_000_000_000_000_000, .little);
        txn.put(dbi, k[0..8], v[0..8], 0) catch |err| fatal(err);
    }
    txn.commit() catch |err| fatal(err);
}

export fn abort() void {
    txn.abort();
}

export fn cursoropen() void {
    dbi = txn.openDBI("test", lmdb.create) catch |err| fatal(err);
    cur = txn.openCursor(dbi) catch |err| fatal(err);
}

export fn cursorfirst() void {
    const e = cur.get("", "", lmdb.op_first) catch |err| fatal(err);
    if (!std.mem.eql(u8, e.key, "b")) @panic("wrong key");
    if (!std.mem.eql(u8, e.val, "22")) @panic("wrong value");
}

export fn cursorput() void {
    cur.put("c", "3", 0) catch |err| fatal(err);
}

export fn cursorcurrent() void {
    const e = cur.get("", "", lmdb.op_get_current) catch |err| fatal(err);
    if (!std.mem.eql(u8, e.key, "c")) @panic("wrong key");
    if (!std.mem.eql(u8, e.val, "3")) @panic("wrong value");
}

export fn cursornext() void {
    const e = cur.get("", "", lmdb.op_next) catch |err| fatal(err);
    if (!std.mem.eql(u8, e.key, "c")) @panic("wrong key");
    if (!std.mem.eql(u8, e.val, "3")) @panic("wrong value");
}

export fn cursordel() void {
    cur.del(lmdb.current) catch |err| fatal(err);
}

export fn cursorclose() void {
    cur.close();
}

export fn valptrs2() u64 {
    return (@as(u64, @intCast(@intFromPtr(&k[0]))) << 32) + @as(u64, @intCast(@intFromPtr(&v[0])));
}

export fn setval() void {
    txn.put(dbi, k[0..16], v[0..16], 0) catch |err| fatal(err);
}
