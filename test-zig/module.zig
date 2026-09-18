//! Test guest module for the Zig SDK

const std = @import("std");
const mdb = @import("mdb");

var txn: mdb.Txn = .{ .id = 0 };
var cur: mdb.Cursor = .{ .id = 0 };
var dbi: mdb.DBI = 0;
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
    txn = mdb.begin(0) catch |err| fatal(err);
}

export fn beginread() void {
    txn = mdb.begin(mdb.readonly) catch |err| fatal(err);
}

export fn db() void {
    dbi = txn.openDBI("test", mdb.create) catch |err| fatal(err);
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
        if (!mdb.isNotFound(err)) fatal(err);
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

fn updateFn(t: mdb.Txn) anyerror!void {
    t.put(dbi, "b", "22", 0) catch {};
}

export fn update() void {
    mdb.update(updateFn) catch |err| fatal(err);
}

fn updatefailFn(t: mdb.Txn) anyerror!void {
    t.put(dbi, "b", "222", 0) catch {};
    return error.ICantBelieveYouveDoneThis;
}

export fn updatefail() void {
    if (mdb.update(updatefailFn)) |_| {
        @panic("Error missing");
    } else |_| {}
}

fn viewFn(t: mdb.Txn) anyerror!void {
    const val = try t.get(dbi, "b");
    if (!std.mem.eql(u8, val, "22")) return error.WrongValue;
}

export fn view() void {
    mdb.view(viewFn) catch |err| fatal(err);
}

fn clearFn(t: mdb.Txn) anyerror!void {
    dbi = t.openDBI("test", mdb.create) catch |err| fatal(err);
    return t.drop(dbi);
}

export fn clear() void {
    mdb.update(clearFn) catch |err| fatal(err);
}

fn subPut(t: mdb.Txn) anyerror!void {
    return t.put(dbi, "sub", "txn", 0);
}

fn subFn(t: mdb.Txn) anyerror!void {
    return t.sub(subPut);
}

export fn sub() void {
    mdb.update(subFn) catch |err| fatal(err);
}

fn subPutFail(t: mdb.Txn) anyerror!void {
    t.put(dbi, "sub", "txn", 0) catch {};
    return error.ICantBelieveYouveDoneThis;
}

fn subabortFn(t: mdb.Txn) anyerror!void {
    t.sub(subPutFail) catch {};
}

export fn subabort() void {
    mdb.update(subabortFn) catch |err| fatal(err);
}

fn subDel(t: mdb.Txn) anyerror!void {
    return t.del(dbi, "sub", "");
}

fn subdelFn(t: mdb.Txn) anyerror!void {
    return t.sub(subDel);
}

export fn subdel() void {
    mdb.update(subdelFn) catch |err| fatal(err);
}

export fn stress(limit: u32) void {
    txn = mdb.begin(0) catch |err| fatal(err);
    dbi = txn.openDBI("test", mdb.create) catch |err| fatal(err);
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
    dbi = txn.openDBI("test", mdb.create) catch |err| fatal(err);
    cur = txn.openCursor(dbi) catch |err| fatal(err);
}

export fn cursorfirst() void {
    const e = cur.get("", "", mdb.op_first) catch |err| fatal(err);
    if (!std.mem.eql(u8, e.key, "b")) @panic("wrong key");
    if (!std.mem.eql(u8, e.val, "22")) @panic("wrong value");
}

export fn cursorput() void {
    cur.put("c", "3", 0) catch |err| fatal(err);
}

export fn cursorcurrent() void {
    const e = cur.get("", "", mdb.op_get_current) catch |err| fatal(err);
    if (!std.mem.eql(u8, e.key, "c")) @panic("wrong key");
    if (!std.mem.eql(u8, e.val, "3")) @panic("wrong value");
}

export fn cursornext() void {
    const e = cur.get("", "", mdb.op_next) catch |err| fatal(err);
    if (!std.mem.eql(u8, e.key, "c")) @panic("wrong key");
    if (!std.mem.eql(u8, e.val, "3")) @panic("wrong value");
}

export fn cursordel() void {
    cur.del(mdb.current) catch |err| fatal(err);
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
