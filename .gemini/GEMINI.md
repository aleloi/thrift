# Project: zig thrift parser
We are iteratively developing a minimal thrift parser for most of the TCompactProtocol. The aim is to be able to parse Parquet file footers, which use most of the basic types except sets and maps. It's being developed in a test-driver fashion, one read command at a time. There are no and will not be any dependencies.

The thrift white paper lists these read functions in its protocol public API:

name, type, seq = readMessageBegin()
readMessageEnd()
name = readStructBegin()
readStructEnd()
name, type, id = readFieldBegin()
readFieldEnd()
k, v, size = readMapBegin()
readMapEnd()
etype, size = readListBegin()
readListEnd()
etype, size = readSetBegin()
readSetEnd()
bool = readBool()
byte = readByte()
i16 = readI16()
i32 = readI32()
i64 = readI64()
double = readDouble()
string = readString()

We will probably need to implement all except the Map and Set ones to be able to parse parquet footer.

## General information
We use zig 0.15, in particular it's std.Io.{Reader, Writer} library. `zig` is in path. You run the tests with `zig test main.zig`. You build with `zig build` (project should always build successfully), and test with `zig test` (tests may fail when doing TDD, but must pass when doing git commit).

Zig standard library entrypoint is /Users/aleloi/.zvm/master/lib/std/std.zig

Zig language reference for the version I'm using is at ./langref.html.in at the repo root

An example python implementation is ./poc_python_thrift_parser/thrift/lib/py/src/protocol/TCompactProtocol.py at the repo root. You can use that for the semantics.

You can serialize example thrift messages by (1) creating a thrift file, (2) running `thrift --gen py:enum,type_hints $THRIFT_FILE`, then writing a script such as `temp_serializer.py` and then running it with `uv run`. Use PEP 723-style inline dependencies for e.g. thrift, and add the generated python folder path (default is gen-py) to sys.path.

Only check in zig files to git.

## Coding style

Try to emulate zig standard library style. Check e.g. /Users/aleloi/.zvm/master/lib/std/mem.zig for a good example. Thrift protocol public read API is exempt from naming rules.
