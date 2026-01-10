# ILP v4 Configuration Guide

This document describes all configuration options for the ILP v4 columnar binary protocol in QuestDB.

## Server Configuration

ILP v4 shares the same TCP port as the existing ILP text protocol (default: 9009). Protocol detection automatically distinguishes between v4 binary and text protocols.

### IlpV4ReceiverConfiguration Options

| Property | Default | Description |
|----------|---------|-------------|
| `isGorillaSupported()` | `true` | Enable Gorilla timestamp compression |
| `isLz4Supported()` | `false` | Enable LZ4 payload compression (not yet implemented) |
| `isZstdSupported()` | `false` | Enable Zstd payload compression (not yet implemented) |
| `getMaxMessageSize()` | `16 MB` | Maximum message size in bytes |
| `getMaxTablesPerMessage()` | `256` | Maximum tables per batch message |
| `getMaxRowsPerTable()` | `1,000,000` | Maximum rows per table in a message |
| `getMaxPipelinedMessages()` | `4` | Maximum in-flight (pipelined) messages |
| `isAutoCreateNewColumns()` | `true` | Auto-create columns on schema evolution |
| `isAutoCreateNewTables()` | `true` | Auto-create tables on first write |
| `getMaxFileNameLength()` | `127` | Maximum column/table name length |
| `getHandshakeTimeoutMs()` | `30,000` | Handshake timeout in milliseconds |
| `isSchemaCachingEnabled()` | `true` | Enable schema caching for reference mode |
| `getMaxCachedSchemas()` | `256` | Maximum schemas to cache per connection |
| `getRecvBufferSize()` | `64 KB` | Initial receive buffer size |
| `getMaxRecvBufferSize()` | `maxMessageSize + 12` | Maximum receive buffer size |

### Capability Flags

During handshake, client and server negotiate capabilities:

| Flag | Bit | Description |
|------|-----|-------------|
| `FLAG_LZ4` | 0 | LZ4 compression support |
| `FLAG_ZSTD` | 1 | Zstd compression support |
| `FLAG_GORILLA` | 2 | Gorilla timestamp encoding |

The negotiated capabilities are the intersection of client and server supported flags.

## Client Configuration (IlpV4Sender)

### Connection Options

```java
IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009);
```

### Sender Options

| Method | Default | Description |
|--------|---------|-------------|
| `useSchemaReference(boolean)` | `false` | Use schema hash references instead of full schemas |
| `autoFlushRows(int)` | `0` (disabled) | Auto-flush after N rows |

### Example Usage

```java
try (IlpV4Sender sender = IlpV4Sender.connect("localhost", 9009)) {
    sender.table("weather")
          .symbol("city", "London")
          .doubleColumn("temperature", 23.5)
          .longColumn("humidity", 65)
          .at(System.currentTimeMillis() * 1000L);

    sender.table("weather")
          .symbol("city", "Paris")
          .doubleColumn("temperature", 25.0)
          .longColumn("humidity", 55)
          .atNow();

    sender.flush();
}
```

## Protocol Detection

The server automatically detects the protocol based on the first 4 bytes:

| Magic Bytes | Protocol |
|-------------|----------|
| `ILP?` | ILP v4 handshake request |
| `ILP4` | ILP v4 direct message (no handshake) |
| `[a-zA-Z]...` | ILP text protocol |

## Schema Modes

### Full Schema Mode (0x00)

Every message includes complete column definitions. Use when:
- First message to a new table
- Schema has changed
- Schema caching is disabled

### Schema Reference Mode (0x01)

Message includes only a schema hash (8 bytes). Use when:
- Schema has been previously sent
- Server has cached the schema
- Reduces message size for repeated writes

If the server doesn't have the cached schema, it responds with `SCHEMA_REQUIRED` and the client should resend with full schema.

## Response Status Codes

| Code | Name | Description |
|------|------|-------------|
| `0x00` | OK | All data written successfully |
| `0x01` | PARTIAL | Some tables failed, others succeeded |
| `0x02` | SCHEMA_REQUIRED | Schema hash not found, resend with full schema |
| `0x03` | SCHEMA_MISMATCH | Column type mismatch |
| `0x04` | TABLE_NOT_FOUND | Table doesn't exist and auto-create disabled |
| `0x05` | PARSE_ERROR | Invalid message format |
| `0x06` | INTERNAL_ERROR | Server internal error |
| `0x07` | OVERLOADED | Server is overloaded, retry with backoff |

### Retriable Errors

- `SCHEMA_REQUIRED` - Retry with full schema
- `OVERLOADED` - Retry with exponential backoff (100ms, 200ms, 400ms... up to 10s)

## Type Mappings

### ILP v4 to QuestDB Type Mapping

| ILP v4 Type | Code | QuestDB Type |
|-------------|------|--------------|
| BOOLEAN | 0x01 | BOOLEAN |
| BYTE | 0x02 | BYTE |
| SHORT | 0x03 | SHORT |
| INT | 0x04 | INT |
| LONG | 0x05 | LONG |
| FLOAT | 0x06 | FLOAT |
| DOUBLE | 0x07 | DOUBLE |
| STRING | 0x08 | STRING |
| SYMBOL | 0x09 | SYMBOL |
| TIMESTAMP | 0x0A | TIMESTAMP |
| DATE | 0x0B | DATE |
| UUID | 0x0C | UUID |
| LONG256 | 0x0D | LONG256 |
| GEOHASH | 0x0E | GEOBYTE/GEOSHORT/GEOINT/GEOLONG |
| VARCHAR | 0x0F | VARCHAR |

### Nullable Types

Add `0x80` to the type code to indicate nullable. For example:
- `0x05` = LONG (not nullable)
- `0x85` = LONG (nullable)
