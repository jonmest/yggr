# Installation

Add `yggr` to your `Cargo.toml`:

```toml
[dependencies]
yggr = "0.1"
```

Or, for the engine without the runtime:

```toml
[dependencies]
yggr-core = "0.1"
```

## Requirements

- Rust 1.85+ (edition 2024)
- `protoc` installed (used by `prost-build` to compile the wire format)

On Debian/Ubuntu:

```bash
sudo apt install protobuf-compiler
```

On macOS:

```bash
brew install protobuf
```
