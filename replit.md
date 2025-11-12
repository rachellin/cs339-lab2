# RustDB - Pedagogical Database Management System

## Overview
RustDB is an educational database management system written in Rust, designed for learning database internals. This is a multi-crate workspace project that implements core database components including storage management, catalog system, and error handling.

## Project Structure
This is a Rust workspace with the following crates:
- **rustdb-storage**: Storage engine with buffer pool, disk manager, and table heap
- **rustdb-catalog**: Schema and field definitions for the database catalog
- **rustdb-error**: Error handling and macros for the project
- **rustdb-dev**: Root library that integrates all components

## Purpose
This project is part of a database systems lab focused on implementing:
1. Tuple packing into pages
2. Page management in heap files
3. Buffer pool for in-memory page management
4. Storage back-end operations

Detailed lab instructions are available in `assign/storage.md`.

## Development Workflow

### Building the Project
The project is built automatically when you run the workflow. To manually build:
```bash
cargo build --workspace
```

### Running Tests
The "Build Project" workflow compiles the code to check for syntax errors. To run tests and verify your implementation:
```bash
cargo test --workspace
```

To run specific tests:
```bash
cargo test <test_name>
```

To run tests for a specific module:
```bash
cargo test <module_path>
```

**Note**: Many tests will initially fail with "not yet implemented" errors - this is expected! The failing tests represent the functionality you need to implement as part of the lab exercises.

### Project Type
This is a **library/CLI project** for educational purposes - not a web application. Development is done through writing code and running tests to validate implementations.

## Current State
- Rust toolchain installed and configured
- Project builds successfully with expected warnings (unfinished student implementations)
- Test framework is operational
- Workspace dependencies managed via Cargo

## Notes
- The `target/` directory is git-ignored (build artifacts)
- Test database file located at `crates/storage/src/disk/data/test.db`
- Requires Rust 1.83 or later
- No external services or environment variables required
