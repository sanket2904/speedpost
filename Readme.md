# PostgreSQL Wire Protocol with In-Memory Caching

This project is an implementation of the PostgreSQL wire protocol with added in-memory caching. It aims to improve the performance of PostgreSQL database operations by caching frequently accessed data in memory.

## Features
- Implements the PostgreSQL wire protocol for communication between the client and server.
- Provides in-memory caching to store frequently accessed data, reducing the need for expensive database queries.

## Getting Started

To get started with this project, clone the repository and build the project using your preferred Rust toolchain.

```bash
git clone https://github.com/sanket2904/speedpost.git
cd yourrepository
cargo build
# env variables for database connection just use the url don't use the username and password
export DATABASE_URL=127.0.0.1:5432
cargo run 
```

