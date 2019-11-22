FROM rustlang/rust:nightly

WORKDIR /meilisearch

COPY . .

RUN cargo build

CMD cargo run
