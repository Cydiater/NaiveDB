FROM rust:latest

WORKDIR /db

COPY . /db/

RUN cargo build --release

CMD ["cargo", "run", "--release"]
