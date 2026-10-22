# Runs the conformance suite against a REAL broker.
#
# WHY a container: the suite needs a live nats-server, and every SDK used to obtain
# one differently — `go install …@latest` here, a downloaded binary there. That made
# the broker an untracked variable: two SDKs could pass against different versions
# and nobody would know. docker-compose.yml pins one image for all four.
#
# Build context is the repository root.
# The version in go.mod, not a guess: a newer toolchain would accept syntax the
# declared minimum does not, and CI would pass on something users cannot build.
FROM golang:1.26-alpine AS build

# `gcc` and `musl-dev` are for `-race`: the race detector builds runtime/cgo, which
# needs a C compiler. Without them the build fails at the last step, after every
# dependency has downloaded — the most expensive place to discover a missing package.
RUN apk add --no-cache git ca-certificates gcc musl-dev
WORKDIR /src

# Dependency layer first: the go.mod files change far less often than the code, so
# this layer survives ordinary edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# -race in the test image too: the agent and extension sides run concurrently by
# design, and the suite exercises both ends of every RPC. A race here is a product
# defect, not a test artefact.
RUN CGO_ENABLED=1 go test -c -race -o /out/conformance.test ./conformance/

FROM alpine:3.21

# The suite starts its own broker per case (natsrun); a real binary has to be on
# PATH for that. It is the same version docker-compose.yml pins for the shared
# broker, so an in-process broker behaves like the shared one.
ARG NATS_VERSION=2.15.1
RUN apk add --no-cache ca-certificates curl \
 && curl -fsSL -o /tmp/nats.tgz \
      "https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/nats-server-v${NATS_VERSION}-linux-amd64.tar.gz" \
 && tar -xzf /tmp/nats.tgz -C /tmp \
 && mv /tmp/nats-server-v${NATS_VERSION}-linux-amd64/nats-server /usr/local/bin/ \
 && rm -rf /tmp/nats.tgz /tmp/nats-server-v${NATS_VERSION}-linux-amd64

COPY --from=build /out/conformance.test /usr/local/bin/conformance.test

# Run as a plain user; nothing here needs root.
RUN adduser -D -u 10001 runner
USER runner

ENTRYPOINT ["/usr/local/bin/conformance.test", "-test.v"]
