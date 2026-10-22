# Build the skipq API server into a small, non-root image.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/skipq ./cmd/skipq

# distroless/static has no shell or package manager: only the binary runs.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/skipq /skipq
ENV SKIPQ_ADDR=:8080
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/skipq"]
