# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build
COPY main.go ./
RUN go build -o server main.go

# Runtime stage
FROM alpine:latest

WORKDIR /app

# Copy binary and static files from builder
COPY --from=builder /app/server .

# Expose port 8080
EXPOSE 8080

# Run the server
CMD ["./server"]
