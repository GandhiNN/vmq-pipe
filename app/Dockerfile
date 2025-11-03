# Stage 1: Build
FROM golang:alpine AS builder

# Set working dir
WORKDIR /app

# Install runtime dependencies
RUN apk update && apk add --no-cache git

# Ensure a portable, static-ish binary
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

# Copy the source code
COPY . .

# Install module dependencies
RUN go mod tidy

# Build binary
RUN go build -o vmqpipe

# Stage 2: Final
FROM art.pmideep.com/icloud-docker-dev/alpine:3.17.3

# Set working dir
WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache git

# Create non-root user for security
RUN addgroup -S appuser \
    && adduser -S -G appuser -H -s /sbin/nologin appuser

# Copy the binary and set ownership
COPY --from=builder --chown=appuser:appuser /app/vmqpipe /app/vmqpipe

# RUN as non-root user
USER appuser

# # Set the entrypoint command
# ENTRYPOINT [ "/app/vmqpipe" ]