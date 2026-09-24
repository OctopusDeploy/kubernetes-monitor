# Build
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
WORKDIR /build

# Dependency installation
COPY go.mod go.sum ./
RUN go mod download

# Build the app from source
COPY . .
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o monitor .

# Runtime image
FROM gcr.io/distroless/static:latest@sha256:3592aa8171c77482f62bbc4164e6a2d141c6122554ace66e5cc910cadb961ff0

# Copy only the binary from the build stage to the final image
COPY --from=build /build/monitor /

# Use a non-root user from distroless for better security
USER 65532:65532

# Set the entry point for the container
ENTRYPOINT ["/monitor"]
