
ARCH=$(uname -m)

if [ "$ARCH" = "armv7l" ]; then
    echo "Building for armv7l..."
    CGO_ENABLED=0 GOOS=linux GOARCH=arm go build -o monitorhubrpi32
fi

if [ "$ARCH" = "aarch64" ]; then
    echo "Building for aarch64..."
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o monitorhubrpi64
fi

if [ "$ARCH" = "x86_64" ]; then
    echo "Building for amd64..."
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o monitorhub64
fi