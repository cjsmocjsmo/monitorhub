arch=$(uname -m)

if [ -d "/usr/local/bin" ]; then
    echo "Installing monitorhub to /usr/local/bin..."
    if [ "$arch" = "aarch64" ]; then
        cp monitorhub64 /usr/local/bin/monitorhub
    fi
    if [ "$arch" = "armv7l" ]; then
        cp monitorhub32 /usr/local/bin/monitorhub
    fi
fi

if [ -d "/etc/systemd/system" ]; then
    echo "Systemd detected. Setting up service..."
    if [ ! -f "/etc/systemd/system/monitorhub.service" ]; then
        echo "Service file does not exist. Creating ..."
        cp monitorhub.service /etc/systemd/system/
    fi
fi

if [ -f "/etc/systemd/system/monitorhub.service" ]; then
    echo "Enabling and starting monitorhub service..."
    systemctl enable monitorhub
    systemctl start monitorhub
fi