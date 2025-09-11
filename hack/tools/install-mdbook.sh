#!/usr/bin/env bash

set -eu

MDBOOK="${1}"
MDBOOK_VERSION="${2}"
CURDIR="${PWD}"

# If it exists, do not redownload
if [ -f "${MDBOOK}-${MDBOOK_VERSION}" ]; then
  exit 0
fi

# Detect OS
OS="$(uname -s)"
ARCH="$(uname -m)"

# Map architecture
case "$ARCH" in
    x86_64) ARCH="x86_64" ;;
    arm64|aarch64) ARCH="aarch64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Determine Linux variant
if [ "$OS" = "Linux" ]; then
    if ldd --version 2>&1 | grep -q "musl"; then
        LIBC="musl"
    else
        LIBC="gnu"
    fi
    OS_TYPE="unknown-linux-$LIBC"
elif [ "$OS" = "Darwin" ]; then
    OS_TYPE="apple-darwin"
else
    echo "Unsupported OS: $OS"
    exit 1
fi

# Construct download URL
URL="https://github.com/rust-lang/mdBook/releases/download/${MDBOOK_VERSION}/mdbook-${MDBOOK_VERSION}-${ARCH}-${OS_TYPE}.tar.gz"
echo "Downloading: $URL"
curl -L "$URL" -o mdbook.tar.gz

# Extract and install
tar -xzf mdbook.tar.gz
chmod +x mdbook
mv mdbook "${MDBOOK}-${MDBOOK_VERSION}"
ln -sf "${MDBOOK}-${MDBOOK_VERSION}" "${MDBOOK}"

# Clean up
rm -f mdbook.tar.gz

TEMP=$(mktemp -d)
trap 'rm -rf "$TEMP"' EXIT

cd "${TEMP}"

TEMPLATE='<div id="version" class="version">\n  VERSION-PLACEHOLDER\n</div>'

"${MDBOOK}" init --theme --title "template" --ignore "none"

cp theme/index.hbs "${CURDIR}/docs/theme/index-template.hbs"
sed -i "/<div id=\"content\" class=\"content\">/i ${TEMPLATE}" "${CURDIR}/docs/theme/index-template.hbs"


echo "$("${MDBOOK}" --version) installed successfully!"
