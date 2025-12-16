#!/usr/bin/env bash
set -euo pipefail

# run-docker-windows.sh
# Helper to build and run the ii3502-lab6 Docker image on Windows environments
# - Ensures .dockerignore exists to exclude .venv and other unnecessary files
# - Rebuilds the Docker image from scratch with no cache
# - If cygpath is available (Git Bash/MinGW), convert $(pwd) to a Windows path
# - If not, assume the current shell provides a usable POSIX path
# Usage:
#   ./run-docker-windows.sh [--] [<docker-args>]
# Example:
#   ./run-docker-windows.sh
#   ./run-docker-windows.sh uv run python -m ii3502_lab6.climate_analysis --output src/main/resources/output/

# Ensure .dockerignore exists to prevent .venv from being sent to Docker daemon
if [[ ! -f .dockerignore ]]; then
  echo "Creating .dockerignore to exclude .venv and cache directories..."
  cat > .dockerignore << 'EOF'
# Python virtual environments
.venv/
venv/
env/
ENV/

# Python cache
__pycache__/
*.py[cod]
*$py.class

# Distribution / packaging
lib/
lib64/
*.egg-info/

# IDE
.vscode/
.idea/

# Git
.git/

# Documentation
report/

# OS
.DS_Store
Thumbs.db
EOF
fi

echo "Building Docker image from scratch (no cache)..."
docker build --no-cache -t ii3502-lab6 .

# Determine host resources path suitable for Docker
if command -v cygpath >/dev/null 2>&1; then
  ROOT_WIN="$(cygpath -w "$(pwd)")"
  HOST_RES="${ROOT_WIN}/src/main/resources"
else
  HOST_RES="$(pwd)/src/main/resources"
fi

# Ensure folder exists on host
mkdir -p "${HOST_RES}"

echo "Running Docker container..."
# Run container with the mounted resources folder and forward any arguments to the container
# Use -- to allow passing args that begin with -
if [[ "$#" -eq 0 ]]; then
  docker run --rm -v "${HOST_RES}:/app/src/main/resources" ii3502-lab6
else
  docker run --rm -v "${HOST_RES}:/app/src/main/resources" ii3502-lab6 "$@"
fi
