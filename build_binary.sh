#!/bin/bash

PACKAGE_NAME=$(poetry version)
PACKAGE_NAME="${PACKAGE_NAME/ /_}"

pyinstaller -F --name "$PACKAGE_NAME" ./simplespark/main_binary.py
