#!/usr/bin/env bash
set -euo pipefail

python -V
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
