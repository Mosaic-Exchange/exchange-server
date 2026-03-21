#!/usr/bin/env bash
cd "$(dirname "$0")/.." && mvn -q compile exec:java
