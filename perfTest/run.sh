#!/usr/bin/env bash
set -e

export NO_LOG_SUCCESS=1

# Reset the database
dropdb assemble_worker_perftest || true;
createdb assemble_worker_perftest;

# Install the schema
DATABASE_URL="assemble_worker_perftest" node ../dist/cli.js --once

# How long does it take to start up and shut down?
DATABASE_URL="assemble_worker_perftest" time node ../dist/cli.js --once

# Schedule the jobs
psql -f init.sql assemble_worker_perftest;

# Finally time the job execution
DATABASE_URL="assemble_worker_perftest" time node ../dist/cli.js --once

# And test latency
node ./latencyTest.js

