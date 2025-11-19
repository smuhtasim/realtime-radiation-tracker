# Radiation Tracker

Small end-to-end project that ingests, processes and visualizes radiation measurements.  
This repository contains a frontend (Vite + React + Tailwind), a backend API (Flask), a data producer, an optional PyFlink processing job, and helper scripts/datasets.
![Uploading Screenshot 2025-11-19 140333.png…]()

## Contents
- Architecture overview
- Quick start (Docker)
- Running components locally
- Key files & folders
- Development notes

## Architecture
- Producer: generates measurement messages (producer/producer.py).
- Backend: exposes REST API and ingests data (backend/app.py).
- Frontend: visualizes data (frontend/src).
- PyFlink job: optional stream/batch processing (pyflink_job/job.py).
- Orchestrated with docker-compose (docker-compose.yml).

## Quick start (Docker)
1. From repository root, build and start all services:
   ```sh
   docker-compose up --build
   ```
2. Open frontend at http://localhost:5173 (or port shown by compose).
3. Backend API is available at the port configured in docker-compose.yml.

Stop services:
```sh
docker-compose down
```

## Running components locally (without Docker)
- Backend:
  - Install dependencies: `pip install -r backend/requirements.txt`
  - Run: `python backend/app.py`
- Producer:
  - Install dependencies: `pip install -r producer/requirements.txt`
  - Run: `python producer/producer.py`
- Frontend:
  - From frontend/: `npm install` then `npm run dev`
- PyFlink:
  - See pyflink_job/README or pyflink_job/job.py for execution steps and required jars.

## API (example)
- Check backend/app.py for implemented endpoints.
- Useful curl example (adjust host/port):
  ```sh
  curl -X POST http://localhost:8000/measurements -H "Content-Type: application/json" -d '{"sensor_id":"s1","value":0.12,"ts":"2025-11-19T12:00:00Z"}'
  ```

## Key files & folders
- docker-compose.yml — service orchestration
- backend/
  - app.py — Flask API
  - Dockerfile, requirements.txt
- frontend/
  - src/ — React app (App.jsx, main.jsx, styles)
  - package.json, vite.config.js, tailwind.config.js
- producer/
  - producer.py — data generator
  - requirements.txt, Dockerfile
- pyflink_job/
  - job.py — optional Flink job, jars/
- dataset/
  - measurements_subset.csv — example dataset
- scripts/
  - subdataset.py — helper to create dataset subsets
- requirements.txt — shared tooling (optional)

## Development notes
- Frontend dev server: run inside frontend with `npm run dev`.
- Backend logs: check container logs via `docker-compose logs backend` or run locally to see Flask output.
- Use the dataset/ CSV for local development and testing.
- Dockerfiles available per service for containerized builds.
