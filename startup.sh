#!/bin/bash
gunicorn --workers 4 --worker-class uvicorn.workers.UvicornWorker --timeout 600 --bind 0.0.0.0:8000 app:app
