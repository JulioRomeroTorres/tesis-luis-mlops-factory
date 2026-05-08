#!/bin/bash
# Startup script for Azure App Service (Python 3.12 Linux)
# Serves the Vite SPA build with gunicorn + Flask
gunicorn --bind=0.0.0.0:8000 --timeout 120 --access-logfile - --error-logfile - app:app
