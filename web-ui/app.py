"""
Minimal SPA server for Azure App Service (Python 3.12).
Serves the Vite build output (dist/) and falls back to index.html
for client-side routing (React Router).
"""

import os
from flask import Flask, send_from_directory

DIST_DIR = os.path.join(os.path.dirname(__file__), "dist")

app = Flask(__name__, static_folder=None)


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    # If the file exists in dist/, serve it directly (JS, CSS, images, etc.)
    full_path = os.path.join(DIST_DIR, path)
    if path and os.path.isfile(full_path):
        return send_from_directory(DIST_DIR, path)
    # Otherwise fall back to index.html for React Router
    return send_from_directory(DIST_DIR, "index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
