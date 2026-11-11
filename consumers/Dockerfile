FROM python:3.12-slim

# Print logs immediately instead of buffering them (otherwise `docker logs` lags behind)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# One image for every group. The group id is the command: "wiki-leaderboard" or "bsky-likes-leaderboard".
# Exec form: python is PID 1 and receives SIGTERM from `docker stop` directly.
ENTRYPOINT ["python", "main.py"]
