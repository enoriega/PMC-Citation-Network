FROM python:3.12-slim-trixie
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy the project into the image
ADD . /app
# Set work directory
WORKDIR /app

# Install the virtual environment
RUN uv sync --locked

# Run the xml processing by default
CMD ["/bin/sh", "/app/process_directories.sh"]

