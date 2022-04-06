# Use Python 3.9 slim versus the full version for smaller Cloud Run instances
FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Cloud Run logs, this is great for debugging
ENV PYTHONUNBUFFERED True

# Copy the requirements.txt file to the container image, otherwise pip install will fail
COPY requirements.txt ./

# Install the requirements
RUN pip install -r requirements.txt

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Using the same settings as Google here
# If using a larger Cloud Run instance then increase these numbers
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
