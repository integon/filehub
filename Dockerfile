# Use the official Python image as the base image
FROM python:3.11.7-slim-bullseye as build

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create a new image for the run stage
FROM python:3.11.7-slim-bullseye

# Install curl and remove cache
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Set the working directory inside the container
WORKDIR /app

# Copy the installed dependencies from the build stage
COPY --from=build /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/

# Create a non-root user and switch to it
RUN adduser --system --group nonrootuser && \
    chown -R nonrootuser:nonrootuser /app

# Switch to the non-root user
USER nonrootuser

# Copy the application code to the working directory
COPY fh.py .

# Specify the command to run on container start
CMD ["python", "fh.py"]

