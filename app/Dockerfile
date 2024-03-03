# Use an official Python runtime as a base image
FROM spark:latest

USER root

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir pandas pyspark

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

VOLUME .:/app

# Run an infinite loop to keep the container running
CMD ["tail", "-f", "/dev/null"]