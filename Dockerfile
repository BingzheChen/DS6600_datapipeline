# syntax=docker/dockerfile:1

# Use Debian Linux Bookworm as the base image
FROM python:3.12.5-bookworm

# Set the working directory to /ds6600_datapipeline
WORKDIR /ds6600_datapipeline

# Copy the requirements.txt file into the container
COPY requirements.txt requirements.txt

# Install packages from requirements.txt using pip
RUN pip install --upgrade pip && pip install -r requirements.txt

# Expose the port for Jupyter Lab
EXPOSE 8888

# Run Jupyter Lab when the container starts
CMD ["jupyter", "lab","--ip=0.0.0.0","--allow-root"]

# # Expose the port for the dashboard
# EXPOSE 8050

# # Run the dashboard when the container starts
# CMD ["python", "app.py"]