# Use an official Python runtime as a parent image
FROM python:3.9-slim


WORKDIR /dataprocess

# Copy the current directory contents into the container at /app
COPY . /dataprocess

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

CMD ["python", "main.py"]



