FROM python:3.10

# Allow statements and log messages to immediately appear in the Knative logs
#ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
COPY . /src
WORKDIR /src

# Install Python Requirements
RUN pip install -r requirements.txt

# Run main.py
CMD [ "python", "main.py"]