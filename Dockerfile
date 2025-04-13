# lightweight Python image
FROM python:3.9

# Set container working directory
WORKDIR /app

#Copy the ETL script and dependencies into the container
COPY etl_script.py .
COPY requirements.txt .

#Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the ETL script when the container starts
CMD ["python", "etl_script.py"]
