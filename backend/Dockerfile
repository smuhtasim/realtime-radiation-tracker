FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy your script and requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
# Run the script
CMD ["python", "app.py"]
