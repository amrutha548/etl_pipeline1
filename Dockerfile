FROM python:3.9-slim

# Set the working directory
WORKDIR /local_streamlit

# Copy the requirements file into the container
COPY requirements.txt /local_streamlit/

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . /local_streamlit/

# Expose the port Streamlit runs on
EXPOSE 8501

# Run the Streamlit app
CMD ["streamlit", "run", "local_streamlit.py", "--server.port=8501", "--server.headless=true"]

