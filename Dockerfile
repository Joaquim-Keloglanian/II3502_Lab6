# Use Apache Spark base image
FROM apache/spark:3.5.1

# Switch to root to install uv
USER root

# Install uv (dependency manager)
RUN pip install uv

# Create spark user home directory
RUN mkdir -p /home/spark && chown spark:spark /home/spark

# Switch back to spark user
USER spark

# Create cache directory
RUN mkdir -p /home/spark/.cache

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Ensure ownership
USER root
RUN chown -R spark:spark /app
USER spark

# Install Python dependencies using uv (PySpark is already included in the base image)
RUN uv sync

# Expose Spark UI port
EXPOSE 4040

# Default command to run the application
CMD ["uv", "run", "python", "-m", "ii3502_lab6.climate_analysis"]