# Dockerfile for Component 2 - Alert Filter
FROM openjdk:11-jre-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install SBT
RUN wget https://github.com/sbt/sbt/releases/download/v1.9.6/sbt-1.9.6.tgz && \
    tar -xzf sbt-1.9.6.tgz && \
    mv sbt /usr/local/sbt && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    rm sbt-1.9.6.tgz

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Compile the project
RUN sbt compile

# Run the application
CMD ["sbt", "run"]
