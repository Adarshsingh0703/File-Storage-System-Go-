# Use the official Golang image as a base
FROM golang:1.23-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files
COPY go.mod go.sum ./

# Download Go dependencies
RUN go mod download

# Copy the rest of the application
COPY . .

# Build the Go app
RUN go build -o FTS .

# Expose port 8080 for the Go app
EXPOSE 8080

# Run the Go app when the container starts
CMD ["./FTS"]
