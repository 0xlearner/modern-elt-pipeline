#!/bin/bash

# Function to kill the process running on a specific port
kill_port() {
    PORT=$1
    PID=$(sudo lsof -t -i:$PORT)
    if [ ! -z "$PID" ]; then
        echo "Killing process $PID running on port $PORT"
        sudo kill -9 $PID
        if [ $? -eq 0 ]; then
            echo "Successfully killed process $PID on port $PORT"
        else
            echo "Failed to kill process $PID on port $PORT"
        fi
    else
        echo "No process running on port $PORT"
    fi
}

# Kill processes running on ports 8080, 9000, and 9001
kill_port 8080
kill_port 9000
kill_port 9001
kill_port 5432
kill_port 3000
