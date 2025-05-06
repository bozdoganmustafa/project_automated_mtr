# Network Path Tracing via Automated MTR Measurements

## Project Purpose
This project automates network path tracing using MTR (My Traceroute) measurements to analyze routing paths and visualize network topology.

## Environment
- Designed to run on Linux distributions
- Tested on **Ubuntu 22.04.3**

## Features
- Automates `mtr` command-line tool execution via Python scripts
- Collects trace data for a list of destination IPs or hostnames
- Constructs full network paths including all traversed hops
- Determines geolocation for each hop
- Builds and exports a network topology graph as a PNG image

## Directory Structure

### 1. Prerequisites
Ensure all dependencies are installed:
```bash
pip install -r requirements.txt
```

### 2. Start the Process
Run the main script to begin automated tracing:
```bash
python3 automated_mtr.py
```
Process is expected to run 1-2 minutes due to mtr cycles and IP look-ups. 
Processing time may vary depending on the number of destinations and the length of the paths.

## Important Notes
- The project focuses on destinations located in **Europe** and the **USA**.
- Destinations outside these regions may result in higher mtr & geolocation error rates and more unknown hops.