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

Optionally create a virtual environment:
```bash
python3 -m venv venv
```

Activate venv if you have set (Bash or sh):
```bash
source venv/bin/activate
. venv/bin/activate
```

Ensure all dependencies are installed:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Start the Process

Activate venv if you have set (Bash or sh):
```bash
source venv/bin/activate
. venv/bin/activate
```

Run the main script to begin automated data collection.
```bash
python3 vm_process_manager.py
```
Optionally with a limit of destination targets (Default=5):
```bash
python3 vm_process_manager.py --limit 15
```

Process is expected to run several minutes due to mtr cycles and IP look-ups. 
Processing time may vary depending on the number of destinations and the length of the paths.
If Interval parameter in automated_mtr.py is set to a value lower than 1.0, script needed to start with root privileges.
```bash
sudo -E python3 vm_process_manager.py
```

Deactivate venv if it is active:
```bash
deactivate
```

From Host machine:
```bash
python3 process_manager.py
```

## Important Notes
- The project focuses on destinations located in **Europe** and the **USA**.
- Destinations outside these regions may result in higher mtr & geolocation error rates and more unknown hops.