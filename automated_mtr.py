import subprocess
import json
import pandas as pd
import os
import csv
import datetime
import time

INTERVAL = 0.1  # MTR interval in seconds

# === Run MTR Command for each Destination ===
def run_mtr(destination: str, output_dir: str, timestamp: str, count: int) -> str:
    """
    Run MTR to the destination host and save JSON output.
    Returns the path to the saved JSON file.
    """
    # === Generate Timestamped Output File === 
    output_file = os.path.join(output_dir, f"mtr_{destination}_{timestamp}.json")
    
    ## interval below 1.0 is not allowed without root privileges.
    ## Workaround: Start the Python script from command line "sudo -E python3 process_manager.py" with root privileges. 
    mtr_cmd = ["mtr", "-rw", "--no-dns", "--aslookup", "--report-cycles", str(count),
    "--interval", str(INTERVAL), "--json", destination]

    # Alternatives: "--no-dns" / "--show-ips"
    # It is not running the command through a shell (e.g., bash).
    # It is using Python’s file writing, not shell redirection ( > output_file ).

    try:
        print(f"[INFO] Running MTR to {destination}...")
        result = subprocess.run(mtr_cmd, capture_output=True, text=True, check=True)
        with open(output_file, "w") as f:
            f.write(result.stdout)
        print(f"[INFO] MTR results saved to {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to run mtr for {destination}: {e}")
        return None

# === Parse and Process JSON ===
def parse_mtr_json(filepath: str, iteration_number: int) -> pd.DataFrame:
    """
    Parse MTR JSON and return a DataFrame of hop records.
    """
    if os.path.getsize(filepath) == 0:
        print(f"[ERROR] JSON file is empty: {filepath}")
        return None
    
    with open(filepath, "r") as f:
        data = json.load(f)

    hops = data.get("report", {}).get("hubs", [])
    if not hops:
        print("[WARNING] No hops found in MTR output.")
        return None
    
    # Hop metrics with fallback if keys are missing
    records = []
    for i, hop in enumerate(hops):
        raw_host = hop.get("host", "")
        if raw_host == "???" or not raw_host:
            host_id = f"path_{iteration_number}_hop_{i+1}"
        else:
            host_id = raw_host
        record = {
            "host": host_id,
            "ASN": hop.get("ASN") or hop.get("asn") or "N/A",
            "count": hop.get("count", 0),
            "Snt": hop.get("Snt") or hop.get("snt") or 0,
            "loss": hop.get("Loss%") or hop.get("loss") or 0,
            "last": hop.get("Last") or hop.get("last") or 0,
            "avg": hop.get("Avg") or hop.get("avg") or 0,
            "best": hop.get("Best") or hop.get("best") or 0,
            "worst": hop.get("Wrst") or hop.get("worst") or 0,
            "stdev": hop.get("StDev") or hop.get("stdev") or 0,
        }
        records.append(record)

    return pd.DataFrame(records)


# === Analyze Content of Trace ===
def analyze_mtr_trace(mtr_result: pd.DataFrame, destination: str, alert_threshold: float):
    """
    Display hop summary and flag high packet loss.
    """
    print(f"\n=== Full Hop Summary for {destination} ===")
    print(mtr_result.to_string(index=False))

    # === Filter High Packet Loss Hops ===
    high_loss = mtr_result[mtr_result["loss"] > alert_threshold]
    if not high_loss.empty:
        print(f"\n=== Hops with High Packet Loss (> {alert_threshold}%) ===")
        print(high_loss.to_string(index=False))
    else:
        print("\n[INFO] No high packet loss detected.")

def filter_mtr_traces(mtr_result: pd.DataFrame, loss_threshold: float) -> pd.DataFrame:
    """
    Filters MTR trace DataFrame to remove rows with packet loss greater than the threshold.

    Parameters:
    - df: DataFrame resulting from MTR JSON parsing.
    - loss_threshold: Maximum allowed packet loss percentage (e.g., 10.0).

    Returns:
    - A filtered DataFrame with rows above the threshold removed.
    """
    if mtr_result is None or mtr_result.empty:
        return mtr_result

    return mtr_result[mtr_result["loss"] <= loss_threshold].copy()

def filter_reachable_ips_with_mtr(input_csv: str, limit: int, output_csv: str):
    """
    Filters a list of IP addresses to find responsive destination IPs using MTR.

    Parameters:
    - input_csv: Path to CSV file containing IPs (one per row).
    - limit: Number of IPs to test.
    - output_csv: File path to save responsive IPs.
    """

    print(f"[INFO] Reading input destinations from {input_csv}")
    with open(input_csv, "r") as f:
        reader = csv.reader(f)
        destinations = [row[0].strip().strip('"') for row in reader if row]

    responsive_ips = []

    for i, ip in enumerate(destinations[:limit]):
        print(f"\n[CHECK] {i+1}/{limit} – Testing {ip}")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "./mtr_logs"
        os.makedirs(output_dir, exist_ok=True)

        json_path = run_mtr(ip, output_dir, timestamp, count=1)
        if json_path is None or not os.path.exists(json_path):
            continue

        df = parse_mtr_json(json_path, i + 1)
        if df is None or df.empty:
            continue

        # Check last hop (destination) response
        last_row = df.iloc[-1]
        loss = last_row.get("loss", 100.0)

        if loss < 100.0:
            print(f"[OK] {ip} is responsive (loss = {loss:.1f}%)")
            responsive_ips.append([ip])
        else:
            print(f"[SKIP] {ip} is unresponsive (loss = {loss:.1f}%)")

        # Optional: cleanup temporary JSON files
        os.remove(json_path)

        time.sleep(0.5)  # Light throttling

    # Save responsive IPs to output CSV
    if responsive_ips:
        pd.DataFrame(responsive_ips, columns=["ip"]).to_csv(output_csv, index=False)
        print(f"\n[SAVED] {len(responsive_ips)} responsive IPs saved to {output_csv}")
    else:
        print("[INFO] No responsive IPs found.")

