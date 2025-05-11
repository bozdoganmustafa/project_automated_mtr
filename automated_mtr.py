import subprocess
import json
import pandas as pd
import os

# === Run MTR Command for each Destination ===
def run_mtr(destination: str, output_dir: str, timestamp: str, count: int) -> str:
    """
    Run MTR to the destination host and save JSON output.
    Returns the path to the saved JSON file.
    """
    # === Generate Timestamped Output File === 
    output_file = os.path.join(output_dir, f"mtr_{destination}_{timestamp}.json")
    
    mtr_cmd = ["mtr", "-rw", "--no-dns", "--aslookup", "--report-cycles", str(count), "--json", destination]

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
def analyze_mtr_trace(df: pd.DataFrame, destination: str, alert_threshold: float):
    """
    Display hop summary and flag high packet loss.
    """
    print(f"\n=== Full Hop Summary for {destination} ===")
    print(df.to_string(index=False))

    # === Filter High Packet Loss Hops ===
    high_loss = df[df["loss"] > alert_threshold]
    if not high_loss.empty:
        print(f"\n=== Hops with High Packet Loss (> {alert_threshold}%) ===")
        print(high_loss.to_string(index=False))
    else:
        print("\n[INFO] No high packet loss detected.")

def filter_mtr_traces(df: pd.DataFrame, loss_threshold: float) -> pd.DataFrame:
    """
    Filters MTR trace DataFrame to remove rows with packet loss greater than the threshold.

    Parameters:
    - df: DataFrame resulting from MTR JSON parsing.
    - loss_threshold: Maximum allowed packet loss percentage (e.g., 10.0).

    Returns:
    - A filtered DataFrame with rows above the threshold removed.
    """
    if df is None or df.empty:
        return df

    return df[df["loss"] <= loss_threshold].copy()
