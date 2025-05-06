import subprocess
import json
import pandas as pd
import datetime
import os
import graph_construction as gc
import IP_geolocation as geo


# === Configuration ===
DESTINATIONS = ["tum.de", "8.8.8.8", "cloudflare.com", "177.192.255.38", "www.international.unb.br", "www.studyinfinland.fi" ]  # Target IP or hostname
COUNT = 10                        # Number of pings per hop
OUTPUT_DIR = "./mtr_logs"         # Folder to save logs
ALERT_LOSS_THRESHOLD = 10.0       # % packet loss to flag a hop

# === Ensure Output Directory Exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# --------------------------------------
def process_mtr_for_destination(destination: str, iteration_number: int):
    """
    Full pipeline for one destination.
    """
    filepath = run_mtr(destination)
    if filepath:
        df = parse_mtr_json(filepath, iteration_number)
        if not df.empty:
            analyze_mtr_trace(df, destination)

            df = geo.find_geolocation(df)
            # === Build the graph ===
            gc.build_mtr_graph(df, iteration_number)


# === Run MTR Command for each Destination ===
def run_mtr(destination: str) -> str:
    """
    Run MTR to the destination host and save JSON output.
    Returns the path to the saved JSON file.
    """
    # === Generate Timestamped Output File === 
    output_file = os.path.join(OUTPUT_DIR, f"mtr_{destination}_{TIMESTAMP}.json")
    
    mtr_cmd = [
        "mtr", "-rw","--no-dns","--aslookup","--report-cycles", str(COUNT), "--json", destination
    ]
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
def analyze_mtr_trace(df: pd.DataFrame, destination: str):
    """
    Display hop summary and flag high packet loss.
    """
    print(f"\n=== Full Hop Summary for {destination} ===")
    print(df.to_string(index=False))

    # === Filter High Packet Loss Hops ===
    high_loss = df[df["loss"] > ALERT_LOSS_THRESHOLD]
    if not high_loss.empty:
        print(f"\n=== ⚠️ Hops with High Packet Loss (> {ALERT_LOSS_THRESHOLD}%) ===")
        print(high_loss.to_string(index=False))
    else:
        print("\n[INFO] No high packet loss detected.")



# ---------- main : Start of Run Script ------------
if __name__ == "__main__":
    
    gc.reset_graph()

    for i, dest in enumerate(DESTINATIONS):
        process_mtr_for_destination(dest, i + 1)  # Pass iteration number
    
    # === Draw the graph ===
    G = gc.get_graph()
    gc.draw_graph(G, os.path.join(OUTPUT_DIR, f"mtr_graph__{TIMESTAMP}.png"))