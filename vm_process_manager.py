import os
import datetime
import argparse
import time

import pandas as pd
import automated_mtr as mtr
import vm_post_process as vm_pp
import utils as utils


# === Configuration ===
PING_CYCLES = 5                   # Number of pings per hop
OUTPUT_DIR = "./mtr_logs"         # Folder to save temporary mtr logs
ALERT_LOSS_THRESHOLD = 70.0       # % packet loss to flag and filter an unresponsive hop
TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

CSV_DIR = "./csv_folder"
DESTINATIONS_FILE = os.path.join(CSV_DIR, "destinations.csv")
DESTINATIONS = pd.read_csv(DESTINATIONS_FILE, header=None)[0].astype(str).str.strip().tolist()
TARGET_LIMIT = 3  # Process only the first destinations until this limit.

VM_CSV_DIR = "./vm_csv_folder" # For experiment results as CSV
UNIX_TIMESTAMP = int(time.time())

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(VM_CSV_DIR, exist_ok=True)

def process_mtr_for_destination(destination: str, iteration_number: int, own_ip: str):
    """
    Full pipeline for one destination.
    Collect latencies and create / extend Latency Matrix and Explored Nodes list.
    """
    filepath = mtr.run_mtr(destination, OUTPUT_DIR, TIMESTAMP, PING_CYCLES)
    if filepath:
        df_mtr_result = mtr.parse_mtr_json(filepath, iteration_number)
        if df_mtr_result is not None and not df_mtr_result.empty:
            
            mtr.analyze_mtr_trace(df_mtr_result, destination, ALERT_LOSS_THRESHOLD)
            
            df_mtr_result = mtr.filter_mtr_traces(df_mtr_result, ALERT_LOSS_THRESHOLD)

            df_mtr_result = vm_pp.filter_mtr_invalid_ips(df_mtr_result)
             # Replace private source IP
            df_mtr_result = vm_pp.replace_private_source_ip(df_mtr_result, own_ip)

            vm_pp.update_explored_nodes_basic(df_mtr_result)

            vm_pp.ensure_latency_matrix_square(vm_pp.get_explored_nodes_df())

            vm_pp.update_latency_matrix_for_source_node(df_mtr_result)

            vm_pp.update_latency_matrix_for_traversed_hops(df_mtr_result)

        # Optional: cleanup temporary JSON files
        os.remove(filepath)

# ---------- main : Start of Run Script ------------
if __name__ == "__main__":
    # Command-line arguments
    parser = argparse.ArgumentParser(description="Run MTR processing with a target limit.")
    parser.add_argument("--limit", type=int, default=TARGET_LIMIT, help="Number of destinations to process")
    args = parser.parse_args()

    TARGET_LIMIT = args.limit  # Use command-line argument instead of hardcoded value

    # Create a folder specific to current VM for CSV results.
    own_ip = utils.find_own_public_ipv4() 
    if not own_ip:
        raise RuntimeError("Could not determine public IP address.")
    # Convert IP to folder name format
    folder_name = own_ip.replace('.', '_')
    machine_dir = os.path.join(VM_CSV_DIR, folder_name)
    os.makedirs(machine_dir, exist_ok=True) 

    ## Load existing Explored Nodes file if available.
    ## Explored nodes includes only set of IPs for complete latency matrix (no geolocation at VM side).
    ## Latency Matrix should have Node_IPs instead of Node_IDs.
    EXPLORED_NODES_FILE = os.path.join(machine_dir, "vm_explored_nodes.csv")
    # New: Add Unix timestamp to filename for uniqueness
    LATENCY_MATRIX_FILE = os.path.join(machine_dir, f"vm_latency_matrix_{UNIX_TIMESTAMP}.csv")
    if os.path.exists(EXPLORED_NODES_FILE):
        vm_pp.load_explored_nodes(EXPLORED_NODES_FILE)


    # Loop over the destinations and process mtr except for own IP address.
    for i, dest in enumerate(DESTINATIONS[:TARGET_LIMIT]):
        if dest == own_ip:
            continue
        process_mtr_for_destination(dest, i + 1, own_ip)
    
    # Don't symmetrize, uni-directional measurement from that VM only.
    # Latency matrices will be merged and symmetrized at host machine.
    # vm_pp.symmetrize_latency_matrix()

    vm_pp.get_explored_nodes_df().to_csv(EXPLORED_NODES_FILE, index=False)
    vm_pp.get_latency_matrix().to_csv(LATENCY_MATRIX_FILE)
    # Save metadata
    with open(os.path.join(machine_dir, "matrix_index.csv"), "a") as f:
        f.write(f"{LATENCY_MATRIX_FILE},{UNIX_TIMESTAMP},{TIMESTAMP}\n")

    ## Put into a new class: DataRegularizer // Latency Matrix Regularizer Similar to post_process.py
    ## Define a new matrix with list of values as stringfied floats. So that, each measurement results at different times can be stored cumulatively.