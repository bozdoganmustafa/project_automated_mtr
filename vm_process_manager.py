import os
import pandas as pd
import automated_mtr as mtr
import datetime
import vm_post_process as vm_pp

# === Configuration ===
PING_CYCLES = 5                   # Number of pings per hop
OUTPUT_DIR = "./mtr_logs"         # Folder to save temporary mtr logs
ALERT_LOSS_THRESHOLD = 70.0       # % packet loss to flag and filter an unresponsive hop
TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

CSV_DIR = "./csv_folder"
DESTINATIONS_FILE = os.path.join(CSV_DIR, "responsive_hetzner.csv")
DESTINATIONS = pd.read_csv(DESTINATIONS_FILE, header=None)[0].astype(str).str.strip().tolist()
TARGET_LIMIT = 5  # Process only the first destinations until this limit.

VM_CSV_DIR = "./vm_csv_folder" # For experiment results as CSV

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(VM_CSV_DIR, exist_ok=True)

def process_mtr_for_destination(destination: str, iteration_number: int):
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

            vm_pp.update_explored_nodes_basic(df_mtr_result)

            vm_pp.ensure_latency_matrix_square(vm_pp.get_explored_nodes_df())

            vm_pp.update_latency_matrix_for_source_node(df_mtr_result)

            vm_pp.update_latency_matrix_for_traversed_hops(df_mtr_result)

        # Optional: cleanup temporary JSON files
        os.remove(filepath)

# ---------- main : Start of Run Script ------------
if __name__ == "__main__":
    ## Start with the saved Latency Matrix file if available.
    ## Start with the existing explored nodes file if available.

    ## Explored nodes need not to include geolocation for VMS. Just set of IPs for complete latency matrix.
    ## Latency Matrix should have Node_IPs instead of Node_IDs.
    
    EXPLORED_NODES_FILE = os.path.join(VM_CSV_DIR, "vm_explored_nodes.csv")
    LATENCY_MATRIX_FILE = os.path.join(VM_CSV_DIR, "vm_latency_matrix.csv")

    if os.path.exists(EXPLORED_NODES_FILE):
        vm_pp.load_explored_nodes(EXPLORED_NODES_FILE)

    if os.path.exists(LATENCY_MATRIX_FILE):
        vm_pp.load_latency_matrix(LATENCY_MATRIX_FILE)

    for i, dest in enumerate(DESTINATIONS[:TARGET_LIMIT]):
        process_mtr_for_destination(dest, i + 1)

    vm_pp.symmetrize_latency_matrix() # Generic behavior.

    vm_pp.get_explored_nodes_df().to_csv(EXPLORED_NODES_FILE, index=False)
    vm_pp.get_latency_matrix().to_csv(LATENCY_MATRIX_FILE)


    ## Put into a new class: DataRegularizer // Latency Matrix Regularizer Similar to post_process.py
    ## Define a new matrix with list of values as stringfied floats. So that, each mtr result can be stored cumulatively.