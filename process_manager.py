import os
import pandas as pd
import graph_construction as gc
import IP_geolocation as geo
import automated_mtr as mtr
import datetime
import post_process as pp
import glob


TOKEN_IPINFO = "34f1e6afbef803"  # Personal IP Info token for "Lite" plan.

# === Configuration ===
PING_CYCLES = 5                   # Number of pings per hop
OUTPUT_DIR = "./mtr_logs"         # Folder to save logs
ALERT_LOSS_THRESHOLD = 10.0       # % packet loss to flag a hop
GRAPH_DIR = "./graph_folder"  # Folder to save graphs

CSV_DIR = "./csv_folder" 
DESTINATIONS_FILE = os.path.join(CSV_DIR, "responsive_hetzner.csv")
DESTINATIONS = pd.read_csv(DESTINATIONS_FILE, header=None)[0].astype(str).str.strip().tolist()
TARGET_LIMIT = 5  # Process only the first destinations until this limit.

# === Ensure Output Directory Exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(GRAPH_DIR, exist_ok=True)

IPGEOLOCATION_DB_DIR = "geolocation_db/ipinfo_lite.csv"

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

VM_CSV_DIR = "./vm_csv_folder" # For experiment results as CSV

def process_mtr_for_destination(destination: str, iteration_number: int):
    """
    Full pipeline for one destination.
    """
    filepath = mtr.run_mtr(destination, OUTPUT_DIR, TIMESTAMP, PING_CYCLES)
    if filepath:
        df_mtr_result = mtr.parse_mtr_json(filepath, iteration_number)
        if df_mtr_result is not None and not df_mtr_result.empty:
            mtr.analyze_mtr_trace(df_mtr_result, destination, ALERT_LOSS_THRESHOLD)
            df_mtr_result = mtr.filter_mtr_traces(df_mtr_result, 99.0)

            df_mtr_result_extended_geolocation = geo.find_geolocation_by_ipinfo(df_mtr_result, TOKEN_IPINFO)

            pp.update_explored_nodes(df_mtr_result_extended_geolocation)
            pp.ensure_latency_matrix_square(pp.get_explored_nodes_df())
            pp.update_latency_matrix_for_source_node(df_mtr_result)
            pp.update_latency_matrix_for_traversed_hops(df_mtr_result)
            # Build the path trace graph
            # gc.build_mtr_graph(df_mtr_result_extended_geolocation, iteration_number)
        # Optional: cleanup temporary JSON files
        os.remove(filepath)


# ---------- main : Start of Run Script ------------
if __name__ == "__main__":
    """
    Entry point for running the full MTR measurement and graph pipeline.
    """

    # Check destination IPs for responsiveness only if needed. Delete the CSV to force re-check.
    """
    filtered_path = "./csv_folder/responsive_hetzner.csv"
    if not os.path.exists(filtered_path):
        mtr.filter_reachable_ips_with_ping("./csv_folder/hetzner.csv", output_csv=filtered_path)
    

    gc.reset_graph()

    for i, dest in enumerate(DESTINATIONS[:TARGET_LIMIT]):
        process_mtr_for_destination(dest, i + 1)

    pp.symmetrize_latency_matrix()

    # === Show final explored nodes
    print("\n=== Explored Nodes ===")
    print(pp.get_explored_nodes_df().reset_index().to_string(index=False))

    # === Show latency matrix
    print("\n=== Latency Matrix (avg ms) ===")
    print(pp.get_latency_matrix().to_string())

    pp.finalize_explored_nodes_index()

    pp.get_explored_nodes_df().to_csv(os.path.join(CSV_DIR, "explored_nodes.csv"))
    pp.get_latency_matrix().to_csv(os.path.join(CSV_DIR, "latency_matrix.csv"))

    # === Draw the graph ===
    ## G = gc.get_graph()
    ## gc.draw_graph(G, os.path.join(GRAPH_DIR, f"mtr_graph__{TIMESTAMP}.png"))

    # === Plot latency heatmap
    heatmap_path = os.path.join(GRAPH_DIR, f"latency_heatmap__{TIMESTAMP}.png")
    gc.plot_latency_heatmap(
        output_file=heatmap_path,
        title="Symmetrized Latency Heatmap",
        latency_matrix=pp.get_latency_matrix()
    )
    """
    # New pipeline
    # Synchronize processes for VMs. 
    # After triggering all VMs, fetch the results from each VM and update the global structures.
    # For development purposes, start with reading them from local folders of VMs.
    # Store an up-to-date Extended Explored Nodes and merge with new explored nodes from VMs.
    # Attach geolocation to the Extended Explored Nodes.
    # Get all latency matrices from VMs, and merge as Overall Latency Matrix.
    EXT_NODES_FILE = os.path.join(CSV_DIR, "extended_explored_nodes.csv")
    if os.path.exists(EXT_NODES_FILE):
        pp.load_extended_explored_nodes(EXT_NODES_FILE)

    # Traverse all subfolders under ./vm_csv_folder looking for vm_explored_nodes.csv
    vm_nodes_files = glob.glob(VM_CSV_DIR + "/**/vm_explored_nodes.csv", recursive=True)

    # Before looking for geo, filter existing IPs.
    for vm_file in vm_nodes_files:
        print(f" Processing VM data file: {vm_file}")
        vm_nodes = pp.load_vm_nodes(vm_file)
        # Eliminate already known nodes
        vm_nodes = pp.eliminate_existing_nodes(vm_nodes)
        if not vm_nodes.empty:
            vm_nodes_with_geolocation = geo.find_geolocation_for_nodes(vm_nodes, TOKEN_IPINFO)
            pp.update_extended_explored_nodes(vm_nodes_with_geolocation)

    pp.save_extended_explored_nodes(EXT_NODES_FILE)

    """
    # Find Geolocations for responsive_hetzner.csv  
    responsive_path = os.path.join(CSV_DIR, "responsive_hetzner.csv")
    responsive_with_geo_path = os.path.join(CSV_DIR, "responsive_hetzner_with_geolocation.csv")

    if os.path.exists(responsive_path):
        responsive_df = pd.read_csv(responsive_path, header=None, names=["IP_address"])
        
        # Use your existing function to add geolocation info
        responsive_with_geo = geo.find_geolocation_for_nodes(responsive_df, TOKEN_IPINFO)
        
        # Save the enriched file
        responsive_with_geo.to_csv(responsive_with_geo_path, index=False)
        print(f" Saved enriched Hetzner IPs to: {responsive_with_geo_path}")
    """





