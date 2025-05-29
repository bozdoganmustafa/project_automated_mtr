import os
import pandas as pd
import graph_construction as gc
import IP_geolocation as geo
import automated_mtr as mtr
import datetime
import post_process as pp
import time

TOKEN_IPINFO = "34f1e6afbef803"  # Personal IP Info token for "Lite" plan.

# === Configuration ===
PING_CYCLES = 5                   # Number of pings per hop
OUTPUT_DIR = "./mtr_logs"         # Folder to save logs
ALERT_LOSS_THRESHOLD = 10.0       # % packet loss to flag a hop
GRAPH_DIR = "./graph_folder"  # Folder to save graphs

CSV_DIR = "./csv_folder" 
DESTINATIONS_FILE = os.path.join(CSV_DIR, "destinations.csv")
DESTINATIONS = pd.read_csv(DESTINATIONS_FILE, header=None)[0].str.strip().tolist()
TARGET_LIMIT = 5  # Process only the first destinations until this limit.

# === Ensure Output Directory Exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(GRAPH_DIR, exist_ok=True)

IPGEOLOCATION_DB_DIR = "geolocation_db/ipinfo_lite.csv"

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

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
    filtered_path = "./csv_folder/responsive_hetzner.csv"
    if not os.path.exists(filtered_path):
        mtr.filter_reachable_ips_with_mtr("./csv_folder/hetzner.csv", limit=50, output_csv=filtered_path)

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
