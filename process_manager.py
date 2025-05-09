import os
import pandas as pd
import graph_construction as gc
import IP_geolocation as geo
import automated_mtr as mtr
import datetime
import post_process as pp

TOKEN_IPINFO = "34f1e6afbef803"  # Personal IP Info token for "Lite" plan.

# === Configuration ===
DESTINATIONS = ["tum.de", "cloudflare.com", "www.international.unb.br", "www.studyinfinland.fi" ]  # Target IP or hostname
# , "8.8.8.8", "177.192.255.38" Removed for now. 1st is USA, 2nd is Brazil.
PING_CYCLES = 10                        # Number of pings per hop
OUTPUT_DIR = "./mtr_logs"         # Folder to save logs
ALERT_LOSS_THRESHOLD = 10.0       # % packet loss to flag a hop

CSV_DIR = "./csv_folder" 

# === Ensure Output Directory Exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)


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
            
            df_mtr_result_extended_geolocation = geo.find_geolocation_by_ipinfo(df_mtr_result, TOKEN_IPINFO)

            pp.update_explored_nodes(df_mtr_result)
            pp.update_latency_matrix(df_mtr_result)

            # === Build the graph ===
            gc.build_mtr_graph(df_mtr_result_extended_geolocation, iteration_number)


# ---------- main : Start of Run Script ------------
if __name__ == "__main__":
    """
    Entry point for running the full MTR measurement and graph pipeline.
    """

    gc.reset_graph()

    for i, dest in enumerate(DESTINATIONS):
        process_mtr_for_destination(dest, i + 1)

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
    G = gc.get_graph()
    gc.draw_graph(G, os.path.join(OUTPUT_DIR, f"mtr_graph__{TIMESTAMP}.png"))
