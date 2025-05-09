import os
import pandas as pd
import graph_construction as gc
import IP_geolocation as geo
import automated_mtr as mtr
import datetime


TOKEN_IPINFO = "34f1e6afbef803"  # Personal IPinfo token

# === Configuration ===
DESTINATIONS = ["tum.de", "8.8.8.8", "cloudflare.com", "177.192.255.38", "www.international.unb.br", "www.studyinfinland.fi" ]  # Target IP or hostname
COUNT = 10                        # Number of pings per hop
OUTPUT_DIR = "./mtr_logs"         # Folder to save logs
ALERT_LOSS_THRESHOLD = 10.0       # % packet loss to flag a hop

# === Ensure Output Directory Exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def process_mtr_for_destination(destination: str, iteration_number: int):
    """
    Full pipeline for one destination.
    """
    filepath = mtr.run_mtr(destination, OUTPUT_DIR, TIMESTAMP, COUNT)
    if filepath:
        df_mtr_result = mtr.parse_mtr_json(filepath, iteration_number)
        if df_mtr_result is not None and not df_mtr_result.empty:
            mtr.analyze_mtr_trace(df_mtr_result, destination, ALERT_LOSS_THRESHOLD)

            df_mtr_result_extended_geolocation = geo.find_geolocation_by_ipinfo(df_mtr_result, TOKEN_IPINFO)
            # === Build the graph ===
            gc.build_mtr_graph(df_mtr_result_extended_geolocation, iteration_number)


# ---------- main : Start of Run Script ------------
if __name__ == "__main__":
    """
    Entry point for running the full MTR measurement and graph pipeline.
    """
    # === Ensure Output Directory Exists ===
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    gc.reset_graph()

    for i, dest in enumerate(DESTINATIONS):
        process_mtr_for_destination(dest, i + 1)

    # === Draw the graph ===
    G = gc.get_graph()
    gc.draw_graph(G, os.path.join(OUTPUT_DIR, f"mtr_graph__{TIMESTAMP}.png"))
