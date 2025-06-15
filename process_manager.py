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
GRAPH_DIR = "./graph_folder"  # Folder to save graphs

CSV_DIR = "./csv_folder" 
DESTINATIONS_FILE = os.path.join(CSV_DIR, "destinations.csv")
DESTINATIONS = pd.read_csv(DESTINATIONS_FILE, header=None)[0].astype(str).str.strip().tolist()
TARGET_LIMIT = 5  # Process only the first destinations until this limit.

# === Ensure Output Directory Exists ===
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(GRAPH_DIR, exist_ok=True)

IPGEOLOCATION_DB_DIR = "geolocation_db/ipinfo_lite.csv"

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

VM_CSV_DIR = "./vm_csv_folder" # To get experiment results of VMs


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
    """

    """

    # === Show final explored nodes
    print("\n=== Explored Nodes ===")
    print(pp.get_explored_nodes_df().reset_index().to_string(index=False))

    # === Show latency matrix
    print("\n=== Latency Matrix (avg ms) ===")
    print(pp.get_latency_matrix().to_string())
    
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
        print(f" Processing VM nodes file: {vm_file}")
        vm_nodes = pp.load_vm_nodes(vm_file)
        # Eliminate already known nodes
        vm_nodes = pp.eliminate_existing_nodes(vm_nodes)
        if not vm_nodes.empty:
            vm_nodes_with_geolocation = geo.find_geolocation_for_nodes(vm_nodes, TOKEN_IPINFO)
            pp.update_extended_explored_nodes(vm_nodes_with_geolocation)

    pp.save_extended_explored_nodes(EXT_NODES_FILE)

    # Generate and save geodesic distance matrix
    distance_matrix = pp.generate_distance_matrix(pp.get_extended_explored_nodes())
    distance_matrix.to_csv(os.path.join(CSV_DIR, "distance_matrix.csv"))
    print(f"Saved distance matrix to {os.path.join(CSV_DIR, 'distance_matrix.csv')}")

    # Generate and save theoretical minimum latency matrix
    theoretical_min_matrix = pp.generate_theoretical_min_latency_matrix()
    theoretical_min_matrix.to_csv(os.path.join(CSV_DIR, "theoretical_min_latency_matrix.csv"))
    print(f"Saved theoretical latency matrix to {os.path.join(CSV_DIR, 'theoretical_min_latency_matrix.csv')}")


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

    # Get all latency matrices and merge as Overall Latency Matrix
    # Initial layout of Overall Latency Matrix is constructured from Overall Explored Nodes.
    pp.ensure_overall_latency_matrix_square(pp.get_extended_explored_nodes())
    # Traverse all subfolders under ./vm_csv_folder looking for vm_latency_matrix.csv
    vm_latency_matrix_files = glob.glob(VM_CSV_DIR + "/**/vm_final_latency_matrix.csv", recursive=True)
    for vm_file in vm_latency_matrix_files:
        print(f" Processing VM latency matrix file: {vm_file}")
        vm_latency_matrix = pp.load_vm_latency_matrix(vm_file)
        if not vm_latency_matrix.empty:
            pp.update_overall_latency_matrix(vm_latency_matrix)
    pp.symmetrize_overall_latency_matrix()
    
    pp.get_overall_latency_matrix().to_csv(os.path.join(CSV_DIR, "overall_latency_matrix.csv"))

    # Compute and save residuals
    pp.generate_residual_latency_matrix()
    pp.get_residual_latency_matrix().to_csv(os.path.join(CSV_DIR, "residual_latency_matrix.csv"))

    # === Plot latency heatmap
    heatmap_path = os.path.join(GRAPH_DIR, f"overall_latency_heatmap__{TIMESTAMP}.png")
    gc.plot_latency_heatmap(
        output_file=heatmap_path,
        title="Overall Latency Matrix Heatmap",
        latency_matrix=pp.get_overall_latency_matrix()
    )

    # === Plot residual latency heatmap
    residual_heatmap_path = os.path.join(GRAPH_DIR, f"residual_latency_heatmap__{TIMESTAMP}.png")
    gc.plot_latency_heatmap(
        output_file=residual_heatmap_path,
        title="Residual Latency Matrix Heatmap",
        latency_matrix=pp.get_residual_latency_matrix()
    )
    
    # === Plot distance heatmap
    distance_heatmap_path = os.path.join(GRAPH_DIR, f"distance_matrix_heatmap__{TIMESTAMP}.png")
    gc.plot_latency_heatmap(
        output_file=distance_heatmap_path,
        title="Geodesic Distance Matrix (km)",
        latency_matrix=pp.get_distance_matrix()
    )

    # === Plot theoretical latency heatmap
    theoretical_heatmap_path = os.path.join(GRAPH_DIR, f"theoretical_latency_heatmap__{TIMESTAMP}.png")
    gc.plot_latency_heatmap(
        output_file=theoretical_heatmap_path,
        title="Theoretical Minimum Latency Matrix (ms)",
        latency_matrix=pp.get_theoretical_min_latency_matrix()
    )
    # === Visualize Hetzner IPs on map
    gc.visualize_ip_geolocations(
        csv_path="./csv_folder/responsive_hetzner_with_geolocation.csv",
        output_html="./graph_folder/hetzner_ip_map.html"
    )