import pandas as pd
import IP_geolocation as geo

# Shared Global Structures

# Global latency matrix to store latencies(ms) between corresponding nodes.
# Rows represent source node_IDs, columns represent destination node_IDs.
# It is a square & symmetric matrix.
overall_latency_matrix = pd.DataFrame()

# Global list of Nodes for all hops with valid IP address in the traces.
extended_explored_nodes = pd.DataFrame(columns=[
    "node_id", "IP_address", "ASN", "latitude", "longitude", "city", "region", "country"
])

def get_extended_explored_nodes():
    return extended_explored_nodes

def get_overall_latency_matrix():
    return overall_latency_matrix

def load_extended_explored_nodes(file_path: str):
    """
    Loads extended explored nodes (with node_id and geolocation) from a CSV file
    into the global extended_explored_nodes. Expects 'node_id' to be the index.
    """
    global extended_explored_nodes
    df = pd.read_csv(file_path, index_col="node_id")

    required_cols = ["IP_address", "ASN", "latitude", "longitude", "city", "region", "country"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    extended_explored_nodes = df

    print(f" Loaded extended explored nodes from {file_path}, total: {len(extended_explored_nodes)}")


def save_extended_explored_nodes(file_path: str):
    """
    Saves the extended_explored_nodes to a CSV file with a clean, ordered index as 'node_id'.
    """
    global extended_explored_nodes

    # Reset and reassign node IDs to be continuous and start from 0
    extended_explored_nodes = extended_explored_nodes.reset_index(drop=True)
    extended_explored_nodes.index.name = "node_id"

    extended_explored_nodes.to_csv(file_path)
    print(f" Saved extended explored nodes to {file_path}, total: {len(extended_explored_nodes)}")


def load_vm_nodes(filepath: str) -> pd.DataFrame:
    """
    Loads VM node IPs from a given CSV file and returns a DataFrame.
    Expects a column named 'IP_address'.
    """
    df = pd.read_csv(filepath)

    if "IP_address" not in df.columns:
        raise ValueError("Missing required 'IP_address' column in the file.")

    return df.dropna(subset=["IP_address"]).reset_index(drop=True)


def update_extended_explored_nodes(vm_nodes_with_geo: pd.DataFrame):
    """
    Updates the global extended_explored_nodes (extended_explored_nodes) with any new IPs from vm_nodes_with_geo.
    Assigns incremental node_id and preserves index.
    """
    global extended_explored_nodes

    if extended_explored_nodes.empty:
        extended_explored_nodes = pd.DataFrame(columns=[
            "IP_address", "ASN", "latitude", "longitude", "city", "region", "country"
        ]).set_index(pd.Index([], name="node_id"))

    existing_ips = set(extended_explored_nodes["IP_address"])
    next_id = extended_explored_nodes.index.max() + 1 if not extended_explored_nodes.empty else 0

    new_entries = []

    for _, row in vm_nodes_with_geo.iterrows():
        ip = row["IP_address"]
        if ip in existing_ips:
            continue

        new_entry = {
            "node_id": next_id,
            "IP_address": ip,
            "ASN": row.get("ASN"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "city": row.get("city"),
            "region": row.get("region"),
            "country": row.get("country")
        }
        new_entries.append(new_entry)
        next_id += 1

    if new_entries:
        new_df = pd.DataFrame(new_entries).set_index("node_id")
        extended_explored_nodes = pd.concat([extended_explored_nodes, new_df])
        extended_explored_nodes.sort_index(inplace=True)


def eliminate_existing_nodes(vm_nodes: pd.DataFrame) -> pd.DataFrame:
    """
    Removes nodes from vm_nodes that already exist in extended_explored_nodes (by IP_address).
    """
    global extended_explored_nodes

    if extended_explored_nodes.empty:
        return vm_nodes.copy()

    existing_ips = set(extended_explored_nodes["IP_address"])
    filtered_nodes = vm_nodes[~vm_nodes["IP_address"].isin(existing_ips)].reset_index(drop=True)
    return filtered_nodes

def ensure_overall_latency_matrix_square(extended_nodes_df: pd.DataFrame):
    """
    Prepares a square empty overall latency matrix with all IPs from extended_explored_nodes.
    """
    global overall_latency_matrix

    ip_list = extended_nodes_df["IP_address"].dropna().unique()

    overall_latency_matrix = pd.DataFrame(index=ip_list, columns=ip_list, dtype=float)
    overall_latency_matrix[:] = pd.NA  # Initialize with NaN

def load_vm_latency_matrix(filepath: str) -> pd.DataFrame:
    """
    Loads a VM latency matrix from CSV with IPs as both row index and column headers.
    """
    try:
        return pd.read_csv(filepath, index_col=0)
    except Exception as e:
        print(f"[ERROR] Failed to load VM latency matrix {filepath}: {e}")
        return pd.DataFrame()

def update_overall_latency_matrix(vm_matrix: pd.DataFrame):
    """
    Updates the global overall_latency_matrix with data from a VM's latency matrix.
    Keeps the minimum latency value if overlapping.
    All the IPs in the input latency matrix are already expected to be in the Overall Latency Matrix.
    """
    global overall_latency_matrix

    for src_ip in vm_matrix.index:
        for dst_ip in vm_matrix.columns:
            try:
                latency = vm_matrix.at[src_ip, dst_ip]
                if pd.isna(latency):
                    continue

                existing = overall_latency_matrix.at[src_ip, dst_ip]

                if pd.isna(existing) or latency < existing:
                    overall_latency_matrix.at[src_ip, dst_ip] = latency
            except KeyError:
                # Ignore IPs that are not part of extended_explored_nodes
                continue


def symmetrize_overall_latency_matrix():
    """
    Symmetrizes the global latency_matrix in-place.
    """
    global overall_latency_matrix
    overall_latency_matrix = symmetrize_matrix(overall_latency_matrix)


def symmetrize_matrix(matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a symmetric version of a given square matrix.
    For each pair (i, j), sets:
        result[i][j] = result[j][i] = min(matrix[i][j], matrix[j][i]) if both exist,
                                     or whichever exists if one is missing.
    """
    result = matrix.copy()
    all_node_ids = set(result.index).union(set(result.columns))

    for i in all_node_ids:
        for j in all_node_ids:
            if i == j:
                continue

            val_ij = result.at[i, j] if j in result.columns and i in result.index else pd.NA
            val_ji = result.at[j, i] if i in result.columns and j in result.index else pd.NA

            if pd.isna(val_ij) and pd.isna(val_ji):
                continue  # nothing to set

            if pd.isna(val_ij):
                result.at[i, j] = val_ji
            elif pd.isna(val_ji):
                result.at[j, i] = val_ij
            else:
                min_val = min(val_ij, val_ji)
                result.at[i, j] = min_val
                result.at[j, i] = min_val

    return result


### --- Below are out-dated functions. --- ###

def finalize_explored_nodes_index():
    """
    Sets 'node_id' as the index of extended_explored_nodes if not already.
    Should be called after all node discovery is done.
    """
    global extended_explored_nodes

    if not extended_explored_nodes.empty and extended_explored_nodes.index.name != "node_id":
        if "node_id" in extended_explored_nodes.columns:
            extended_explored_nodes.set_index("node_id", inplace=True)
            print(" Set 'node_id' as index for extended_explored_nodes.")
        else:
            print("[WARNING] 'node_id' column not found in extended_explored_nodes.")
