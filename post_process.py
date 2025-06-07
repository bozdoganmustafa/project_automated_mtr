import pandas as pd
import IP_geolocation as geo

# Shared Global Structures

# Global latency matrix to store latencies(ms) between corresponding nodes.
# Rows represent source node_IDs, columns represent destination node_IDs.
# It is a square & symmetric matrix.
latency_matrix = pd.DataFrame()

# Global list of Nodes for all hops with valid IP address in the traces.
extended_explored_nodes = pd.DataFrame(columns=[
    "node_id", "IP_address", "ASN", "latitude", "longitude", "city", "region", "country"
])
next_node_id = [0]  # Mutable counter

def get_extended_explored_nodes():
    return extended_explored_nodes

def get_latency_matrix():
    return latency_matrix

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


### --- Below are out-dated functions. --- ###

def update_explored_nodes(mtr_result: pd.DataFrame):
    """
    Assign node_IDs for each explored node/hop in MTR traces and store metadata (IP, geolocation) in the global extended_explored_nodes.
    Avoids duplicate entries based on IP_address.
    """
    global extended_explored_nodes

    existing_ips = set(extended_explored_nodes["IP_address"]) if not extended_explored_nodes.empty else set()

    for _, row in mtr_result.iterrows():
        ip = row["host"]
        if not geo.is_valid_ip(ip) or ip in existing_ips:
            continue

        new_entry = {
            "node_id": next_node_id[0],
            "IP_address": ip,
            "ASN": row.get("ASN"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "city": row.get("city"),
            "region": row.get("region"),
            "country": row.get("country")
        }
        extended_explored_nodes = pd.concat([extended_explored_nodes, pd.DataFrame([new_entry])], ignore_index=True)
        next_node_id[0] += 1



def get_node_id(IP_address: str) -> int | None:
    entry = extended_explored_nodes[extended_explored_nodes["IP_address"] == IP_address]
    return entry["node_id"].values[0] if not entry.empty else None

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

def update_latency_matrix_for_source_node(mtr_result: pd.DataFrame):
    """
    Update global latency_matrix with latencies from each MTR result mtr_result.
    Assumes first hop is the source and subsequent hops are destinations.
    Latencies in between hops are not evaluated.
    Latency is calculated as: avg - stdev and smoothed to be non-decreasing.
    Hops without valid IPs are ignored.
    Matrix will be symmetrized at next steps.
    """
    global latency_matrix

    if mtr_result.empty or len(mtr_result) < 2:
        return  # need at least source and one destination

    src_ip = mtr_result.iloc[0]["host"]
    src_node_id = get_node_id(src_ip)

    if src_node_id is None:
        return  # unknown source
    
    latencies = []
    for _, row in mtr_result.iterrows():
        avg = row.get("avg", None)
        stdev = row.get("stdev", None)
        if avg is None or stdev is None:
            latencies.append(None)
        else:
            latencies.append(avg - stdev)

    # Monotonic increase by smoothing
    smoothed = []
    prev = 0
    for val in latencies:
        if val is None:
            smoothed.append(None)
            continue
        val = max(val, prev)
        smoothed.append(val)
        prev = val

    # Ensure destination columns exist
    for _, row in mtr_result.iloc[1:].iterrows():
        dst_ip = row["host"]
        dst_node_id = get_node_id(dst_ip)
        if dst_node_id is None:
            continue
        if dst_node_id not in latency_matrix.columns:
            latency_matrix[dst_node_id] = pd.NA

    # Ensure row for this source exists
    if src_node_id not in latency_matrix.index:
        latency_matrix.loc[src_node_id] = pd.NA 
    # Fill values from source to all hops
    for i in range(1, len(mtr_result)): # skip first row (source)
        dst_ip = mtr_result.iloc[i]["host"]
        dst_node_id = get_node_id(dst_ip)
        if dst_node_id is None:
            continue
        latency = smoothed[i]
        if latency is None:
            continue

        current = latency_matrix.at[src_node_id, dst_node_id]
        if pd.isna(current) or latency < current:
            latency_matrix.at[src_node_id, dst_node_id] = latency

def update_latency_matrix_for_traversed_hops(mtr_result: pd.DataFrame):
    """
    Update global latency_matrix with latencies between each consecutive hop in the path.
    Latency between hop i and hop i+1 is calculated as:
        latency(i → i+1) = (avg_i+1 - stdev_i+1) - (avg_i - stdev_i)
    Negative values (due to measurement noise) are suppressed by applying a smoothing technique.
    Hops without valid IPs are ignored.
    """
    global latency_matrix

    if mtr_result.empty or len(mtr_result) < 2:
        return


    # Take all latency values as Average - Standard Deviation in the path.
    latencies = []
    for _, row in mtr_result.iterrows():
        avg = row.get("avg", None)
        stdev = row.get("stdev", None)
        if avg is None or stdev is None:
            latencies.append(None)
        else:
            latencies.append(avg - stdev)

    # Monotonic Increase in consecutive latencies by applying smoothing.
    smoothed_latencies = []
    prev = 0
    for val in latencies:
        if val is None:
            smoothed_latencies.append(None)
            continue
        val = max(val, prev)  # Enforce monotonic increase
        smoothed_latencies.append(val)
        prev = val

    # Loop over consecutive hop pairs
    for i in range(len(mtr_result) - 1):
        src_row = mtr_result.iloc[i]
        dst_row = mtr_result.iloc[i + 1]

        src_ip = src_row["host"]
        dst_ip = dst_row["host"]

        src_node_id = get_node_id(src_ip)
        dst_node_id = get_node_id(dst_ip)

        if src_node_id is None or dst_node_id is None:
            continue
        
        src_latency = smoothed_latencies[i]
        dst_latency = smoothed_latencies[i + 1]

        if src_latency is None or dst_latency is None:
            continue

        delta_latency = dst_latency - src_latency

        # Ensure matrix structure, both row and column exist.
        if dst_node_id not in latency_matrix.columns:
            latency_matrix[dst_node_id] = pd.NA
        if src_node_id not in latency_matrix.index:
            latency_matrix.loc[src_node_id] = pd.NA

        current = latency_matrix.at[src_node_id, dst_node_id]
        if pd.isna(current) or delta_latency < current:
            latency_matrix.at[src_node_id, dst_node_id] = delta_latency

def symmetrize_latency_matrix():
    """
    Symmetrizes the global latency_matrix in-place.
    """
    global latency_matrix
    latency_matrix = symmetrize_matrix(latency_matrix)

def ensure_latency_matrix_square(nodes_df: pd.DataFrame):
    """
    Ensures each node_id in the provided DataFrame exists as both a row and column
    in the global latency_matrix. This keeps the matrix square.
    """
    global latency_matrix

    node_ids = nodes_df["node_id"].tolist() if "node_id" in nodes_df.columns else []

 # First, ensure at least one column exists before adding rows
    if latency_matrix.empty:
        # Create a dummy column with NA values to allow safe row assignment
        latency_matrix["__init__"] = pd.NA

    for node_id in node_ids:
        if node_id not in latency_matrix.index:
            latency_matrix.loc[node_id] = pd.NA
        if node_id not in latency_matrix.columns:
            latency_matrix[node_id] = pd.NA

    # Remove dummy column if it still exists
    if "__init__" in latency_matrix.columns:
        latency_matrix.drop(columns="__init__", inplace=True)

def symmetrize_matrix(mtr_result: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a symmetric version of the given latency matrix.
    For each pair (i, j), sets:
        result[i][j] = result[j][i] = min(mtr_result[i][j], mtr_result[j][i]) if both exist,
                                     or whichever exists if one is missing.
    """
    result = mtr_result.copy()
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
