import pandas as pd
import IP_geolocation as geo

# shared global structures
latency_matrix = pd.DataFrame()  # rows: source node_ID, columns: destination node_ID

# Global list of Nodes for all hops with valid IP address in the traces.
explored_nodes_df = pd.DataFrame(columns=[
    "node_id", "IP_address", "ASN", "latitude", "longitude", "city", "region", "country"
])
next_node_id = [0]  # Mutable counter

# Global latency matrix to store average latencies between nodes. 
latency_matrix = pd.DataFrame()  # rows: source node_ID, columns: destination node_ID

def get_explored_nodes_df():
    return explored_nodes_df

def get_latency_matrix():
    return latency_matrix

def update_explored_nodes(df: pd.DataFrame):
    """
    Assign node_IDs for each explored node/hop in MTR traces and store metadata in the global explored_nodes_df.
    Avoids duplicate entries based on IP_address.
    """
    global explored_nodes_df

    existing_ips = set(explored_nodes_df["IP_address"]) if not explored_nodes_df.empty else set()

    for _, row in df.iterrows():
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
        explored_nodes_df = pd.concat([explored_nodes_df, pd.DataFrame([new_entry])], ignore_index=True)
        next_node_id[0] += 1

def get_node_id(ip: str) -> int | None:
    entry = explored_nodes_df[explored_nodes_df["IP_address"] == ip]
    return entry["node_id"].values[0] if not entry.empty else None

def finalize_explored_nodes_index():
    """
    Sets 'node_id' as the index of explored_nodes_df if not already.
    Should be called after all node discovery is done.
    """
    global explored_nodes_df

    if not explored_nodes_df.empty and explored_nodes_df.index.name != "node_id":
        if "node_id" in explored_nodes_df.columns:
            explored_nodes_df.set_index("node_id", inplace=True)
            print("[INFO] Set 'node_id' as index for explored_nodes_df.")
        else:
            print("[WARNING] 'node_id' column not found in explored_nodes_df.")

def update_latency_matrix_for_source_node(df: pd.DataFrame):
    """
    Update global latency_matrix with new average latencies from MTR results df.
    Assumes first hop is the source and subsequent hops are destinations.
    Latencies in between hops are not evaluated.
    Matrix will be symmetrized. Not Implemented yet.
    """
    global latency_matrix

    if df.empty or len(df) < 2:
        return  # need at least source and one destination

    src_ip = df.iloc[0]["host"]
    src_node_id = get_node_id(src_ip)

    if src_node_id is None:
        return  # unknown source
    
    # Ensure destination columns exist
    for _, row in df.iloc[1:].iterrows():
        dst_ip = row["host"]
        dst_node_id = get_node_id(dst_ip)
        if dst_node_id is None:
            continue
        if dst_node_id not in latency_matrix.columns:
            latency_matrix[dst_node_id] = pd.NA

    # Ensure row for this source exists
    if src_node_id not in latency_matrix.index:
        latency_matrix.loc[src_node_id] = pd.NA 

    for _, row in df.iloc[1:].iterrows():  # skip first row (source)
        dst_ip = row["host"]
        dst_node_id = get_node_id(dst_ip)
        if dst_node_id is None:
            continue

        best_latency = row.get("best", None)
        if best_latency is not None:
            current = latency_matrix.at[src_node_id, dst_node_id] if dst_node_id in latency_matrix.columns else None

            if pd.isna(current) or best_latency < current:
                latency_matrix.at[src_node_id, dst_node_id] = best_latency


def update_latency_matrix_for_traversed_hops(df: pd.DataFrame):
    """
    Update global latency_matrix with best latencies between each consecutive hop in the path.
    Latency between hop i and hop i+1 is calculated as:
        latency(i → i+1) = best_latency(i+1) - best_latency(i)
    Negative values (due to measurement noise) are preserved.
    """
    global latency_matrix

    if df.empty or len(df) < 2:
        return

    # Loop over consecutive hop pairs
    for i in range(len(df) - 1):
        src_row = df.iloc[i]
        dst_row = df.iloc[i + 1]

        src_ip = src_row["host"]
        dst_ip = dst_row["host"]

        src_node_id = get_node_id(src_ip)
        dst_node_id = get_node_id(dst_ip)

        if src_node_id is None or dst_node_id is None:
            continue
        
        # Use best latency value which is the minimum observed latency.
        src_best = src_row.get("best", None)
        dst_best = dst_row.get("best", None)

        if src_best is None or dst_best is None:
            continue

        delta_latency = dst_best - src_best  # keep even if negative

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

def symmetrize_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a symmetric version of the given latency matrix.
    For each pair (i, j), sets:
        result[i][j] = result[j][i] = min(df[i][j], df[j][i]) if both exist,
                                     or whichever exists if one is missing.
    """
    result = df.copy()
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
