import pandas as pd

# Shared Global Structures for VM Post Processing

# VM specific latency matrix to store latencies(ms) between corresponding nodes.
# Rows represent source node IP addresses, columns represent destination node IP addresses.
# It will be a square & symmetric matrix.
latency_matrix = pd.DataFrame()

# Global list of Nodes for all hops with valid IP address in the traces.
explored_nodes = pd.DataFrame(columns=[
    "IP_address"
])

def get_explored_nodes_df():
    return explored_nodes

def get_latency_matrix():
    return latency_matrix

def load_explored_nodes(file_path: str):
    """
    Loads explored node IPs from CSV and updates the global explored_nodes DataFrame.
    CSV is expected to have index + IP_address as a single column.
    """
    global explored_nodes
    df = pd.read_csv(file_path)

    # If there's an unnamed index column, drop it
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Keep only the 'IP_address' column
    if "IP_address" not in df.columns:
        raise ValueError("Expected column 'IP_address' not found.")

    explored_nodes = df[["IP_address"]].dropna().reset_index(drop=True)
    print(f"[INFO] Loaded explored nodes from {file_path}, total: {len(explored_nodes)}")


def load_latency_matrix(file_path: str):
    """
    Loads a latency matrix from a CSV file and sets the global latency_matrix variable.
    Rows and columns are IP addresses.
    """
    global latency_matrix
    latency_matrix = pd.read_csv(file_path, index_col=0)
    print(f"[INFO] Loaded latency matrix from {file_path}, shape: {latency_matrix.shape}")

    
def filter_mtr_invalid_ips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows from the MTR DataFrame that have invalid IP addresses in the 'host' field.
    """
    if df is None or df.empty:
        return df

    return df[df["host"].apply(is_valid_ip)].reset_index(drop=True)

def update_explored_nodes_basic(mtr_result: pd.DataFrame):
    """
    Adds IPs without geolocation.
    Stores results in explored_nodes_df with minimal structure.
    """
    global explored_nodes

    if explored_nodes.empty:
        existing_ips = set()
    else:
        existing_ips = set(explored_nodes["IP_address"])

    for _, row in mtr_result.iterrows():
        ip = row["host"]
        if not is_valid_ip(ip) or ip in existing_ips:
            continue
        new_entry = {"IP_address": ip}
        explored_nodes = pd.concat([explored_nodes, pd.DataFrame([new_entry])], ignore_index=True)


def ensure_latency_matrix_square(nodes_df: pd.DataFrame):
    """
    Ensures each IP address in the provided DataFrame exists as both a row and column
    in the global latency_matrix. This keeps the matrix square.
    """
    global latency_matrix

     # Safely extract the IP list from the 'IP_address' column
    if "IP_address" not in nodes_df.columns:
        raise ValueError("Expected column 'IP_address' not found in nodes_df.")

    ip_list = nodes_df["IP_address"].dropna().unique()

    # Add dummy column to allow row extension if empty
    if latency_matrix.empty:
        latency_matrix["__init__"] = pd.NA

    for ip in ip_list:
        if ip not in latency_matrix.index:
            latency_matrix = pd.concat([latency_matrix, pd.DataFrame(index=[ip])])
        if ip not in latency_matrix.columns:
            latency_matrix[ip] = pd.NA

    if "__init__" in latency_matrix.columns:
        latency_matrix.drop(columns="__init__", inplace=True)


def is_valid_ip(ip: str) -> bool:
    """
    Returns True if the string appears to be a valid IPv4 address.
    """
    import re
    return re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip) is not None


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

    mtr_result = mtr_result.reset_index(drop=True)

    src_ip = mtr_result.iloc[0]["host"]

    if not is_valid_ip(src_ip):
        return  # skip if source IP is not valid
    
    latencies = extract_smoothed_latencies(mtr_result)
    print("After extract_smoothed_latencies:", latencies)

    # Monotonic increase (non-decreasing) by smoothing
    latencies = enforce_monotonic_increase(latencies)
    print("After enforce_monotonic_increase:", latencies, "\n")

    # Fill values from source to all hops
    for i in range(1, len(mtr_result)): # skip first row (source)
        dst_ip = mtr_result.iloc[i]["host"]
        if not is_valid_ip(dst_ip):
            continue
        latency = latencies[i]
        if latency is None:
            continue

        current = latency_matrix.at[src_ip, dst_ip]
        if pd.isna(current) or latency < current:
            latency_matrix.at[src_ip, dst_ip] = latency


def update_latency_matrix_for_traversed_hops(mtr_result: pd.DataFrame):
    """
    Update global latency_matrix with latencies between each consecutive hop in the path.
    Uses IP addresses directly instead of node IDs.
    Latency between hop i and hop i+1 is calculated as:
        latency(i → i+1) = (avg_i+1 - stdev_i+1) - (avg_i - stdev_i)
    Negative values (due to measurement noise) are suppressed by applying a smoothing technique.
    Hops without valid IPs are ignored.
    """
    global latency_matrix

    if mtr_result.empty or len(mtr_result) < 2:
        return

    mtr_result = mtr_result.reset_index(drop=True)

    # Compute smoothed latencies
    latencies = extract_smoothed_latencies(mtr_result)

    latencies = enforce_monotonic_increase(latencies)

    # Loop over consecutive hops
    for i in range(len(mtr_result) - 1):
        src_ip = mtr_result.iloc[i]["host"]
        dst_ip = mtr_result.iloc[i + 1]["host"]

        if not is_valid_ip(src_ip) or not is_valid_ip(dst_ip):
            continue

        src_latency = latencies[i]
        dst_latency = latencies[i + 1]

        if src_latency is None or dst_latency is None:
            continue

        delta_latency = dst_latency - src_latency

        current = latency_matrix.at[src_ip, dst_ip]
        if pd.isna(current) or delta_latency < current:
            latency_matrix.at[src_ip, dst_ip] = delta_latency

# Extracts latencies from MTR result as avg - stdev.
def extract_smoothed_latencies(mtr_result: pd.DataFrame) -> list:
    """
    Extracts latencies from MTR result as avg - stdev.
    The first row (assumed to be the source) is set to 0.
    """
    latencies = []
    for i, row in enumerate(mtr_result.itertuples(index=False)):
        avg = getattr(row, "avg", None)
        stdev = getattr(row, "stdev", None)
        if i == 0:
            latencies.append(0)  # Force source latency to 0
        elif avg is None or stdev is None:
            latencies.append(None)
        else:
            latencies.append(max(0.0, avg - stdev))  # Ensure non-negative latency
    return latencies

# Ensure non-decreasing list by lowering previous higher values.
def enforce_monotonic_increase(values):
    latencies = values.copy()
    for i in range(len(latencies) - 2, -1, -1):  # Start from second-last going backwards
        if latencies[i] is not None and latencies[i + 1] is not None:
            latencies[i] = min(latencies[i], latencies[i + 1])
    return latencies

def symmetrize_latency_matrix():
    """
    Symmetrizes the global latency_matrix in-place.
    """
    global latency_matrix
    latency_matrix = symmetrize_matrix(latency_matrix)

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
