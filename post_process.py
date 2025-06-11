import pandas as pd
import IP_geolocation as geo
from geopy.distance import geodesic

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


def generate_residual_latency_matrix():
    """
    Generate a matrix that shows residual latency: observed - theoretical_min.
    Only computes for IP pairs with valid observed latency and known geo info.
    """
    global residual_latency_matrix

    extended_nodes = get_extended_explored_nodes()
    ip_to_geo = {
        row["IP_address"]: (row["latitude"], row["longitude"])
        for _, row in extended_nodes.iterrows()
        if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
    }

    overall_matrix = get_overall_latency_matrix()
    ips = extended_nodes["IP_address"].dropna().unique()

    # Initialize square matrix
    residual_latency_matrix = pd.DataFrame(index=ips, columns=ips, dtype=float)

    for src_ip in ips:
        for dst_ip in ips:
            if src_ip == dst_ip: # TODO Maybe remove
                residual_latency_matrix.at[src_ip, dst_ip] = 0.0
                continue

            try:
                observed = overall_matrix.at[src_ip, dst_ip]
            except KeyError:
                continue

            if pd.isna(observed):
                continue

            if src_ip not in ip_to_geo or dst_ip not in ip_to_geo:
                continue

            lat1, lon1 = ip_to_geo[src_ip]
            lat2, lon2 = ip_to_geo[dst_ip]
            distance = calculate_geodesic_distance(lat1, lon1, lat2, lon2)
            theoretical_min = calculate_theoretical_minimum_latency(distance)

            if theoretical_min is None:
                continue

            residual = observed - theoretical_min
            residual_latency_matrix.at[src_ip, dst_ip] = round(max(residual, 0), 3)

    print("[INFO] Residual latency matrix generated.")

def get_residual_latency_matrix():
    global residual_latency_matrix
    return residual_latency_matrix


def calculate_geodesic_distance(lat1, lon1, lat2, lon2) -> float:
    """
    Returns the geodesic distance (in kilometers) between two lat/lon coordinates.
    """
    try:
        return geodesic((lat1, lon1), (lat2, lon2)).kilometers
    except Exception as e:
        print(f"[WARNING] Failed to compute geodesic distance: {e}")
        return None

def calculate_theoretical_minimum_latency(distance_km: float) -> float:
    """
    Calculate theoretical round-trip latency in milliseconds.
    Assumes 2/3 speed of light in fiber (~200,000 km/s).
    Round-trip: 2 * one-way.
    """
    if distance_km is None:
        return None
    speed_km_per_s = 200_000  # 2/3 of light speed
    latency_sec = (2 * distance_km) / speed_km_per_s
    return latency_sec * 1000  # Convert to ms


def generate_distance_matrix(extended_explored_nodes: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a symmetric distance matrix (in km) based on geodesic distance
    between all pairs of nodes in extended_explored_nodes.
    """
    if "IP_address" not in extended_explored_nodes.columns:
        raise ValueError("Missing 'IP_address' column in input dataframe.")
    if "latitude" not in extended_explored_nodes.columns or "longitude" not in extended_explored_nodes.columns:
        raise ValueError("Missing 'latitude' or 'longitude' columns in input dataframe.")

    ips = extended_explored_nodes["IP_address"].tolist()
    latitudes = extended_explored_nodes["latitude"].tolist()
    longitudes = extended_explored_nodes["longitude"].tolist()

    # Initialize empty DataFrame
    distance_matrix = pd.DataFrame(index=ips, columns=ips, dtype=float)

    for i in range(len(ips)):
        lat1, lon1 = latitudes[i], longitudes[i]
        for j in range(i, len(ips)):
            lat2, lon2 = latitudes[j], longitudes[j]

            if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
                distance = None
            else:
                distance = calculate_geodesic_distance(lat1, lon1, lat2, lon2)

            # Set both (i,j) and (j,i) to ensure symmetry
            distance_matrix.at[ips[i], ips[j]] = distance
            distance_matrix.at[ips[j], ips[i]] = distance

    return distance_matrix