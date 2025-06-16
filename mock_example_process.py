import pandas as pd
import post_process as pp

import compute_ricci_curvature as ricci

ips = ["65.108.94.222", "5.223.62.210", "5.78.98.32"]

def create_mock_latency_matrix() -> pd.DataFrame:
    """
    Creates a mock latency matrix DataFrame with IPs as index and columns.
    Diagonal values are set to NaN or dash.
    """
    global ips
    data = [
        [None, 182, 177],
        [182, None, 153],
        [177, 153, None]
    ]
    
    latency_matrix = pd.DataFrame(data, index=ips, columns=ips)
    print("Mock Latency Matrix (ms):")
    print(latency_matrix)
    return latency_matrix

def create_mock_distance_matrix() -> pd.DataFrame:
    """
    Creates a mock geodesic distance matrix (in km) between IPs based on lat/lon.
    """
    ip_geo = {
        "65.108.94.222": (60.1695, 24.9354),     # Helsinki
        "5.223.62.210": (1.2897, 103.8501),      # Singapore
        "5.78.98.32": (45.5229, -122.9898),      # Hillsboro, Oregon
    }
    ips = list(ip_geo.keys())
    distance_matrix = pd.DataFrame(index=ips, columns=ips, dtype=float)
    for src_ip in ips:
        for dst_ip in ips:
            if src_ip == dst_ip:
                distance_matrix.at[src_ip, dst_ip] = None
            else:
                lat1, lon1 = ip_geo[src_ip]
                lat2, lon2 = ip_geo[dst_ip]
                dist = pp.calculate_geodesic_distance(lat1, lon1, lat2, lon2)
                distance_matrix.at[src_ip, dst_ip] = dist
    print("Mock Distance Matrix (km):")
    print(distance_matrix)
    return distance_matrix

def create_mock_theoretical_latency_matrix(distance_matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a theoretical latency matrix (ms) from a given distance matrix (km).
    Uses pp.calculate_theoretical_minimum_latency().
    """
    ips = distance_matrix.index.tolist()
    theoretical_matrix = pd.DataFrame(index=ips, columns=ips, dtype=float)
    for src in ips:
        for dst in ips:
            distance = distance_matrix.at[src, dst]
            if pd.notna(distance):
                delay = pp.calculate_theoretical_minimum_latency(distance)
                theoretical_matrix.at[src, dst] = round(delay, 3)
    print("Mock Theoretical Latency Matrix (ms):")
    print(theoretical_matrix)
    return theoretical_matrix

def create_mock_residual_latency_matrix(latency_matrix: pd.DataFrame, theoretical_matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a residual latency matrix (ms) as (observed - theoretical).
    Values are clamped to zero if negative.
    """
    ips = latency_matrix.index.tolist()
    residual_matrix = pd.DataFrame(index=ips, columns=ips, dtype=float)

    for src in ips:
        for dst in ips:
            observed = latency_matrix.at[src, dst]
            theoretical = theoretical_matrix.at[src, dst]

            if pd.notna(observed) and pd.notna(theoretical):
                residual = max(observed - theoretical, 0)
                residual_matrix.at[src, dst] = round(residual, 3)
    print("Mock Residual Latency Matrix (ms):")
    print(residual_matrix)
    return residual_matrix

def create_mock_graph_matrix(residual_matrix: pd.DataFrame, threshold_ms: float) -> pd.DataFrame:
    """
    Creates a binary graph matrix: 1 if residual latency < threshold, else 0.
    """
    ips = residual_matrix.index.tolist()
    graph_matrix = pd.DataFrame(index=ips, columns=ips, dtype=int)

    for src in ips:
        for dst in ips:
            value = residual_matrix.at[src, dst]
            if pd.notna(value) and value < threshold_ms:
                graph_matrix.at[src, dst] = 1
            else:
                graph_matrix.at[src, dst] = 0
    print(f"\nGraph Matrix (threshold < {threshold}ms):")
    print(graph_matrix)
    return graph_matrix

if __name__ == "__main__":
    latency_matrix = create_mock_latency_matrix()
    distance_matrix = create_mock_distance_matrix()
    theoretical_latency_matrix = create_mock_theoretical_latency_matrix(distance_matrix)
    residual_matrix = create_mock_residual_latency_matrix(latency_matrix, theoretical_latency_matrix)
    threshold = 100  # ms
    graph_matrix = create_mock_graph_matrix(residual_matrix, threshold)

    # === Generate test graph matrix from residuals
    test_graph_matrix = ricci.generate_graph_matrix_from_residuals(residual_matrix, threshold)
    print("\n[TEST] Graph Matrix from Residuals:")
    print(test_graph_matrix)

    # === Construct networkx graph
    test_networkx_graph = ricci.construct_networkx_graph(test_graph_matrix)
    print("Nodes:", list(test_networkx_graph.nodes()))
    print("\n[TEST] NetworkX Graph Edges:")
    print(list(test_networkx_graph.edges(data=True)))

    # === Assign weights from original latency matrix
    ricci.assign_edge_weights_from_latency(latency_matrix)
    print("\n[TEST] NetworkX Graph with Weights:")
    for u, v, data in test_networkx_graph.edges(data=True):
        print(f"{u} -- {v} | weight: {data['weight']}")

    # === Compute Ollivier Ricci curvature
    test_ricci_graph = ricci.compute_ollivier_ricci_curvatures()
    print("\n[TEST] Ricci Curvature Results:")
    for u, v, data in test_ricci_graph.edges(data=True):
        curvature = data.get("ricciCurvature", None)
        print(f"{u} -- {v} | Ricci Curvature: {curvature}")
