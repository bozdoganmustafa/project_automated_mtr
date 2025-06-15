import pandas as pd
import networkx as nx
from GraphRicciCurvature.OllivierRicci import OllivierRicci


graph_networkx = nx.Graph()  # Global graph


def generate_graph_matrix_from_residuals(residual_latency_matrix: pd.DataFrame, threshold_latency: float) -> pd.DataFrame:
    # Create a boolean mask: True where latency is non-null and less than threshold
    condition = (residual_latency_matrix < threshold_latency) & residual_latency_matrix.notna()
    
    # Convert to integers: True → 1, False → 0
    graph_matrix = condition.astype(int)

    # Remove IPs with no connections (all 0s in row and column)
    valid_ips = graph_matrix.index[
        (graph_matrix.sum(axis=0) + graph_matrix.sum(axis=1)) > 0
    ]
    graph_matrix = graph_matrix.loc[valid_ips, valid_ips]

    return graph_matrix


def construct_networkx_graph(graph_matrix: pd.DataFrame):
    global graph_networkx
    graph_networkx = nx.Graph()

    for row_ip in graph_matrix.index:
        for col_ip in graph_matrix.columns:
            if graph_matrix.at[row_ip, col_ip] == 1:
                graph_networkx.add_edge(row_ip, col_ip)
                if row_ip == col_ip:
                    print(f"Self-edge added: ({row_ip}, {col_ip})")


def assign_edge_weights_from_latency(latency_matrix: pd.DataFrame):
    global graph_networkx
    
    for u, v in graph_networkx.edges():
        weight = None

        # Try both symmetric positions
        if u in latency_matrix.index and v in latency_matrix.columns:
            weight = latency_matrix.at[u, v]
        elif v in latency_matrix.index and u in latency_matrix.columns:
            weight = latency_matrix.at[v, u]

        # Fallback if missing or invalid
        if pd.isna(weight) or weight <= 0:
            weight = 1.0  # default positive fallback

        graph_networkx[u][v]['weight'] = float(weight)


def compute_ollivier_ricci_curvatures():
    global graph_networkx
    orc = OllivierRicci(
        graph_networkx,
        weight='weight',
        alpha=0.5,
        method='OTD',   # Use simpler transport distance
        proc=1,     # Use one process only. Avoid multiprocessing/networKit
        verbose="INFO"
    )
    orc.compute_ricci_curvature()
    return orc.G  # Contains "ricciCurvature" attribute on each edge
