import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

# === Global graph instance ===
G = nx.Graph()

def get_graph() -> nx.Graph:
    """
    Returns the current global MTR graph.
    """
    global G
    return G

def reset_graph():
    """
    Resets the global MTR graph to an empty state.
    Use this before starting a new measurement series.
    """
    global G
    G.clear()

def build_mtr_graph(mtr_result: pd.DataFrame, path_id: int) -> nx.Graph:
    """
    Build an undirected graph from an DataFrame of MTR output. It shows the path traces.
    Consecutive calls will be added to the same graph, cumulatively.
    Nodes are unique IPs; edges connect consecutive hops.
    """
    global G

    previous_ip = None

    for i, row in mtr_result.iterrows():
        ip = row['host'] or f"UNKNOWN_HOST"

        # Add node if it doesn't already exist
        if ip not in G:
            G.add_node(ip, IP_address=ip,
                    loss_ratio=row['loss'],
                    path_id=path_id,
                    lat=row.get("latitude"),
                    lon=row.get("longitude"),
                    city=row.get("city"))
        # multiple path ids are ignored for now.

        # Add edge between current and previous hop (even if IP is the same)
        if previous_ip is not None:
            G.add_edge(previous_ip, ip, path_id=path_id)

        previous_ip = ip

    return G

def draw_graph(G: nx.Graph, output_file: str):
    """
    Draw the network graph, display and save it to a file.
    """
    # Create combined label from structured attributes
    labels = {}
    for node, data in G.nodes(data=True):
        label = f"{data.get('IP_address', node)}\n"
        label += f"Loss: {data.get('loss_ratio', 0):.1f}%\n"
        label += f"Path: {data.get('path_id', '?')}\n"
        city = data.get("city") or "Unknown"
        lat = data.get("lat")
        lon = data.get("lon")
        label += f"{city}\n"
        if lat is not None and lon is not None:
            label += f"({lat:.2f}, {lon:.2f})"
        labels[node] = label
    

    plt.figure(figsize=(30, 20))
    # pos = nx.spring_layout(G, seed=42, k=0.8)
    pos = nx.shell_layout(G) # Circular layout
    nx.draw(G, pos, with_labels=False, node_color='lightblue', node_size=1200)
    nx.draw_networkx_labels(G, pos, labels, font_size=8)

    # nx.draw_networkx_edges(G, pos, alpha=0.4, width=1)

    # === Static color palette (up to 10 paths)
    path_colors = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ]
    # === Draw edges with static colors
    for u, v, data in G.edges(data=True):
        path_id = data.get('path_id', 0)
        color = path_colors[path_id % len(path_colors)]
        nx.draw_networkx_edges(G, pos, edgelist=[(u, v)], edge_color=color, width=1.5, alpha=0.7)


    plt.title("MTR Graph")
    plt.axis("off")
    # plt.tight_layout()
    plt.savefig(output_file, format='png', dpi=300)
    plt.close()
    print(f"Graph saved to {output_file}")
    plt.show()

    return

def plot_latency_heatmap(output_file: str, title: str, latency_matrix: pd.DataFrame):
    """
    Plots a heatmap with improved axis labeling for larger latency matrices.
    Displays as many IP labels as feasible without overlap.
    """
    if latency_matrix.empty:
        print("Provided latency matrix is empty. Heatmap not generated.")
        return

    plot_df = latency_matrix.copy().replace({pd.NA: np.nan})
    plot_df = plot_df.astype("float64")

    num_labels = len(plot_df)
    size = max(12, min(num_labels * 0.3, 30))  # Dynamic figure size based on matrix size
    font_size = max(6, 600 // num_labels)     # Adjust font size based on number of IPs

    plt.figure(figsize=(size, size))
    ax = sns.heatmap(
        plot_df,
        cmap="coolwarm",
        center=0,
        linewidths=0.3,
        linecolor='gray',
        cbar_kws={"label": "Latency (ms)"},
        square=True,
        xticklabels=True,
        yticklabels=True,
        annot=False
    )

    ax.set_title(title, fontsize=font_size + 2)
    ax.set_xlabel("Destination IP", fontsize=font_size + 1)
    ax.set_ylabel("Source IP", fontsize=font_size + 1)

    ax.set_xticklabels(ax.get_xticklabels(), rotation=90, fontsize=font_size)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=font_size)

    plt.tight_layout()
    plt.savefig(output_file, format='png', dpi=300)
    plt.close()

    print(f"Heatmap saved to {output_file}")


# Visualize a list of nodes with geolocation info using Folium with clustering on map.
def visualize_ip_geolocations(csv_path: str, output_html: str):
    import folium
    from folium.plugins import MarkerCluster
    import pandas as pd
    import os
    """
    Visualizes IP geolocations from a CSV file using Folium with clustering.
    
    Parameters:
    - csv_path: Path to CSV file containing latitude, longitude columns
    - output_html: Path to save the interactive HTML map
    """
    if not os.path.exists(csv_path):
        print(f"[ERROR] File not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    required_cols = {"latitude", "longitude"}
    if not required_cols.issubset(df.columns):
        print(f"[ERROR] CSV must contain columns: {required_cols}")
        return

    # Drop rows with missing coordinates
    df = df.dropna(subset=["latitude", "longitude"])

    # Create map centered roughly at geographical mean
    start_coords = [df["latitude"].mean(), df["longitude"].mean()]
    m = folium.Map(location=start_coords, zoom_start=2, tiles='cartodbpositron')

    marker_cluster = MarkerCluster().add_to(m)

    for _, row in df.iterrows():
        lat, lon = row["latitude"], row["longitude"]
        # Optional: you could also add popup info like ASN or city
        popup_text = f"{row.get('city', '')}, {row.get('country', '')} ({row.get('ASN', '')})"
        folium.Marker(location=[lat, lon], popup=popup_text).add_to(marker_cluster)

    m.save(output_html)
    print(f"[INFO] Saved map visualization to {output_html}")


def plot_matrix_histogram(matrix: pd.DataFrame, output_file: str, title: str = "Value Distribution", bins: int = 50):
    """
    Plots and saves a histogram of all non-NaN values in the matrix.

    Parameters:
    - matrix (pd.DataFrame): The input matrix with numeric values.
    - output_file (str): Path to save the histogram plot.
    - title (str): Title of the plot.
    - bins (int): Number of histogram bins (default 50).
    """
    flat_values = matrix.values.flatten()
    valid_values = pd.Series(flat_values).dropna()

    if valid_values.empty:
        print(f"[WARN] No valid values found in matrix for: {title}")
        return

    plt.figure(figsize=(10, 6))
    plt.hist(valid_values, bins=bins, color='steelblue', edgecolor='black')
    plt.title(title)
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_file, format='png', dpi=300)
    plt.close()

    print(f"[INFO] Histogram saved to {output_file}")