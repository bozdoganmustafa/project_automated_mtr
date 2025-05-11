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

def build_mtr_graph(df: pd.DataFrame, path_id: int) -> nx.Graph:
    """
    Build an undirected graph from an DataFrame of MTR output. It shows the path traces.
    Consecutive calls will be added to the same graph, cumulatively.
    Nodes are unique IPs; edges connect consecutive hops.
    """
    global G

    previous_ip = None

    for i, row in df.iterrows():
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
    print(f"[INFO] Graph saved to {output_file}")
    plt.show()

    return

def plot_latency_heatmap(output_file: str, title: str, df: pd.DataFrame):
    """
    Plots a heatmap of the latency matrix and saves it to the given file path.
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np

    if df.empty:
        print("[WARN] Provided latency matrix is empty. Heatmap not generated.")
        return

    plot_df = df.copy().replace({pd.NA: np.nan})
    plot_df = plot_df.astype("float64")

    plt.figure(figsize=(12, 10))
    sns.heatmap(
        plot_df,
        annot=True,
        fmt=".1f",
        cmap="coolwarm",
        center=0,
        linewidths=0.5,
        linecolor='gray',
        cbar_kws={"label": "Latency (ms)"},
        square=True
    )

    plt.title(title)
    plt.xlabel("Destination Node ID")
    plt.ylabel("Source Node ID")
    plt.tight_layout()
    plt.savefig(output_file, format='png', dpi=300)
    plt.close()

    print(f"[INFO] Heatmap saved to {output_file}")
