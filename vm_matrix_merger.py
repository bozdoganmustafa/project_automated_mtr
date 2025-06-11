import os
import argparse
import pandas as pd
from datetime import datetime
import utils
from collections import defaultdict, OrderedDict


# Configuration
VM_CSV_DIR = "./vm_csv_folder"


def parse_matrix_index(machine_dir: str, start_ts: int, end_ts: int) -> list:
    """
    Checks current VM folder and,
    Parses matrix_index.csv and filters file entries between given UNIX timestamps.
    Returns list of valid latency matrix file paths.
    """
    index_file = os.path.join(machine_dir, "matrix_index.csv")
    if not os.path.exists(index_file):
        print(f"No matrix_index.csv found in {machine_dir}")
        return []

    selected_files = []
    with open(index_file, "r") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 2:
                continue
            file_path, unix_ts = parts[0], int(parts[1])
            if start_ts <= unix_ts <= end_ts:
                selected_files.append(file_path)
    return selected_files


def load_latency_matrix(path):
    try:
        return pd.read_csv(path, index_col=0)
    except Exception as e:
        print(f"Failed to load {path}: {e}")
        return pd.DataFrame()

def merge_latency_matrices(matrix_files: list) -> pd.DataFrame:
    """
    Merge multiple latency matrices by averaging values per cell.
    """
    cell_values = defaultdict(list)
    all_ips_set = set()
    all_ips_list = []

    for file in matrix_files:
        df = load_latency_matrix(file)

        df = df.astype("float64")
        df = df.where(pd.notnull(df), None)  # Convert NaNs to None
        rows, cols = df.index.tolist(), df.columns.tolist()
        for ip in rows + cols:
            if ip not in all_ips_set:
                all_ips_set.add(ip)
                all_ips_list.append(ip)

        for i in rows:
            for j in cols:
                val = df.at[i, j]
                if val is not None:
                    cell_values[(i, j)].append(val)

    merged_latency_matrix = pd.DataFrame(index=all_ips_list, columns=all_ips_list, dtype=float)

    for (i, j), values in cell_values.items():
        merged_latency_matrix.at[i, j] = sum(values) / len(values)

    return merged_latency_matrix


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge VM latency matrices over time interval.")
    parser.add_argument("--start", type=int, default=0, help="Start UNIX timestamp (inclusive)")
    parser.add_argument("--end", type=int, default=int(datetime.now().timestamp()), help="End UNIX timestamp (inclusive)")
    args = parser.parse_args()

    # Resolve own folder name by IP
    own_ip = utils.find_own_public_ipv4()
    if not own_ip:
        raise RuntimeError("Could not determine public IP address.")
    folder_name = own_ip.replace('.', '_')
    machine_dir = os.path.join(VM_CSV_DIR, folder_name)

    # Get latency matrix files within timestamp window
    matrix_files = parse_matrix_index(machine_dir, args.start, args.end)
    if not matrix_files:
        print("No matrix files found in the specified time range.")
        exit(0)

    print(f"Found {len(matrix_files)} latency matrix files to merge.")

    # Merge and save
    print(f"Merging files from UNIX timestamps {args.start} to {args.end}")
    merged_matrix = merge_latency_matrices(matrix_files)
    output_file = os.path.join(machine_dir, "vm_final_latency_matrix.csv")
    merged_matrix.to_csv(output_file)
    print(f"Saved merged latency matrix to: {output_file}")

