import subprocess
import os

## PREREQUISITES: SSH key-based authentication should be set up for the current machine to access the VMs without entering a password.

# === VM CONFIGURATION ===
VMs = {
    "Helsinki": {
        "ip": "65.108.94.222",
        "relative_path": "vm_csv_folder/65_108_94_222"
    },
    "Singapore": {
        "ip": "5.223.62.210",
        "relative_path": "vm_csv_folder/5_223_62_210"
    },
    "Hillsboro": {
        "ip": "5.78.98.32",
        "relative_path": "vm_csv_folder/5_78_98_32"
    }
}

USERNAME = "mustafa"
REMOTE_PROJECT_PATH = "~/projects/repo_mtr"
REMOTE_VENV_ACTIVATE = ". venv/bin/activate"


# === FUNCTION: Connect to VM and Merge Latency matrices into Final latency matrix ===
def merge_results_at_vm(vm_ip):
    print(f"Merging results on VM {vm_ip}...")

    remote_cmd = (
        f'cd {REMOTE_PROJECT_PATH} && '
        f'{REMOTE_VENV_ACTIVATE} && '
        f'python3 vm_matrix_merger.py'
    )

    try:
        subprocess.run(
            ["ssh", f"{USERNAME}@{vm_ip}", remote_cmd],
            check=True
        )
        print(f"Merging complete on {vm_ip}")
    except subprocess.CalledProcessError as e:
        print(f"Merge script failed on {vm_ip}: {e}")
    except Exception as e:
        print(f"General error on {vm_ip}: {e}")

# === FUNCTION: Copy Results from Remote VM to Local ===
# TODO: Later on: Fetch only final structures instead of all files/matrices.
def fetch_results_from_vm(vm_ip, relative_path):
    print(f"Fetching files from {vm_ip}:{relative_path}...")

    remote_full_path = f"{USERNAME}@{vm_ip}:{REMOTE_PROJECT_PATH}/{relative_path}/*"
    local_path = os.path.join(os.getcwd(), relative_path)

    os.makedirs(local_path, exist_ok=True)  # Ensure local dir exists

    try:
        subprocess.run(
            ["scp", "-r", remote_full_path, local_path],
            check=True
        )
        print(f"Files copied to {local_path}")
    except subprocess.CalledProcessError as e:
        print(f"SCP failed for {vm_ip}: {e}")
    except Exception as e:
        print(f"General error fetching from {vm_ip}: {e}")

# === MAIN FUNCTION ===
def main():
    for location, vm_info in VMs.items():
        vm_ip = vm_info["ip"]
        results_folder_path = vm_info["relative_path"]

        print(f"\n=== Handling VM: {location} ({vm_ip}) ===")
        merge_results_at_vm(vm_ip)
        fetch_results_from_vm(vm_ip, results_folder_path)

# === ENTRY POINT ===
if __name__ == "__main__":
    main()
