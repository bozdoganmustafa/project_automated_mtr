from datetime import datetime, timedelta
import subprocess
import shutil
import os

## IMPORTANT WARNING ##
# This script will first clear all results on the remote VMs before starting new measurements.

# === GLOBAL MEASUREMENT SETTINGS ===
period_seconds = 28800           # Period between measurements (8 hours) in seconds
repetitions = 21                  # Number of measurement repetitions in total

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

def synchronize_project(vm_ip):
    print(f"Synchronizing project on {vm_ip}...")

    remote_cmd = (
        f'cd {REMOTE_PROJECT_PATH} && '
        f'{REMOTE_VENV_ACTIVATE} && '
        f'git restore . && '
        f'git clean -fd && '
        f'git pull'
    )

    try:
        subprocess.run(
            ["ssh", f"{USERNAME}@{vm_ip}", remote_cmd],
            check=True
        )
        print(f"Git repo updated on {vm_ip}")
    except subprocess.CalledProcessError as e:
        print(f"Git pull failed on {vm_ip}: {e}")
    except Exception as e:
        print(f"Error during synchronization on {vm_ip}: {e}")


def clear_all_vm_results(vm_ip, relative_path):
    """
    Optionally, Back-up all results as gz (for csv) file under vm_csv_folder/ <VM >.
    Manually, push the cleaned results to github from Host machine.
    """

    print(f"Clearing results for VM {vm_ip}...")

    # --- Step 1–3: Remote cleanup
    remote_cmd = (
        f'cd {REMOTE_PROJECT_PATH} && '
        f'rm -rf {relative_path}/*'
    )

    try:
        subprocess.run(
            ["ssh", f"{USERNAME}@{vm_ip}", remote_cmd],
            check=True
        )
        print(f"Remote results cleared for {vm_ip}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to clear remote results on {vm_ip}: {e}")
    except Exception as e:
        print(f"General SSH error on {vm_ip}: {e}")

    # --- Step 4: Local cleanup
    local_path = os.path.join(os.getcwd(), relative_path)
    try:
        if os.path.exists(local_path):
            for filename in os.listdir(local_path):
                file_path = os.path.join(local_path, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # Delete file or symlink
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Delete subdirectory
            print(f"Local contents cleared in {local_path}")
        else:
            print(f"Local path does not exist: {local_path}")
    except Exception as e:
        print(f"Failed to clear local contents: {e}")


# === FUNCTION: Start Measurement Process on Remote VM ===
def start_measurement_process(vm_ip, period, repetitions):
    print(f"Starting measurement process on {vm_ip}...")

    # Step 1: Kill any tmux sessions safely
    kill_tmux_cmd = (
        f'cd {REMOTE_PROJECT_PATH} && '
        f'{REMOTE_VENV_ACTIVATE} && '
        f'tmux kill-server || true'
    )

    # Step 2: Start new tmux session with measurement loop
    start_tmux_cmd = (
        f'cd {REMOTE_PROJECT_PATH} && '
        f'{REMOTE_VENV_ACTIVATE} && '
        f'tmux new-session -d -s vmrun bash -c \''
        f'for i in $(seq 1 {repetitions}); do '
        f'python3 vm_process_manager.py; '
        f'sleep {period}; '
        f'done\''
    )

    try:
        subprocess.run(["ssh", f"{USERNAME}@{vm_ip}", kill_tmux_cmd], check=True)
        subprocess.run(["ssh", f"{USERNAME}@{vm_ip}", start_tmux_cmd], check=True)
        print(f"Measurement process started on {vm_ip} in tmux session 'vmrun'")
    except subprocess.CalledProcessError as e:
        print(f"Failed to start measurement on {vm_ip}: {e}")
    except Exception as e:
        print(f"General error on {vm_ip}: {e}")


# === MAIN FUNCTION ===
def main():
    global period_seconds, repetitions, VMs
    
    period_minutes = period_seconds // 60
    print(f"\n Measurement Configuration:")
    print(f"Period between measurements: {period_minutes} minutes")
    print(f"Number of repetitions: {repetitions}\n")
    total_duration = period_seconds * (repetitions - 1)
    expected_end_time = datetime.now() + timedelta(seconds=total_duration)
    print(f"Expected final measurement ends at: {expected_end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    for location, vm_info in VMs.items():
        print() # line break
        ip = vm_info["ip"]
        path = vm_info["relative_path"]
        synchronize_project(ip)
        clear_all_vm_results(ip, path)
        print(f"=== Starting Measurement at VM: {location} ({ip}) ===")
        start_measurement_process(ip, period_seconds, repetitions)

# === ENTRY POINT ===
if __name__ == "__main__":
    main()