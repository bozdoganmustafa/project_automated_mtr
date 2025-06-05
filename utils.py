import subprocess
import ipaddress

# Find the public IPv4 address of the current host machine.
def find_own_public_ipv4() -> str:
    """
    Returns the first public IPv4 address from 'hostname -I'.
    Skips private, loopback, and reserved addresses.
    """
    try:
        # Get all host IPs from hostname -I
        result = subprocess.check_output(["hostname", "-I"], text=True).strip()
        ip_list = result.split()

        for ip_str in ip_list:
            try:
                ip = ipaddress.ip_address(ip_str)
                if ip.version == 4 and not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved):
                    print(f"[INFO] Detected public IPv4 address: {ip}")
                    return str(ip)
            except ValueError:
                continue  # Skip malformed IPs

        raise RuntimeError("No public IPv4 address found.")
    
    except Exception as e:
        print(f"[ERROR] Failed to detect public IPv4 address: {e}")
        return None
