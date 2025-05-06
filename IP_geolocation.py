import pandas as pd
import requests
import time

def find_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gets geolocation information for each IP address by IP-lookup from www.ip-api.com.
    Add geolocation fields to the MTR DataFrame. 
    Skips placeholder hosts like ??? or names that aren't valid IPs.
    """
    # Initialize columns
    df["latitude"] = None
    df["longitude"] = None
    df["country"] = None
    df["city"] = None
    df["region"] = None

    for idx, row in df.iterrows():
        ip = row["host"]

        # Skip invalid or placeholder IPs
        if not is_valid_ip(ip):
            continue

        try:
            response = requests.get(f"http://ip-api.com/json/{ip}?fields=status,message,country,regionName,city,lat,lon")
            data = response.json()

            if data.get("status") == "success":
                df.at[idx, "latitude"] = data.get("lat")
                df.at[idx, "longitude"] = data.get("lon")
                df.at[idx, "country"] = data.get("country")
                df.at[idx, "city"] = data.get("city")
                df.at[idx, "region"] = data.get("regionName")
            else:
                print(f"[WARN] Geolocation failed for {ip}: {data.get('message')}")
        except Exception as e:
            print(f"[ERROR] Exception while looking up {ip}: {e}")

        time.sleep(1.5)  # Avoid hitting rate limit (max 45 req/min)

    return df

def is_valid_ip(ip: str) -> bool:
    """
    Returns True if the string appears to be a valid IPv4 address.
    """
    import re
    return re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip) is not None