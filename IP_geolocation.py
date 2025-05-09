import pandas as pd
import requests
import time

def find_geolocation_by_ipinfo(df: pd.DataFrame, token: str = None) -> pd.DataFrame:
    """
    Add geolocation fields to the MTR DataFrame using IPinfo API.
    Uses the free tier or token if provided.
    """
    # Initialize new fields if not present
    for col in ["latitude", "longitude", "country", "region", "city", "org"]:
        if col not in df.columns:
            df[col] = None

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    for idx, row in df.iterrows():
        ip = row["host"]

        if not is_valid_ip(ip):
            continue

        try:
            url = f"https://ipinfo.io/{ip}/json"
            response = requests.get(url, headers=headers)
            data = response.json()

            # Extract lat/lon from 'loc'
            loc = data.get("loc", "")
            lat, lon = (None, None)
            if loc:
                try:
                    lat, lon = map(float, loc.split(","))
                except ValueError:
                    pass

            df.at[idx, "latitude"] = lat
            df.at[idx, "longitude"] = lon
            df.at[idx, "country"] = data.get("country")
            df.at[idx, "region"] = data.get("region")
            df.at[idx, "city"] = data.get("city")

            if token: # If token is provided, use extended fields
                org_field = data.get("org", "")
                if org_field:
                    df.at[idx, "org"] = org_field

                    # Overwrite ASN only if current one is invalid
                    if row["ASN"] in [None, "", "N/A", "AS???"]:
                        if org_field.startswith("AS"):
                            df.at[idx, "ASN"] = org_field.split(" ")[0]

        except Exception as e:
            print(f"[ERROR] Failed to fetch geolocation for {ip}: {e}")

        time.sleep(1.0)  # Light throttling

    return df

## Depreceated function
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