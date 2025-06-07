import pandas as pd
import requests
import time
from ipaddress import ip_address, ip_network


def find_geolocation_for_nodes(vm_nodes: pd.DataFrame, token: str = None) -> pd.DataFrame:
    """
    Adds geolocation fields to a list of IPs in a DataFrame using the IPinfo API.
    Input: vm_nodes must have a column 'IP_address'.
    Output: Same DataFrame with columns: ASN, latitude, longitude, city, region, country.
    """
    nodes_with_geolocation = vm_nodes.copy()
    for col in ["ASN", "latitude", "longitude", "city", "region", "country", "org"]:
        if col not in nodes_with_geolocation.columns:
            nodes_with_geolocation[col] = None

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    for idx, row in nodes_with_geolocation.iterrows():
        ip = row["IP_address"]

        if not is_valid_ip(ip):
            continue

        try:
            url = f"https://ipinfo.io/{ip}/json"
            response = requests.get(url, headers=headers, timeout=5)
            data = response.json()

            # Parse location
            loc = data.get("loc", "")
            lat, lon = (None, None)
            if loc:
                try:
                    lat, lon = map(float, loc.split(","))
                except ValueError:
                    pass

            nodes_with_geolocation.at[idx, "latitude"] = lat
            nodes_with_geolocation.at[idx, "longitude"] = lon
            nodes_with_geolocation.at[idx, "country"] = data.get("country")
            nodes_with_geolocation.at[idx, "region"] = data.get("region")
            nodes_with_geolocation.at[idx, "city"] = data.get("city")

            org_field = data.get("org", "")
            nodes_with_geolocation.at[idx, "org"] = org_field

            if org_field.startswith("AS"):
                nodes_with_geolocation.at[idx, "ASN"] = org_field.split(" ")[0]
            else:
                nodes_with_geolocation.at[idx, "ASN"] = "AS???"

        except Exception as e:
            print(f"[ERROR] Failed to fetch geolocation for {ip}: {e}")

        time.sleep(1.0)  # Throttle requests to avoid IPinfo rate limits

    return nodes_with_geolocation


## Currently, a deficient function since downloaded IPinfo database lacks of latitude and longitude.
def find_geolocation_by_ipinfo_database(mtr_result: pd.DataFrame, db_path: str) -> pd.DataFrame:
    """
    Enrich MTR result with geolocation info from a local IPinfo-style CSV database.
    db_path: path to the CSV containing IP blocks and location info.
    Expected columns in DB:
        - network (CIDR), country, region, city, latitude, longitude, org (optional), asn (optional)
    """
    # Load local geolocation DB once
    geo_db = pd.read_csv(db_path)
    geo_db["network"] = geo_db["network"].apply(ip_network)

    # Initialize columns if missing
    for col in ["latitude", "longitude", "country", "region", "city", "org"]:
        if col not in mtr_result.columns:
            mtr_result[col] = None

    for idx, row in mtr_result.iterrows():
        ip = row["host"]
        if not is_valid_ip(ip):
            continue

        ip_obj = ip_address(ip)
        match = geo_db[geo_db["network"].apply(lambda net: ip_obj in net)]

        if not match.empty:
            matched_row = match.iloc[0]
            mtr_result.at[idx, "latitude"] = matched_row.get("latitude")
            mtr_result.at[idx, "longitude"] = matched_row.get("longitude")
            mtr_result.at[idx, "country"] = matched_row.get("country")
            mtr_result.at[idx, "region"] = matched_row.get("region")
            mtr_result.at[idx, "city"] = matched_row.get("city")

            org = matched_row.get("org", "")
            if org:
                mtr_result.at[idx, "org"] = org
                if row["ASN"] in [None, "", "N/A", "AS???"] and "AS" in org:
                    mtr_result.at[idx, "ASN"] = org.split(" ")[0]

        # Optional light throttle
        time.sleep(0.01)

    return mtr_result

## It uses ipinfo API. We need to use the database instead.
def find_geolocation_by_ipinfo(mtr_result: pd.DataFrame, token: str = None) -> pd.DataFrame:
    """
    Add geolocation fields to the MTR DataFrame using IPinfo API.
    Uses the free tier or token if provided.
    """
    # Initialize new fields if not present
    for col in ["latitude", "longitude", "country", "region", "city", "org"]:
        if col not in mtr_result.columns:
            mtr_result[col] = None

    headers = {"Authorization": f"Bearer {token}"} if token else {}

    for idx, row in mtr_result.iterrows():
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

            mtr_result.at[idx, "latitude"] = lat
            mtr_result.at[idx, "longitude"] = lon
            mtr_result.at[idx, "country"] = data.get("country")
            mtr_result.at[idx, "region"] = data.get("region")
            mtr_result.at[idx, "city"] = data.get("city")

            if token: # If token is provided, use extended fields
                org_field = data.get("org", "")
                if org_field:
                    mtr_result.at[idx, "org"] = org_field

                    # Overwrite ASN only if current one is invalid
                    if row["ASN"] in [None, "", "N/A", "AS???"]:
                        if org_field.startswith("AS"):
                            mtr_result.at[idx, "ASN"] = org_field.split(" ")[0]

        except Exception as e:
            print(f"[ERROR] Failed to fetch geolocation for {ip}: {e}")

        time.sleep(1.0)  # Light throttling

    return mtr_result

## Depreceated function. It uses ip-api.com.
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