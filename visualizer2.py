import sys
import pandas as pd
import folium
from folium_arrow_icon import ArrowIcon
from datetime import timedelta
import os
import math


MERGE_WINDOW_SECONDS = 6


def signal_color(signal):
    try:
        signal = float(signal)
    except (ValueError, TypeError):
        return "gray"

    if signal == 0:
        return "black"

    if signal >= -50:
        return "green"
    elif signal >= -60:
        return "lightgreen"
    elif signal >= -70:
        return "orange"
    elif signal >= -80:
        return "red"
    else:
        return "darkred"
    
def relative_signal_level_color(signal, noise):
    try:
        level = float(signal) - float(noise)
    except (ValueError, TypeError):
        return "gray"
    
    if level == 0:
        return "black"
    
    if level >= 40:
        return "green"
    elif level >= 30:
        return "lightgreen"
    elif level >= 20:
        return "yellow"
    elif level >= 10:
        return "orange"
    elif level >= 5:
        return "red"
    else:
        return "darkred"
    
def bitrate_color(bitrate):
    try:
        bitrate = float(bitrate)
    except (ValueError, TypeError):
        return "gray"
    
    if bitrate == 0:
        return "black"
    
    if bitrate >= 50:
        return "green"
    elif bitrate >= 40:
        return "lightgreen"
    elif bitrate >= 30:
        return "yellow"
    elif bitrate >= 20:
        return "orange"
    elif bitrate >= 10:
        return "red"
    else:
        return "darkred"
    
def bitrate_color_halow(bitrate):
    try:
        bitrate = float(bitrate)
    except (ValueError, TypeError):
        return "gray"
    
    if bitrate == 0:
        return "black"
    
    if bitrate >= 29:
        return "green"
    elif bitrate >= 20:
        return "lightgreen"
    elif bitrate >= 15:
        return "yellow"
    elif bitrate >= 10:
        return "orange"
    elif bitrate >= 5:
        return "red"
    else:
        return "darkred"

def preprocess_data(df):
    """
    Merge samples taken at the same lat/lon within MERGE_WINDOW_SECONDS.
    Track min/max signal and noise.
    """

    df = df.copy()

    # Parse datetime
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    # Drop invalid rows
    df = df.dropna(subset=["datetime", "latitude", "longitude"])

    # Sort for time-window grouping
    df = df.sort_values(["latitude", "longitude", "datetime"])

    merged_rows = []

    # Group by exact location
    for (lat, lon), group in df.groupby(["latitude", "longitude"]):
        group = group.sort_values("datetime")

        current_bucket = []
        bucket_start_time = None

        for _, row in group.iterrows():
            if not current_bucket:
                current_bucket = [row]
                bucket_start_time = row["datetime"]
                continue

            if row["datetime"] - bucket_start_time <= timedelta(seconds=MERGE_WINDOW_SECONDS):
                current_bucket.append(row)
            else:
                merged_rows.append(merge_bucket(current_bucket))
                current_bucket = [row]
                bucket_start_time = row["datetime"]

        if current_bucket:
            merged_rows.append(merge_bucket(current_bucket))

    return pd.DataFrame(merged_rows)


def merge_bucket(rows):
    """
    Merge a list of rows into a single aggregated row.
    """
    dfb = pd.DataFrame(rows)

    merged = {
        "datetime": dfb["datetime"].min(),
        "latitude": dfb["latitude"].iloc[0],
        "longitude": dfb["longitude"].iloc[0],

        # Signal / noise extrema
        "signal_min": dfb["wireless_signal"].min(),
        "signal_max": dfb["wireless_signal"].max(),
        "noise_min": dfb["wireless_noisef"].min(),
        "noise_max": dfb["wireless_noisef"].max(),
        "wireless_txrate_max": dfb["wireless_txrate"].max(),
        "wireless_txrate_min": dfb["wireless_txrate"].min(),
        "wireless_rxrate_max": dfb["wireless_rxrate"].max(),
        "wireless_rxrate_min": dfb["wireless_rxrate"].min(),

        # Representative values (mean)
        "wireless_signal": dfb["wireless_signal"].mean(),
        "wireless_noisef": dfb["wireless_noisef"].mean(),
        "wireless_rssi": dfb["wireless_rssi"].mean(),
        "wireless_txpower": dfb["wireless_txpower"].mean(),
        "wireless_distance": dfb["wireless_distance"].mean(),
        "wireless_ccq": dfb["wireless_ccq"].mean(),
        "wireless_txrate": dfb["wireless_txrate"].mean(),
        "wireless_rxrate": dfb["wireless_rxrate"].mean(),

        # Static / categorical fields (first value)
        "wireless_channel": dfb["wireless_channel"].iloc[0],
        "wireless_frequency": dfb["wireless_frequency"].iloc[0],
        "wireless_opmode": dfb["wireless_opmode"].iloc[0],

        "samples_merged": len(dfb)
    }

    return merged


def generate_map(csv_file, output_html="wireless_map.html"):
    df = pd.read_csv(csv_file)

    #df = preprocess_data(df)

    #if df.empty:
    #    print("No valid data after preprocessing.")
    #    return

    center_lat = df["latitude"].mean()
    center_lon = df["longitude"].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles="OpenStreetMap"
    )

    # Add markers to map
    for _, row in df.iterrows():
        popup_html=""

        popup_html = f"""
        <b>Date/Time:</b> {row['datetime']}<br>
        <b>Channel:</b> {row['channel']}<br>
        <b>Frequency:</b> {row['frequency']} MHz<br>
        <b>Rx Bitrate:</b> {row['rxbitrate']} Mb/s<br>
        <b>Tx Bitrate:</b> {row['txbitrate']} Mb/s<br>
        <b>Signal:</b> {row['signalLevel']} dBm<br>
        <b>Noise:</b> {row['noiseLevel']} dBm<br>
        <b>Heading:</b> {row['heading']} degrees<br>
        <b>Tx Power</b> {row['txpower']} dBm<br>
        """

        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            icon = ArrowIcon (
                10,
                math.radians(row['heading']),
                color=bitrate_color_halow(row["txbitrate"])
            ),
            popup=popup_html
        ).add_to(m)

    m.save(output_html)
    print(f"Map saved to {output_html}")


def main(csv_path: str, output_path: str):
    if os.path.isdir(csv_path): #if a directory is passed, recursively iterate through all subdirectories, and generate html for every csv file found.
        for f in os.scandir(csv_path):
            if f.is_file():
                if f.path.split('.')[-1].strip() == "csv":
                    generate_map(f.path, f.path + '.html')
            else:
                main(f.path, f.path)
    else:
        generate_map(csv_path, output_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python visualizer.py <input.csv> [output.html] OR <inputdir>")
        sys.exit(1)

    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "wireless_map.html"

    main(csv_path, output_path)
