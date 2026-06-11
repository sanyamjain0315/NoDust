#!/usr/bin/env python3
"""
A live plotting script that consumes sensor telemetry from Kafka
and dynamically plots concentrations for a targeted site in real-time.
"""

import os
import json
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "site-sensor-raw")
TARGET_SITE = os.getenv("TARGET_SITE", "SITE_01")  # The specific site to monitor
MAX_DATAPoints = 50                                # Max points to show on x-axis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Initialize Kafka Consumer
# Note: auto_offset_reset='latest' ensures we plot live data, not historical data
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)

# ------------------------------------------------------------------
# Data Structures for Plotting
# ------------------------------------------------------------------
# Holds lists of timestamps and concentrations per pollutant type
# e.g., {"PM2.5": {"x": [], "y": []}, "NO2": {"x": [], "y": []}}
plot_data = {}

# Setup Matplotlib Figure
fig, ax = plt.subplots(figsize=(10, 6))
lines = {}  # Stores line objects for animation updates

ax.set_title(f"Real-Time Air Quality Monitor: {TARGET_SITE}", fontsize=14, fontweight='bold')
ax.set_xlabel("Timestamp (Local Time)")
ax.set_ylabel("Concentration (µg/m³)")
ax.grid(True, linestyle="--", alpha=0.6)

# ------------------------------------------------------------------
# Core Logic
# ------------------------------------------------------------------

def consume_kafka_messages():
    """
    Checks Kafka for new messages without blocking the UI thread.
    Returns a list of matching messages found in this check interval.
    """
    messages = []
    # Use consumer.poll to fetch available data without locking up Matplotlib
    records = consumer.poll(timeout_ms=50) 
    
    for topic_partition, partition_records in records.items():
        for record in partition_records:
            msg = record.value
            if msg.get("site_id") == TARGET_SITE:
                messages.append(msg)
    return messages


def animate(frame):
    """
    Matplotlib animation callback loop. Updates data arrays and redraws lines.
    """
    new_msgs = consume_kafka_messages()
    
    if not new_msgs:
        return lines.values()  # Nothing new, return existing lines

    for msg in new_msgs:
        pollutant = msg["pollutant_type"]
        conc = msg["concentration"]
        
        # Parse ISO timestamp to a displayable local datetime object
        # Strips out 'Z' offset syntax if present for compatibility
        ts_str = msg["timestamp"].replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str).astimezone(None) 
        
        # Initialize dictionary keys dynamically if a new pollutant type appears
        if pollutant not in plot_data:
            plot_data[pollutant] = {"x": [], "y": []}
            # Create a new visual line on the plot for this pollutant
            lines[pollutant], = ax.plot([], [], label=pollutant, linewidth=2)
            ax.legend(loc="upper left")

        # Append new coordinates
        plot_data[pollutant]["x"].append(dt)
        plot_data[pollutant]["y"].append(conc)

        # Cap the history so the graph doesn't become squished or slow down
        if len(plot_data[pollutant]["x"]) > MAX_DATAPoints:
            plot_data[pollutant]["x"].pop(0)
            plot_data[pollutant]["y"].pop(0)

    # Refresh data source for every tracked line
    for pollutant, tracking in plot_data.items():
        lines[pollutant].set_data(tracking["x"], tracking["y"])

    # Dynamically adjust boundaries to fit new data ranges perfectly
    ax.relim()
    ax.autoscale_view()
    
    # Rotate time labels so they don't overlap on the bottom axis
    fig.autofmt_xdate()
    
    return lines.values()


def main():
    logging.info(f"Starting consumer for topic '{TOPIC}'. Filtering for site: {TARGET_SITE}")
    
    # Use FuncAnimation to trigger the UI redraw every 200 milliseconds
    # blit=False is used because our axes scale automatically 
    ani = animation.FuncAnimation(fig, animate, interval=200, blit=False, cache_frame_data=False)
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Closing live plot visualization.")