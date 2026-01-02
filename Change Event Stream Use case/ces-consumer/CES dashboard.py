import streamlit as st
import pandas as pd
import plotly.express as px
import asyncio
import json
from datetime import datetime
from azure.core.credentials import AzureSasCredential
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

# Azure Event Hub config
EH_FQDN = "dcbb20251204.servicebus.windows.net"
EH_NAME = "eh1"
CONSUMER_GROUP = "$Default"
SAS_TOKEN = ''
BLOB_CONN_STR = ""
BLOB_CONTAINER = "ces-checkpoint"

# Session state for data
if "sensors" not in st.session_state:
    st.session_state["sensors"] = {}

if "readings" not in st.session_state:
    st.session_state["readings"] = []

if "debug_log" not in st.session_state:
    st.session_state["debug_log"] = []

# Parsing function
def _parse_ces_message(raw_body: str):
    env = json.loads(raw_body)
    inner = json.loads(env.get("data", "{}"))
    return {
        "operation": env.get("operation"),
        "eventsource": inner.get("eventsource", {}),
        "eventrow": inner.get("eventrow", {})
    }

# Async EventHub consumer
async def on_event(partition_context, event):
    body = event.body_as_str(encoding="UTF-8")
    msg = _parse_ces_message(body)
    source = msg["eventsource"]
    row = msg["eventrow"]
    operation = msg["operation"]

    tbl = source.get("tbl")
    debug_entry = {
        "partition": partition_context.partition_id,
        "table": tbl,
        "operation": operation,
        "timestamp": datetime.utcnow().isoformat(),
        "row": row
    }
    st.session_state["debug_log"].insert(0, debug_entry)
    st.session_state["debug_log"] = st.session_state["debug_log"][:100]  # max 100 ostatnich zdarzeń

    if tbl == "Sensors":
        current_raw = row.get("current")
        if isinstance(current_raw, str):
            current = json.loads(current_raw)
        else:
            current = current_raw
        sensor_id = int(current["SensorID"])
        st.session_state["sensors"][sensor_id] = {
            "SensorID": sensor_id,
            "SensorName": current["SensorName"],
            "Location": current["Location"],
            "Model": current["Model"],
            "InstallDate": current["InstallDate"],
            "IsActive": current["IsActive"] in ("1", 1, True)
        }

    elif tbl == "TemperatureReadings":
        current_raw = row.get("current")
        if isinstance(current_raw, str):
            current = json.loads(current_raw)
        else:
            current = current_raw
        st.session_state["readings"].append({
            "ReadingID": int(current["ReadingID"]),
            "SensorID": int(current["SensorID"]),
            "TemperatureCelsius": float(current["TemperatureCelsius"]),
            "RecordedAt": current["RecordedAt"]
        })

# RECEIVE bez checkpointów
async def receive_events():
    client = EventHubConsumerClient(
        fully_qualified_namespace=EH_FQDN,
        eventhub_name=EH_NAME,
        consumer_group=CONSUMER_GROUP,
        credential=AzureSasCredential(SAS_TOKEN)
    )
    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="-1",
            max_wait_time=30
        )

# Start the consumer in the background
def start_event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(receive_events())


if "eventhub_started" not in st.session_state:
    st.session_state.eventhub_started = True
    start_event_loop()

# --- UI Layout ---
st.set_page_config(layout="wide")
st.title("🌡️ Real-Time Sensor Dashboard")

col1, col2 = st.columns(2)

sensor_df = pd.DataFrame.from_dict(st.session_state["sensors"], orient="index")

with col1:
    st.subheader("📋 Lista sensorów")
    if not sensor_df.empty and "SensorID" in sensor_df.columns:
        st.dataframe(sensor_df.sort_values("SensorID"), use_container_width=True)
    else:
        st.info("Oczekiwanie na dane o sensorach z Event Huba...")

with col2:
    st.subheader("📊 Statystyki")
    if not sensor_df.empty:
        active = sensor_df["IsActive"].sum()
        avg_temp = pd.DataFrame(st.session_state["readings"])["TemperatureCelsius"].mean()
        st.metric("Aktywne sensory", active)
        st.metric("Średnia temperatura", f"{avg_temp:.2f} °C" if avg_temp else "Brak danych")
    else:
        st.info("Brak danych do statystyk – czekam na pierwsze zdarzenia.")

st.subheader("📈 Temperatury w czasie")
if not sensor_df.empty:
    selected_id = st.selectbox("Wybierz SensorID:", options=sensor_df["SensorID"])
    readings_df = pd.DataFrame(st.session_state["readings"])
    readings_df["RecordedAt"] = pd.to_datetime(readings_df["RecordedAt"])
    sensor_readings = readings_df[readings_df["SensorID"] == selected_id]
    fig = px.line(sensor_readings, x="RecordedAt", y="TemperatureCelsius", title=f"Sensor {selected_id} - Temperatura")
    fig.update_traces(mode="lines+markers")
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Brak danych do wykresu – czekam na pierwsze pomiary.")

st.subheader("🚨 Alerty temperatury")
readings_df = pd.DataFrame(st.session_state["readings"])
if not readings_df.empty:
    high_temp_df = readings_df[readings_df["TemperatureCelsius"] > 45.0]
    if not high_temp_df.empty:
        st.error(f"Wykryto {len(high_temp_df)} pomiarów powyżej 45°C!")
        st.dataframe(high_temp_df, use_container_width=True)
    else:
        st.success("Brak krytycznych temperatur.")
else:
    st.info("Brak odczytów temperatury – czekam na dane z Event Huba.")

# --- DEBUG LOG ---
st.subheader("🛠️ Ostatnie zdarzenia (debug)")
debug_df = pd.DataFrame(st.session_state["debug_log"])
if not debug_df.empty:
    st.dataframe(debug_df.head(20), use_container_width=True)
else:
    st.info("Brak logów zdarzeń.")