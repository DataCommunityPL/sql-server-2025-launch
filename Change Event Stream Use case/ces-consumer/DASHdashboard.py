import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import asyncio
import json
import threading
from datetime import datetime
from azure.core.credentials import AzureSasCredential
from azure.eventhub.aio import EventHubConsumerClient

# Azure Event Hub config
EH_FQDN = "bialykruk2008.servicebus.windows.net"
EH_NAME = "eh1"
CONSUMER_GROUP = "$Default"
SAS_TOKEN = 'SharedAccessSignature ...'

# Global data storage
debug_log = []
sensors = {}
readings = []

def _parse_ces_message(raw_body: str):
    env = json.loads(raw_body)
    inner = json.loads(env.get("data", "{}"))
    return {
        "operation": env.get("operation"),
        "eventsource": inner.get("eventsource", {}),
        "eventrow": inner.get("eventrow", {})
    }

async def on_event(partition_context, event):
    try:
        body = event.body_as_str(encoding="UTF-8")
        msg = _parse_ces_message(body)
        source = msg["eventsource"]
        row = msg["eventrow"]
        tbl = source.get("tbl")

        current_raw = row.get("current")
        if not current_raw or current_raw.strip() in ("", "{}", None):
            return

        try:
            current = json.loads(current_raw) if isinstance(current_raw, str) else current_raw
        except json.JSONDecodeError:
            return

        if not isinstance(current, dict) or "SensorID" not in current:
            return

        debug_log.insert(0, {
            "partition": partition_context.partition_id,
            "table": tbl,
            "operation": msg["operation"],
            "timestamp": datetime.utcnow().isoformat(),
            "row": json.dumps(current)
        })
        if len(debug_log) > 100:
            debug_log.pop()

        if tbl == "Sensors":
            sensor_id = int(current["SensorID"])
            sensors[sensor_id] = {
                "SensorID": sensor_id,
                "SensorName": current["SensorName"],
                "Location": current["Location"],
                "Model": current["Model"],
                "InstallDate": current["InstallDate"],
                "IsActive": current["IsActive"] in ("1", 1, True)
            }

        elif tbl == "TemperatureReadings":
            readings.append({
                "ReadingID": int(current["ReadingID"]),
                "SensorID": int(current["SensorID"]),
                "TemperatureCelsius": float(current["TemperatureCelsius"]),
                "RecordedAt": current["RecordedAt"]
            })

        await partition_context.update_checkpoint(event)

    except Exception as e:
        print(f"Error processing event: {e}")

async def receive_events():
    client = EventHubConsumerClient(
        fully_qualified_namespace=EH_FQDN,
        eventhub_name=EH_NAME,
        consumer_group=CONSUMER_GROUP,
        credential=AzureSasCredential(SAS_TOKEN)
    )
    async with client:
        await client.receive(on_event=on_event, starting_position="-1", max_wait_time=30)

def start_eventhub_client():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(receive_events())

if not hasattr(globals(), "_eventhub_started"):
    _eventhub_started = True
    threading.Thread(target=start_eventhub_client, daemon=True).start()

# Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Sensor Dashboard"

app.layout = dbc.Container([
    html.H2("🌡️ Real-Time Sensor Dashboard"),
    html.Hr(),
    dbc.Row([
        dbc.Col([
            html.H5("📋 Lista sensorów"),
            dcc.Interval(id="interval-static", interval=10000, n_intervals=0),
            html.Div(id="sensor-table")
        ], width=6),
        dbc.Col([
            html.H5("📊 Statystyki"),
            html.Div(id="stats")
        ], width=6)
    ]),
    html.Hr(),
    html.H5("📈 Temperatury w czasie"),
    dcc.Dropdown(id="sensor-dropdown"),
    dcc.Interval(id="interval-live", interval=3000, n_intervals=0),
    html.Div(id="sensor-description", style={"marginTop": "10px"}),
    dcc.Graph(id="temp-chart"),
    html.Hr(),
    html.H5("🚨 Alerty temperatury"),
    html.Div(id="alerts"),
    html.Hr(),
    html.H5("🛠️ Debug"),
    html.Div(id="debug-log")
])

# 📊 Callback: Static data (co 10s)
@app.callback(
    Output("sensor-table", "children"),
    Output("sensor-dropdown", "options"),
    Output("stats", "children"),
    Output("alerts", "children"),
    Output("debug-log", "children"),
    Input("interval-static", "n_intervals")
)
def update_static(n):
    sensor_df = pd.DataFrame.from_dict(sensors, orient="index")
    readings_df = pd.DataFrame(readings)

    sensor_table = html.Div("Brak danych") if sensor_df.empty else dbc.Table.from_dataframe(sensor_df, striped=True, bordered=True, hover=True)

    dropdown_options = [
        {"label": f"{row['SensorName']} ({row['SensorID']})", "value": row["SensorID"]}
        for _, row in sensor_df.iterrows()
    ] if not sensor_df.empty else []

    stats = [html.Div("Brak danych")]
    if not sensor_df.empty:
        stats = [html.Div(f"Aktywne sensory: {sensor_df['IsActive'].sum()}")]
        if not readings_df.empty:
            avg = readings_df["TemperatureCelsius"].mean()
            stats.append(html.Div(f"Średnia temperatura: {avg:.2f} °C"))

    alerts = html.Div("Brak danych")
    if not readings_df.empty:
        high = readings_df[readings_df["TemperatureCelsius"] > 22.0]
        alerts = dbc.Alert(
            f"Wykryto {len(high)} pomiarów powyżej 22°C!" if not high.empty else "Brak krytycznych temperatur.",
            color="danger" if not high.empty else "success"
        )

    debug_df = pd.DataFrame(debug_log[:20])
    debug = html.Div("Brak logów") if debug_df.empty else dbc.Table.from_dataframe(debug_df, striped=True, bordered=True, hover=True)

    return sensor_table, dropdown_options, stats, alerts, debug

# 📈 Callback: Wykres i opis (co 3s)
@app.callback(
    Output("temp-chart", "figure"),
    Output("sensor-description", "children"),
    Input("interval-live", "n_intervals"),
    Input("sensor-dropdown", "value"),
    State("sensor-dropdown", "options")
)
def update_graph(n, selected_sensor, options):
    readings_df = pd.DataFrame(readings)
    fig = px.line()
    desc = html.Div("Wybierz sensor")

    if not selected_sensor and options:
        selected_sensor = options[0]["value"]

    if selected_sensor and not readings_df.empty:
        df = readings_df[readings_df["SensorID"] == selected_sensor].copy()
        if not df.empty:
            df["RecordedAt"] = pd.to_datetime(df["RecordedAt"], errors="coerce")
            df = df.sort_values("RecordedAt")
            fig = px.line(df, x="RecordedAt", y="TemperatureCelsius", markers=True,
                          title=f"Sensor {selected_sensor} – Temperatura (Live)")

        s = sensors.get(selected_sensor)
        if s:
            desc = html.Div([
                html.P(f"Nazwa: {s['SensorName']}")
            ])

    return fig, desc

if __name__ == "__main__":
    app.run(debug=True, port=8050)