import os
import asyncio
import json
from typing import Dict, Any

from azure.core.credentials import AzureSasCredential
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

EH_FQDN = "dcbb20251204.servicebus.windows.net"
EH_NAME = "eh1"
CONSUMER_GROUP = "$Default"

SAS_TOKEN = 'SharedAccessSignature ...'


BLOB_CONN_STR = "DefaultEndpointsProtocol=https;..."
BLOB_CONTAINER = "ces-checkpoint"

op_map = {"INS": "INSERT", "UPD": "UPDATE", "DEL": "DELETE"}

def _parse_ces_message(raw_body: str) -> Dict[str, Any]:

    env = json.loads(raw_body)                  


    inner = json.loads(env.get("data", "{}"))
    out = {
        "specversion": env.get("specversion"),
        "type": env.get("type"),
        "time": env.get("time"),
        "operation": env.get("operation"),
        "eventsource": inner.get("eventsource", {}),
        "eventrow": inner.get("eventrow", {}),
    }
    return out

async def main():
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_CONN_STR, 
        BLOB_CONTAINER
    )

    client = EventHubConsumerClient(
        fully_qualified_namespace=EH_FQDN,
        eventhub_name=EH_NAME,
        consumer_group=CONSUMER_GROUP,
        credential=AzureSasCredential(SAS_TOKEN),
        checkpoint_store=checkpoint_store,
    )

    print("starting... press Ctrl+C to stop.")


    async def on_event(partition_context, event):
        body = event.body_as_str(encoding="UTF-8")
        msg = _parse_ces_message(body)

        op = op_map.get(msg.get("operation"), msg.get("operation"))
        src = msg["eventsource"]
        row = msg["eventrow"]

        old_raw = row.get("old") or "{}"
        cur_raw = row.get("current") or "{}"
        old = json.loads(old_raw) if isinstance(old_raw, str) else old_raw
        current = json.loads(cur_raw) if isinstance(cur_raw, str) else cur_raw

        print(
            f"[{partition_context.partition_id}] "
            f"{op} {src.get('db')}.{src.get('schema')}.{src.get('tbl')} "
            f"PK={src.get('pkkey')} "
            f"time={msg.get('time')}"
        )
        if op == "UPDATE":
            changes = {
                k: (old.get(k), current.get(k))
                for k in set(old.keys()).union(current.keys())
                if old.get(k) != current.get(k)
            }
            print("  Δ:", changes)
        elif op == "INSERT":
            print("  +", current)
        elif op == "DELETE":
            print("  -", old)

        await partition_context.update_checkpoint(event)

    async with client:
        await client.receive(
            on_event=on_event,
            starting_position="-1",     # "-1" from earliest, "@latest" from latest
            max_wait_time=30            # seconds to wait for activity before yielding control
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stopping...")
