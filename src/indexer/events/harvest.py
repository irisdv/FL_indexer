from typing import List, NamedTuple
from apibara import IndexerRunner, Info, NewBlock, NewEvents
from apibara.model import EventFilter, BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

harvest_abi = {
    "name": "HarvestResource",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "owner","type": "felt"},
        {"name": "land_id", "type": "felt"},
        {"name": "time", "type": "felt"},
        {"name": "resource_type", "type": "felt"},
        {"name": "resource_uid", "type": "felt"},
        {"name": "block_comp", "type": "felt"},
        {"name": "pos_x", "type": "felt"},
        {"name": "pos_y", "type": "felt"}
    ],
}

harvest_decoder = FunctionCallSerializer(
    abi=harvest_abi,
    identifier_manager=identifier_manager_from_abi([harvest_abi, uint256_abi]),
)

def decode_harvest_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return harvest_decoder.to_python(data)

async def handle_harvest_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("Harvest event")
    block_time = block.timestamp
    harvests = [
        {
            "event": decode_harvest_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Harvest decoded.")
    harvest_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "resource_type": encode_int_as_bytes(tr["event"].resource_type),
            "resource_uid": encode_int_as_bytes(tr["event"].resource_uid),
            "block_comp": encode_int_as_bytes(tr["event"].block_comp),
            "pos_x": encode_int_as_bytes(tr["event"].pos_x),
            "pos_y": encode_int_as_bytes(tr["event"].pos_y),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in harvests
    ]
    await info.storage.insert_many("harvest", harvest_docs)
    print("    Harvests stored.")