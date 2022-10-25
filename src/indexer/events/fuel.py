from typing import List, NamedTuple
from apibara import IndexerRunner, Info, NewBlock, NewEvents
from apibara.model import EventFilter, BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

fuel_abi = {
    "name": "FuelProduction",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "owner","type": "felt"},
        {"name": "land_id", "type": "felt"},
        {"name": "time", "type": "felt"},
        {"name": "building_type_id", "type": "felt"},
        {"name": "building_uid", "type": "felt"},
        {"name": "pos_x", "type": "felt"},
        {"name": "pos_y", "type": "felt"},
        {"name": "nb_blocks", "type": "felt"}
    ],
}

fuel_decoder = FunctionCallSerializer(
    abi=fuel_abi,
    identifier_manager=identifier_manager_from_abi([fuel_abi, uint256_abi]),
)

def decode_fuel_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return fuel_decoder.to_python(data)

async def handle_fuel_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("Fuel Production event")
    block_time = block.timestamp
    fuels = [
        {
            "event": decode_fuel_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Fuel decoded.")
    fuel_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "building_type_id": encode_int_as_bytes(tr["event"].building_type_id),
            "building_uid": encode_int_as_bytes(tr["event"].building_uid),
            "pos_x": encode_int_as_bytes(tr["event"].pos_x),
            "pos_y": encode_int_as_bytes(tr["event"].pos_y),
            "nb_blocks": encode_int_as_bytes(tr["event"].nb_blocks),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in fuels
    ]
    await info.storage.insert_many("fuel", fuel_docs)
    print("    Fuel production stored.")