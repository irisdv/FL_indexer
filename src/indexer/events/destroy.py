from typing import List, NamedTuple
from apibara import IndexerRunner, Info, NewBlock, NewEvents
from apibara.model import EventFilter, BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

destroy_abi = {
    "name": "Destroy",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "owner","type": "felt"},
        {"name": "land_id", "type": "felt"},
        {"name": "timestamp", "type": "felt"},
        {"name": "building_type_id", "type": "felt"},
        {"name": "building_uid", "type": "felt"},
        {"name": "block_comp", "type": "felt"},
        {"name": "pos_x", "type": "felt"},
        {"name": "pos_y", "type": "felt"}
    ],
}

destroy_decoder = FunctionCallSerializer(
    abi=destroy_abi,
    identifier_manager=identifier_manager_from_abi([destroy_abi, uint256_abi]),
)

def decode_destroy_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return destroy_decoder.to_python(data)

async def handle_destroy_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("Destroy event")
    block_time = block.timestamp
    destroys = [
        {
            "event": decode_destroy_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Destroy decoded.")
    destroy_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].timestamp),
            "building_type_id": encode_int_as_bytes(tr["event"].building_type_id),
            "building_uid": encode_int_as_bytes(tr["event"].building_uid),
            "block_comp": encode_int_as_bytes(tr["event"].block_comp),
            "pos_x": encode_int_as_bytes(tr["event"].pos_x),
            "pos_y": encode_int_as_bytes(tr["event"].pos_y),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in destroys
    ]
    await info.storage.insert_many("destroy", destroy_docs)
    print("    Destroy stored.")