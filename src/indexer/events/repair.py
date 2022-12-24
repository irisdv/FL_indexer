from typing import List, NamedTuple
from apibara import Info
from apibara.model import BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

repair_abi = {
    "name": "Repair",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "owner","type": "felt"},
        {"name": "land_id", "type": "felt"},
        {"name": "time", "type": "felt"},
        {"name": "building_type_id", "type": "felt"},
        {"name": "building_uid", "type": "felt"},
        {"name": "pos_x", "type": "felt"},
        {"name": "pos_y", "type": "felt"}
    ],
}

repair_decoder = FunctionCallSerializer(
    abi=repair_abi,
    identifier_manager=identifier_manager_from_abi([repair_abi, uint256_abi]),
)

def decode_repair_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return repair_decoder.to_python(data)

async def handle_repair_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("Repair event")
    block_time = block.timestamp
    repairs = [
        {
            "event": decode_repair_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Repairs decoded.")
    repair_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "building_type_id": encode_int_as_bytes(tr["event"].building_type_id),
            "building_uid": encode_int_as_bytes(tr["event"].building_uid),
            "pos_x": encode_int_as_bytes(tr["event"].pos_x),
            "pos_y": encode_int_as_bytes(tr["event"].pos_y),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in repairs
    ]
    await info.storage.insert_many("repairs", repair_docs)
    print("    Repairs stored.")

    # update cabin in buildings 
    for tr in repairs:
        await info.storage.find_one_and_update(
            "buildings",
            {
                "building_uid": encode_int_as_bytes(tr["event"].building_uid), 
                "land_id": encode_int_as_bytes(tr["event"].land_id)
            },
            {"$set": {"decay": 0, "updated_at": block_time}},
        )