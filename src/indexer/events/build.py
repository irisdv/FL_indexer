from typing import List, NamedTuple
from apibara import Info
from apibara.model import BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

build_abi = {
    "name": "Build",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "owner","type": "felt"},
        {"name": "land_id", "type": "felt"},
        {"name": "time", "type": "felt"},
        {"name": "building_type_id", "type": "felt"},
        {"name": "building_uid", "type": "felt"},
        {"name": "block_comp", "type": "felt"},
        {"name": "pos_x", "type": "felt"},
        {"name": "pos_y", "type": "felt"}
    ],
}

build_decoder = FunctionCallSerializer(
    abi=build_abi,
    identifier_manager=identifier_manager_from_abi([build_abi, uint256_abi]),
)

def decode_harvest_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return build_decoder.to_python(data)

async def handle_build_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    block_time = block.timestamp
    event = decode_harvest_event(ev.data),
    print("Build event", event)
    builds = [
        {
            "event": decode_harvest_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Build decoded.", builds)

    build_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "building_type_id": encode_int_as_bytes(tr["event"].building_type_id),
            "building_uid": encode_int_as_bytes(tr["event"].building_uid),
            "block_comp": encode_int_as_bytes(tr["event"].block_comp),
            "pos_x": encode_int_as_bytes(tr["event"].pos_x),
            "pos_y": encode_int_as_bytes(tr["event"].pos_y),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
            "updated_at": block_time,
        }
        for tr in builds
    ]
    await info.storage.insert_many("build", build_docs)

    building_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "building_type_id": encode_int_as_bytes(tr["event"].building_type_id),
            "building_uid": encode_int_as_bytes(tr["event"].building_uid),
            "block_comp": encode_int_as_bytes(tr["event"].block_comp),
            "pos_x": encode_int_as_bytes(tr["event"].pos_x),
            "pos_y": encode_int_as_bytes(tr["event"].pos_y),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
            "status": "built",
            "decay": 0,
            "active_cycles": 0,
            "incoming_cycles": 1 if tr["event"].building_type_id > 2 else 0,
            "last_fuel": tr["event"].time,
            "updated_at": block_time,
        }
        for tr in builds
    ]
    await info.storage.insert_many("buildings", building_docs)

    # update map block 
    for tr in builds:
        land = await info.storage.find_one("lands", {"land_id": encode_int_as_bytes(tr["event"].land_id)})
        if land is not None:
            print('land', land)
            print('block', land['map'][tr["event"].pos_y][tr["event"].pos_x])
            land["map"][tr["event"].pos_y][tr["event"].pos_x] = tr["event"].block_comp
            print('map block updated', land["map"][tr["event"].pos_y][tr["event"].pos_x])
            await info.storage.find_one_and_update(
                "lands",
                {"land_id": encode_int_as_bytes(tr["event"].land_id)},
                {"$set": { 
                    "map": land["map"], 
                    "updated_at": block_time 
                }}
            )