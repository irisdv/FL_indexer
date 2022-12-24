from typing import List, NamedTuple
from apibara import Info
from apibara.model import BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi, create_map_array

newGame_abi = {
    "name": "NewGame",
    "type": "event",
    "keys": [],
    "outputs": [
      { "name": "owner", "type": "felt" },
      { "name": "land_id", "type": "felt" },
      { "name": "time", "type": "felt" }
    ],
}

newGame_decoder = FunctionCallSerializer(
    abi=newGame_abi,
    identifier_manager=identifier_manager_from_abi([newGame_abi, uint256_abi]),
)

def decode_new_game_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return newGame_decoder.to_python(data)

async def handle_init_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("NewGame event")
    block_time = block.timestamp
    # event = decode_new_game_event(ev.data),
    # print('event init ----------- ', event)

    inits = [
        {
            "event": decode_reset_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Inits decoded.", inits)

    init_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in inits
    ]

    await info.storage.insert_many("inits", init_docs)
    print("    Inits stored.")

    cabins = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "building_type_id": encode_int_as_bytes(1),
            "building_uid": encode_int_as_bytes(1),
            "block_comp": encode_int_as_bytes(20100011199),
            "pos_x": encode_int_as_bytes(20),
            "pos_y": encode_int_as_bytes(8),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
            "status": "built",
            "decay": 100,
            "active_cycles": 0,
            "incoming_cycles": 0,
            "last_fuel": tr["event"].time,
            "updated_at": block_time,
        }
        for tr in inits
    ]
    await info.storage.insert_many("buildings", cabins)
    print("    Cabin stored.")    

    # create map
    for tr in inits:
        map = create_map_array()
        await info.storage.insert_one("lands", {
            "map": map, 
            "land_id": encode_int_as_bytes(tr["event"].land_id), 
            "transaction_hash": tr["transaction_hash"],
            "time": encode_int_as_bytes(tr["event"].time),
            "timestamp": block_time, 
            "updated_at": block_time,
        })
        print("    Initialized lands.")


reset_abi = {
    "name": "ResetGame",
    "type": "event",
    "keys": [],
    "outputs": [
      { "name": "owner", "type": "felt" },
      { "name": "time", "type": "felt" },
      { "name": "land_id", "type": "felt" },
    ],
}

reset_decoder = FunctionCallSerializer(
    abi=reset_abi,
    identifier_manager=identifier_manager_from_abi([reset_abi, uint256_abi]),
)

def decode_reset_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return reset_decoder.to_python(data)

async def handle_reset_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    block_time = block.timestamp
    print("Reset event")
    resets = [
        {
            "event": decode_reset_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Resets decoded.")

    reset_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "time": encode_int_as_bytes(tr["event"].time),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in resets
    ]
    await info.storage.insert_many("resets", reset_docs)

    # Delete all buildings that are not cabin
    for tr in resets:
        await info.storage.delete_many(
            "buildings",
            {
                "status": "built",
                "building_uid": {"$ne": encode_int_as_bytes(1)},
                "land_id": encode_int_as_bytes(tr["event"].land_id),
            },
        )

        # Update decay cabin
        await info.storage.find_one_and_update(
            "buildings",
            {
                "building_uid": encode_int_as_bytes(1),
                "land_id": encode_int_as_bytes(tr["event"].land_id),
            },
            {"$set": { "decay": 100, "updated_at": block_time }}
        )

        # Reset map
        map = create_map_array()
        await info.storage.find_one_and_update(
            "lands",
            {"land_id": encode_int_as_bytes(tr["event"].land_id)},
            {"$set": { 
                "map": map, 
                "updated_at": block_time 
            }}
        )