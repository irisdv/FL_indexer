from typing import List, NamedTuple
from apibara import Info
from apibara.model import BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

move_abi = {
    "name": "Move",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "owner","type": "felt"},
        {"name": "land_id", "type": "felt"},
        {"name": "time", "type": "felt"},
        {"name": "infra_type", "type": "felt"},
        {"name": "infra_type_id", "type": "felt"},
        {"name": "infra_uid", "type": "felt"},
        {"name": "pos_x", "type": "felt"},
        {"name": "pos_y", "type": "felt"},
        {"name": "new_pos_x", "type": "felt"},
        {"name": "new_pos_y", "type": "felt"}
    ],
}

move_decoder = FunctionCallSerializer(
    abi=move_abi,
    identifier_manager=identifier_manager_from_abi([move_abi, uint256_abi]),
)

def decode_move_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return move_decoder.to_python(data)

async def handle_move_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("Move event")
    block_time = block.timestamp
    moves = [
        {
            "event": decode_move_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Move decoded.")
    move_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "infra_type": encode_int_as_bytes(tr["event"].infra_type),
            "infra_type_id": encode_int_as_bytes(tr["event"].infra_type_id),
            "infra_uid": encode_int_as_bytes(tr["event"].infra_uid),
            "pos_x": encode_int_as_bytes(tr["event"].pos_x),
            "pos_y": encode_int_as_bytes(tr["event"].pos_y),
            "new_pos_x": encode_int_as_bytes(tr["event"].new_pos_x),
            "new_pos_y": encode_int_as_bytes(tr["event"].new_pos_y),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in moves
    ]
    await info.storage.insert_many("moves", move_docs)
    print("    Move stored.")

    for tr in moves:
        await info.storage.find_one_and_update(
            "buildings",
            {
                "building_uid": encode_int_as_bytes(tr["event"].building_uid),
                "land_id": encode_int_as_bytes(tr["event"].land_id),
            },
            {"$set": {
                "pos_x": encode_int_as_bytes(tr["event"].new_pos_x),
                "pos_y": encode_int_as_bytes(tr["event"].new_pos_y),
                "updated_at": block_time,
            }}
        )
        print("    Buildings updated with new position.")

        # * Update map block
        land = await info.storage.find_one("lands", {"land_id": encode_int_as_bytes(tr["event"].land_id)})
        if land is not None:
            new_block = land["map"][tr["event"].pos_y][tr["event"].pos_x]
            land["map"][tr["event"].new_pos_y][tr["event"].new_pos_x] = new_block
            land["map"][tr["event"].pos_y][tr["event"].pos_x] = 0
            await info.storage.find_one_and_update(
                "lands",
                {"land_id": encode_int_as_bytes(tr["event"].land_id)},
                {"$set": { 
                    "map": land["map"], 
                    "updated_at": block_time 
                }}
            )