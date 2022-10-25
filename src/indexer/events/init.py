from typing import List, NamedTuple
from apibara import IndexerRunner, Info, NewBlock, NewEvents
from apibara.model import EventFilter, BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

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
    inits = [
        {
            "event": decode_new_game_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Inits decoded.")
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
    print("Reset event")
    block_time = block.timestamp
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
    await info.storage.insert_many("reset", reset_docs)
    print("    Resets stored.")