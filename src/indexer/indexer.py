import asyncio
import sys
from argparse import ArgumentParser
from typing import List, NamedTuple
from datetime import datetime

from apibara import IndexerRunner, Info, NewBlock, NewEvents
from apibara.indexer.runner import IndexerRunnerConfiguration
from apibara.model import EventFilter
from pymongo import MongoClient

from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi

indexer_id = "indexer-transfer"
map_address = "0x052c936c5624517d671a6378ab0ede31e4c6d4584357ebb432bb1313af93599c"
frenslands_address = "0x060363b467a2b8d409234315babe6be180020e0bb65d708c0d09be6fd3691a2f"

uint256_abi = {
    "name": "Uint256",
    "type": "struct",
    "size": 2,
    "members": [
        {"name": "low", "offset": 0, "type": "felt"},
        {"name": "high", "offset": 1, "type": "felt"},
    ],
}

transfer_abi = {
    "name": "Transfer",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "from_address", "type": "felt"},
        {"name": "to_address", "type": "felt"},
        {"name": "token_id", "type": "Uint256"},
    ],
}
transfer_decoder = FunctionCallSerializer(
    abi=transfer_abi,
    identifier_manager=identifier_manager_from_abi([transfer_abi, uint256_abi]),
)

# New Game init events
newGame_abi = {
    "name": "NewGame",
    "type": "event",
    "keys": [],
    "outputs": [
      { "name": "owner", "type": "felt" },
      { "name": "land_id", "type": "felt" },
      { "name": "biome_id", "type": "felt" },
      { "name": "time", "type": "felt" }
    ],
}
newGame_decoder = FunctionCallSerializer(
    abi=newGame_abi,
    identifier_manager=identifier_manager_from_abi([newGame_abi, uint256_abi]),
)


def decode_transfer_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return transfer_decoder.to_python(data)

def decode_new_game_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return newGame_decoder.to_python(data)

def encode_int_as_bytes(n: int) -> bytes:
    """Encode an integer to bytes so that it can be stored in a db."""
    data = n.to_bytes(32, "big")
    print(data)
    return n.to_bytes(32, "big")


async def handle_events(info: Info, block_events: NewEvents):
    """Handle a group of events grouped by block."""
    print(f"Received events for block {block_events.block.number}")

    block_time = block_events.block.timestamp
    print(f"Handle block events: Block No. {block_events.block.number} - {block_time}")

    for ev in block_events.events:
        print(ev.name, ev.transaction_hash.hex())
        if ev.name == "Transfer":
            print("Transfer event")

            transfers = [
                {
                    "event": decode_transfer_event(ev.data),
                    "transaction_hash": ev.transaction_hash,
                }
                # for event in block_events.events
            ]
            print("    Transfers decoded.")

            transfers_docs = [
                {
                    "from_address": encode_int_as_bytes(tr["event"].from_address),
                    "to_address": encode_int_as_bytes(tr["event"].to_address),
                    "token_id": encode_int_as_bytes(tr["event"].token_id),
                    "transaction_hash": tr["transaction_hash"],
                    "timestamp": block_time,
                }
                for tr in transfers
            ]

            # Now store to the database.
            await info.storage.insert_many("transfers", transfers_docs)
            print("    Transfers stored.")

            new_token_owner = dict()
            for transfer in transfers:
                new_token_owner[transfer["event"].token_id] = transfer["event"].to_address

            for token_id, new_owner in new_token_owner.items():
                token_id = encode_int_as_bytes(token_id)
                # Use upsert to store the token if it's the first
                # time indexing it.
                await info.storage.find_one_and_replace(
                    "tokens",
                    {"token_id": token_id},
                    {
                        "token_id": token_id,
                        "owner": encode_int_as_bytes(new_owner),
                        "updated_at": block_time,
                    },
                    upsert=True,
                )
            print("    Owners updated.")

        elif ev.name == "NewGame":
            print("NewGame event")
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
                    "biome_id": encode_int_as_bytes(tr["event"].biome_id),
                    "transaction_hash": tr["transaction_hash"],
                    "timestamp": block_time,
                }
                for tr in inits
            ]
            await info.storage.insert_many("inits", init_docs)
            print("    Inits stored.")


async def handle_block(info: Info, block: NewBlock):
    # Store the block information in the database.
    block = {
        "number": block.new_head.number,
        "hash": block.new_head.hash,
        "timestamp": block.new_head.timestamp.isoformat(),
    }
    await info.storage.insert_one("blocks", block)


async def run_indexer(server_url=None, mongo_url=None, restart=None):
    print("Starting Apibara indexer")
    
    runner = IndexerRunner(
        config=IndexerRunnerConfiguration(
            apibara_url=server_url,
            apibara_ssl=True,
            storage_url=mongo_url,
        ),
        reset_state=restart,
        indexer_id=indexer_id,
        new_events_handler=handle_events,
    )

    runner.set_context({
        "network": "starknet-goerli"
    })

    runner.add_block_handler(handle_block)

    runner.add_event_filters(
        filters=[
            EventFilter.from_event_name(
                name="Transfer",
                address="0x052c936c5624517d671a6378ab0ede31e4c6d4584357ebb432bb1313af93599c",
            ),
            EventFilter.from_event_name(
                name="NewGame", address=frenslands_address
            ),
        ],
        index_from_block=300_000,
    )

    print("Initialization completed. Entering main loop.")

    await runner.run()