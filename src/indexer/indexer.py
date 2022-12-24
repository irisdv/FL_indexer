import asyncio
import sys

from apibara import IndexerRunner, Info, NewBlock, NewEvents
from apibara.indexer.runner import IndexerRunnerConfiguration
from apibara.model import EventFilter

from indexer.events.transfers import handle_transfer_events
from indexer.events.init import handle_init_events, handle_reset_events
from indexer.events.harvest import handle_harvest_events
from indexer.events.destroy import handle_destroy_events
from indexer.events.build import handle_build_events
from indexer.events.repair import handle_repair_events
from indexer.events.move import handle_move_events
from indexer.events.fuel import handle_fuel_events
from indexer.events.claim import handle_claim_events

indexer_id = "indexer-all"
map_address = "0x052c936c5624517d671a6378ab0ede31e4c6d4584357ebb432bb1313af93599c"
frenslands_address = "0x0274f30014f7456d36b82728eb655f23dfe9ef0b7e0c6ca827052ab2d01a5d65"

async def handle_events(info: Info, block_events: NewEvents):
    """Handle a group of events grouped by block."""
    print(f"Received events for block {block_events.block.number}")

    block_time = block_events.block.timestamp
    print(f"Handle block events: Block No. {block_events.block.number} - {block_time}")

    for ev in block_events.events:
        print(ev.name, ev.transaction_hash.hex())
        if ev.name == "Transfer":
            await handle_transfer_events(info, block_events.block, ev)
        elif ev.name == "NewGame":
            await handle_init_events(info, block_events.block, ev)
        elif ev.name == "HarvestResource":
            await handle_harvest_events(info, block_events.block, ev)
        elif ev.name == "Destroy":
            await handle_destroy_events(info, block_events.block, ev)
        elif ev.name == "Build":
            await handle_build_events(info, block_events.block, ev)
        elif ev.name == "Repair":
            await handle_repair_events(info, block_events.block, ev)
        elif ev.name == "Move":
            await handle_move_events(info, block_events.block, ev)
        elif ev.name == "FuelProduction":
            await handle_fuel_events(info, block_events.block, ev)
        elif ev.name == "Claim":
            await handle_claim_events(info, block_events.block, ev)
        elif ev.name == "ResetGame":
            await handle_reset_events(info, block_events.block, ev)


async def handle_block(info: Info, block: NewBlock):
    # Store the block information in the database.
    print(f"Block: {block.new_head.number}")
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
            EventFilter.from_event_name(
                name="HarvestResource", address=frenslands_address
            ),
            EventFilter.from_event_name(
                name="Destroy", address=frenslands_address
            ),
            EventFilter.from_event_name(
                name="Build", address=frenslands_address
            ),
            EventFilter.from_event_name(
                name="Repair", address=frenslands_address
            ),
            EventFilter.from_event_name(
                name="Move", address=frenslands_address
            ),
            EventFilter.from_event_name(
                name="FuelProduction", address=frenslands_address
            ),
            EventFilter.from_event_name(
                name="Claim", address=frenslands_address
            ),
            EventFilter.from_event_name(
                name="ResetGame", address=frenslands_address
            ),
        ],
        index_from_block=300_000,
    )
    print("Initialization completed. Entering main loop.")

    await runner.run()