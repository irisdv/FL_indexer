from typing import List, NamedTuple
from apibara import Info
from apibara.model import BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

claim_abi = {
    "name": "Claim",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "owner","type": "felt"},
        {"name": "land_id", "type": "felt"},
        {"name": "time", "type": "felt"},
        {"name": "block_number", "type": "felt"},
        {"name": "building_counter", "type": "felt"}
    ],
}

claim_decoder = FunctionCallSerializer(
    abi=claim_abi,
    identifier_manager=identifier_manager_from_abi([claim_abi, uint256_abi]),
)

def decode_claim_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return claim_decoder.to_python(data)

async def handle_claim_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("Claim Production event")
    block_time = block.timestamp
    claims = [
        {
            "event": decode_claim_event(ev.data),
            "transaction_hash": ev.transaction_hash,
        }
    ]
    print("    Claim decoded.")
    claim_docs = [
        {
            "owner": encode_int_as_bytes(tr["event"].owner),
            "land_id": encode_int_as_bytes(tr["event"].land_id),
            "time": encode_int_as_bytes(tr["event"].time),
            "block_number": encode_int_as_bytes(tr["event"].block_number),
            "building_counter": encode_int_as_bytes(tr["event"].building_counter),
            "transaction_hash": tr["transaction_hash"],
            "timestamp": block_time,
        }
        for tr in claims
    ]
    await info.storage.insert_many("claims", claim_docs)
    print("    Claim production stored.")

    # update buildings cycles
    for tr in claims:
        land_id = encode_int_as_bytes(tr["event"].land_id)
        buildings = await info.storage.find("buildings", {"land_id": land_id})
        results_list = list(buildings)

        if len(results_list) > 0:
            for building in results_list:
                active_cycles = building["active_cycles"]
                incoming_cycles = building["incoming_cycles"]
                last_fuel = building["last_fuel"]

                if incoming_cycles == 0:
                    active_cycles = 0
                else:
                    passed_blocks = tr["event"].block_number - last_fuel
                    if incoming_cycles <= passed_blocks:
                        active_cycles = 0
                        incoming_cycles = 0
                    else:
                        active_cycles = 0
                        incoming_cycles = incoming_cycles - passed_blocks
                
                await info.storage.find_one_and_update(
                    "buildings",
                    {
                        "building_uid": building["building_uid"],
                        "land_id": building["land_id"],
                    },
                    {"$set": 
                        {
                            "active_cycles": active_cycles,
                            "incoming_cycles": incoming_cycles,
                            "last_fuel": tr["event"].block_number,
                        }
                    }
                )