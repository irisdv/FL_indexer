from typing import List, NamedTuple
from apibara import Info
from apibara.model import BlockHeader, StarkNetEvent
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

    for de in fuels:
        building = await info.storage.find_one("buildings", {
            "building_uid": encode_int_as_bytes(de["event"].building_uid), 
            "land_id": encode_int_as_bytes(de["event"].land_id)
        })
        if building is not None:
            active_cycles = building["active_cycles"]
            incoming_cycles = building["incoming_cycles"]
            last_fuel = building["last_fuel"]

            print("building", building)
            print("active_cycles", active_cycles)
            print("incoming_cycles", incoming_cycles)
            print("nb_blocks", de["event"].nb_blocks)

            if incoming_cycles == 0:
                incoming_cycles = de["event"].nb_blocks
            else:
                # todo check que bon calcul
                passed_blocks = de["event"].time - last_fuel
                print("passed_blocks", passed_blocks)
                if incoming_cycles <= passed_blocks:
                    active_cycles += incoming_cycles
                    incoming_cycles = de["event"].nb_blocks
                else:
                    active_cycles += passed_blocks
                    incoming_cycles = incoming_cycles - passed_blocks + de["event"].nb_blocks

            print("active_cycles", active_cycles)
            print("incoming_cycles", incoming_cycles)
            print("nb_blocks", de["event"].nb_blocks)
            
            await info.storage.find_one_and_update(
                "buildings",
                {
                    "building_uid": encode_int_as_bytes(de["event"].building_uid),
                    "land_id": encode_int_as_bytes(de["event"].land_id),
                },
                {"$set": {
                    "active_cycles": active_cycles,
                    "incoming_cycles": incoming_cycles,
                    "last_fuel": de["event"].time,
                }},
            )
    print("    Buildings updated with fuels.")