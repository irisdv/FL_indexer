from typing import List, NamedTuple
from apibara import Info
from apibara.model import BlockHeader, StarkNetEvent
from starknet_py.contract import FunctionCallSerializer, identifier_manager_from_abi
from indexer.utils import encode_int_as_bytes, uint256_abi

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

def decode_transfer_event(data: List[bytes]) -> NamedTuple:
    data = [int.from_bytes(b, "big") for b in data]
    return transfer_decoder.to_python(data)

async def handle_transfer_events(info: Info, block: BlockHeader, ev: StarkNetEvent):
    print("Transfer event")
    block_time = block.timestamp
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