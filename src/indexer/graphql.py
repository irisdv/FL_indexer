import asyncio
from datetime import datetime
from typing import List, NewType, Optional
from decimal import Decimal
from indexer.utils import encode_int_as_bytes

import strawberry
from aiohttp import web
import aiohttp_cors
from pymongo import MongoClient
from strawberry.aiohttp.views import GraphQLView
from indexer.indexer import indexer_id

def parse_hex(value):
    print(value)
    if not value.startswith("0x"):
        raise valueError("invalid Hex value")
    if len(value) % 2 == 1:
        value = "0" + value
    return bytes.fromhex(value.replace("0x", ""))


def serialize_hex(token_id):
    print(token_id)
    return "0x" + token_id.hex()

HexValue = strawberry.scalar(
    NewType("HexValue", bytes),
    parse_value=parse_hex,
    serialize=serialize_hex
)

@strawberry.type
class Transfer:
    from_address: HexValue
    to_address: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            from_address=data["from_address"],
            to_address=data["to_address"],
            timestamp=data["timestamp"],
        )

@strawberry.type
class Token:
    token_id: HexValue
    owner: HexValue
    updated_at: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            token_id=data["token_id"],
            owner=data["owner"],
            updated_at=data["updated_at"],
        )
    
    @strawberry.field
    def transfers(self, info, limit: int = 10, skip: int = 0) -> List[Transfer]:
        db = info.context["db"]
        query = (
            db["transfers"]
            .find({"token_id": self.token_id})
            .limit(limit)
            .skip(skip)
            .sort("timestamp", -1)
        )
        
        return [Transfer.from_mongo(t) for t in query]

# returns Token with the given id, if it exists
def get_token_by_id(info, id: HexValue) -> Optional[Token]:
    db = info.context["db"]
    token = db["tokens"].find_one({"token_id": id, "_chain.valid_to": None})

    if token is not None:
        return Token.from_mongo(token)
    return None

# Returns a list of tokns, optionally filtered by their owners
# skip and limit used for pagination
def get_tokens(
    info, owner: Optional[HexValue] = None, limit: int = 10, skip: int = 0
) -> List[Token]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}
    if owner is not None:
        filter["owner"] = owner

    query = db["tokens"].find(filter).skip(skip).limit(limit).sort("updated_at", -1)

    return [Token.from_mongo(t) for t in query]

# ------- New Game Event ------
@strawberry.type
class GameInit:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            timestamp=data["timestamp"],
        )

# returns event for a given tokenId
def get_init_by_id(info, land_id: HexValue) -> Optional[GameInit]:
    db = info.context["db"]
    query = db["inits"].find_one({"land_id": land_id, "_chain.valid_to": None})

    if query is not None:
        return GameInit.from_mongo(query)

# ------- Harvest Event ------

@strawberry.type
class HarvestResource:
    owner: HexValue
    land_id: HexValue
    resource_type: HexValue
    resource_uid: HexValue
    block_comp: HexValue
    pos_x: HexValue
    pos_y: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            resource_type=data["resource_type"],
            resource_uid=data["resource_uid"],
            block_comp=data["block_comp"],
            pos_x=data["pos_x"],
            pos_y=data["pos_y"],
            timestamp=data["timestamp"],
        )

# returns event for a given tokenId
def get_harvest_by_id(info, land_id: Optional[HexValue] = None, limit: int = 10, skip: int = 0
) -> List[HarvestResource]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}
    if land_id is not None:
        filter["land_id"] = land_id

    query = db["harvest"].find(filter).skip(skip).sort("updated_at", -1)
    return [HarvestResource.from_mongo(t) for t in query]

# Returns a list of tokns, optionally filtered by their owners
# skip and limit used for pagination
def get_harvest_array(
    info, owner: Optional[HexValue] = None, land_id: Optional[HexValue] = None, limit: int = 10, skip: int = 0
) -> List[HarvestResource]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}
    if owner is not None:
        filter["owner"] = owner
    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["harvest"].find(filter).skip(skip).limit(limit).sort("updated_at", -1)

    return [HarvestResource.from_mongo(t) for t in query]

# returns harvest events for a given land_id with timestamp of tx greater than time
def get_harvest_by_id_time(info, id: HexValue, time: datetime) -> Optional[HarvestResource]:
    db = info.context["db"]
    harvest = db["harvest"].find_one({"land_id": id, "_chain.valid_to": None, "timestamp": {"$gt": time}})

    if harvest is not None:
        return HarvestResource.from_mongo(harvest)
    return None

# ------- End Harvest Event ------

# ------- Build Event ------

@strawberry.type
class Build:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    building_type_id: HexValue
    building_uid: HexValue
    block_comp: HexValue
    pos_x: HexValue
    pos_y: HexValue
    timestamp: datetime
    status: str
    decay: int
    active_cycles: int
    incoming_cycles: int
    last_fuel: int
    updated_at: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            building_type_id=data["building_type_id"],
            building_uid=data["building_uid"],
            block_comp=data["block_comp"],
            pos_x=data["pos_x"],
            pos_y=data["pos_y"],
            timestamp=data["timestamp"],
            status=data["status"],
            decay=data["decay"],
            active_cycles=data["active_cycles"],
            incoming_cycles=data["incoming_cycles"],
            last_fuel=data["last_fuel"],
            updated_at=data["updated_at"],
        )

# returns builds event for a given tokenId
def get_all_buildings(info, land_id: HexValue, skip: int = 0, limit: int = 10) -> List[Build]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["buildings"].find(filter).skip(skip).limit(limit).sort("updated_at", -1)

    return [Build.from_mongo(t) for t in query]

# returns all building that haven't been destroyed
def get_buildings_state(info, land_id: Optional[HexValue], skip: int = 0, limit: int = 10) -> List[Build]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None, "status": {"$ne": "destroyed"}}  
    if land_id is not None:
        filter["land_id"] = land_id

    query = db["buildings"].find(filter).skip(skip).limit(limit).sort("updated_at", -1)

    return [Build.from_mongo(t) for t in query]

# returns build events for a given land_id with block equals to block
def get_build_by_id_block(info, land_id: HexValue, block: HexValue) -> List[Build]:
    db = info.context["db"]
    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    if block is not None:
        filter['time'] = block

    query = db["buildings"].find(filter).sort("updated_at", -1)

    return [Build.from_mongo(t) for t in query]

# ------- FuelProduction Event ------
@strawberry.type
class FuelProduction:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    building_type_id: HexValue
    building_uid: HexValue
    pos_x: HexValue
    pos_y: HexValue
    nb_blocks: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            building_type_id=data["building_type_id"],
            building_uid=data["building_uid"],
            pos_x=data["pos_x"],
            pos_y=data["pos_y"],
            nb_blocks=data["nb_blocks"],
            timestamp=data["timestamp"],
        )

# returns fuelProduction event for a given tokenId
def get_fuel_by_id(info, land_id: HexValue, skip: int = 0) -> List[FuelProduction]:
    db = info.context["db"]
    
    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["fuel"].find(filter).skip(skip).sort("updated_at", -1)

    return [FuelProduction.from_mongo(t) for t in query]

# returns fuelProduction events for a given land_id with timestamp of tx greater than time
def get_fuel_by_id_time(info, id: HexValue, time: datetime) -> Optional[FuelProduction]:
    db = info.context["db"]
    fuel = db["fuel"].find_one({"land_id": id, "_chain.valid_to": None, "timestamp": {"$gt": time}})

    if fuel is not None:
        return FuelProduction.from_mongo(fuel)
    return None

# returns fuelProduction events for a given land_id with block equals to block
def get_fuel_by_id_block(info, id: HexValue, block: HexValue) -> Optional[FuelProduction]:
    db = info.context["db"]
    fuel = db["fuel"].find_one({"land_id": id, "_chain.valid_from": block})

    if fuel is not None:
        return FuelProduction.from_mongo(fuel)
    return None


# ------- Claim Event ------
@strawberry.type
class ClaimResources:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    block_number: HexValue
    building_counter: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            block_number=data["block_number"],
            building_counter=data["building_counter"],
            timestamp=data["timestamp"],
        )

# returns claim event for a given tokenId
def get_claim_by_id(info, land_id: HexValue, skip: int = 0) -> List[ClaimResources]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["claims"].find(filter).skip(skip).sort("updated_at", -1)

    return [ClaimResources.from_mongo(t) for t in query]

# returns claim events for a given land_id with timestamp of tx greater than time
def get_claim_by_id_time(info, id: HexValue, time: datetime) -> Optional[ClaimResources]:
    db = info.context["db"]
    claims = db["claims"].find_one({"land_id": id, "_chain.valid_to": None, "timestamp": {"$gt": time}})

    if claims is not None:
        return ClaimResources.from_mongo(claims)
    return None

# returns claim events for a given land_id with block equals to block
def get_claim_by_id_block(info, id: HexValue, block: HexValue) -> Optional[ClaimResources]:
    db = info.context["db"]
    claims = db["claims"].find_one({"land_id": id, "_chain.valid_from": block})

    if claims is not None:
        return ClaimResources.from_mongo(claims)
    return None


# ------- Repair Event ------
@strawberry.type
class RepairBuilding:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    building_type_id: HexValue
    building_uid: HexValue
    pos_x: HexValue
    pos_y: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            building_type_id=data["building_type_id"],
            building_uid=data["building_uid"],
            pos_x=data["pos_x"],
            pos_y=data["pos_y"],
            timestamp=data["timestamp"],
        )

# returns claim event for a given tokenId
def get_repairs_by_id(info, land_id: Optional[HexValue]) -> List[RepairBuilding]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id

    query = db["repairs"].find(filter).sort("updated_at", -1)

    return [RepairBuilding.from_mongo(t) for t in query]

# ------- Moves Event ------
@strawberry.type
class MoveInfrastructure:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    infra_type: HexValue
    infra_type_id: HexValue
    pos_x: HexValue
    pos_y: HexValue
    new_pos_x: HexValue
    new_pos_y: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            infra_type=data["infra_type"],
            infra_type_id=data["infra_type_id"],
            pos_x=data["pos_x"],
            pos_y=data["pos_y"],
            new_pos_x=data["new_pos_x"],
            new_pos_y=data["new_pos_y"],
            timestamp=data["timestamp"],
        )

# returns move event for a given tokenId
def get_moves_by_id(info, land_id: HexValue, skip: int = 0) -> List[MoveInfrastructure]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["moves"].find(filter).skip(skip).sort("updated_at", -1)

    return [MoveInfrastructure.from_mongo(t) for t in query]


# ------- Reset Game Event ------
@strawberry.type
class ResetGame:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            timestamp=data["timestamp"],
        )

# returns move event for a given tokenId
def get_resets_by_id(info, land_id: HexValue, limit: int = 10, skip: int = 0) -> List[ResetGame]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["reset"].find(filter).skip(skip).limit(limit).sort("updated_at", -1)

    return [ResetGame.from_mongo(t) for t in query]

# ------- Reset Destroy Event ------
@strawberry.type
class DestroyInfrastructure:
    owner: HexValue
    land_id: HexValue
    time: HexValue
    building_type_id: HexValue
    block_comp: HexValue
    pos_x: HexValue
    pos_y: HexValue
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            owner=data["owner"],
            land_id=data["land_id"],
            time=data["time"],
            building_type_id=data["building_type_id"],
            block_comp=data["block_comp"],
            pos_x=data["pos_x"],
            pos_y=data["pos_y"],
            timestamp=data["timestamp"],
        )

# returns move event for a given tokenId
def get_destroy_by_id(info, land_id: HexValue, limit: int = 10, skip: int = 0) -> List[DestroyInfrastructure]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["destroy"].find(filter).skip(skip).limit(limit).sort("updated_at", -1)

    return [DestroyInfrastructure.from_mongo(t) for t in query]

@strawberry.type
class Land:
    map: List[List[Decimal]]
    land_id: HexValue
    time: HexValue
    timestamp: datetime
    updated_at: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            map=data["map"],
            land_id=data["land_id"],
            time=data["time"],
            timestamp=data["timestamp"],
            updated_at=data["updated_at"],
        )

def get_map(info, land_id: Optional[HexValue], skip: int = 0) -> List[Land]:
    db = info.context["db"]

    filter = {"_chain.valid_to": None}    
    if land_id is not None:
        filter["land_id"] = land_id
    
    query = db["lands"].find(filter).skip(skip).sort("updated_at", -1)

    return [Land.from_mongo(t) for t in query]

# ------- End Destroy Event ------

@strawberry.type
class Query:
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
    token: Optional[Token] = strawberry.field(resolver=get_token_by_id)
    # * 
    wasInit: Optional[GameInit] = strawberry.field(resolver=get_init_by_id)
    getLand: List[Land] = strawberry.field(resolver=get_map)
    getAllBuildings: List[Build] = strawberry.field(resolver=get_all_buildings)
    getBuildingsState: List[Build] = strawberry.field(resolver=get_buildings_state)
    # Harvest
    harvest: List[HarvestResource] = strawberry.field(resolver=get_harvest_by_id)
    harvestTime: Optional[HarvestResource] = strawberry.field(resolver=get_harvest_by_id_time)
    harvestAll: List[HarvestResource] = strawberry.field(resolver=get_harvest_array)
    # Build
    buildByBlock: List[Build] = strawberry.field(resolver=get_build_by_id_block)
    # FuelProduction
    fuel: List[FuelProduction] = strawberry.field(resolver=get_fuel_by_id)
    fueldTime: Optional[FuelProduction] = strawberry.field(resolver=get_fuel_by_id_time)
    fuelBlock: Optional[FuelProduction] = strawberry.field(resolver=get_fuel_by_id_block)
    # Claim
    claim: List[ClaimResources] = strawberry.field(resolver=get_claim_by_id)
    claimTime: Optional[ClaimResources] = strawberry.field(resolver=get_claim_by_id_time)
    claimBlock: Optional[ClaimResources] = strawberry.field(resolver=get_claim_by_id_block)
    repair: List[RepairBuilding] = strawberry.field(resolver=get_repairs_by_id)
    move: List[MoveInfrastructure] = strawberry.field(resolver=get_moves_by_id)
    reset: List[ResetGame] = strawberry.field(resolver=get_resets_by_id)
    destroy: List[DestroyInfrastructure] = strawberry.field(resolver=get_destroy_by_id)

class IndexerGraphQLView(GraphQLView):
    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        self._db = db

    async def get_context(self, _request, _response):
        return {"db": self._db}


async def run_graphql_api(mongo_url=None):
    if mongo_url is None:
        mongo_url = "mongodb://apibara:apibara@localhost:27017"

    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]

    schema = strawberry.Schema(query=Query)
    view = IndexerGraphQLView(db, schema=schema)

    app = web.Application()

    app.router.add_post("/graphql", view)
    cors = aiohttp_cors.setup(app, defaults={
    "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*"
        )
    })
    for route in list(app.router.routes()):
        cors.add(route)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port="8080")
    await site.start()

    print(f"GraphQL server started on port 8080")

    while True:
        await asyncio.sleep(5_000)