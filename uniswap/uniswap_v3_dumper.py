from binascii import hexlify
import os
import json
import dotenv
import eth_utils
from web3research import Web3Research
from web3research.common import Address, Hash
from web3research.evm import ContractDecoder

dotenv.load_dotenv()

dotenv.load_dotenv()

# for localhost
from web3 import Web3

w3 = Web3()
w3r = Web3Research(api_token=os.environ["W3R_API_KEY"])
if os.environ["W3R_BACKEND"]:
    eth = w3r.eth(backend=os.environ["W3R_BACKEND"])
else:
    eth = w3r.eth()

TOP_BLOCK = 21000000

uniswap_v3_facyory_address = Address("0x1F98431c8aD98523631AE4a59f267346ea31F984")


def ensure_all_pools_fetched_from_factory():
    pools = []

    if not os.path.exists("output/uniswap_v3_pools.json"):
        PoolCreated_topic = Hash(
            "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
        )

        factory_abi = json.load(open("uniswap/abi/uniswap_v3_factory_abi.json"))
        factory_decoder = ContractDecoder(w3, factory_abi)
        limit = 100
        offset = 0

        while True:
            events = eth.events(
                f"address = {uniswap_v3_facyory_address} and topic0 = {PoolCreated_topic} and blockNumber <= {TOP_BLOCK} ",
                limit=limit,
                offset=offset,
            )
            count = 0
            for event in events:
                log = factory_decoder.decode_event_log("PoolCreated", event)
                log["timestamp"] = event["blockTimestamp"]
                log["blockNumber"] = event["blockNumber"]
                log["transactionHash"] = event["transactionHash"]
                pools.append(log)
                count += 1
            if count < limit:
                break
            else:
                offset += limit

        json.dump(pools, open("output/uniswap_v3_pools.json", "w"), indent=2)

    else:
        pools = json.load(open("output/uniswap_v3_pools.json"))

    return sorted(pools, key=lambda x: x["blockNumber"])


def extract_pool_events(pool_address: str):
    pool_address = Address(pool_address)

    data = {
        "Burn": [],
        "Collect": [],
        "CollectProtocol": [],
        "Flash": [],
        "IncreaseObservationCardinalityNext": [],
        "Initialize": [],
        "Mint": [],
        "SetFeeProtocol": [],
        "Swap": [],
    }

    pool_abi = json.load(open("uniswap/abi/uniswap_v3_pool_abi.json"))
    pool_decoder = ContractDecoder(w3, pool_abi)

    Burn_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Burn"))
        ).decode()
    )
    Collect_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Collect"))
        ).decode()
    )
    CollectProtocol_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(
                pool_decoder.get_event_abi("CollectProtocol")
            )
        ).decode()
    )
    Flash_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Flash"))
        ).decode()
    )
    IncreaseObservationCardinalityNext_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(
                pool_decoder.get_event_abi("IncreaseObservationCardinalityNext")
            )
        ).decode()
    )
    Initialize_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Initialize"))
        ).decode()
    )
    Mint_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Mint"))
        ).decode()
    )
    SetFeeProtocol_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(
                pool_decoder.get_event_abi("SetFeeProtocol")
            )
        ).decode()
    )
    Swap_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Swap"))
        ).decode()
    )

    limit = 100_000
    offset = 0
    while True:
        where = f"address = {pool_address} and topic0 = {Burn_topic} and blockNumber <= {TOP_BLOCK} "
        # print(where)
        events = eth.events(where=where, limit=limit, offset=offset)
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Burn", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["Burn"].extend(docs)
        if len(docs) < limit:
            print("Burn", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {Collect_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Collect", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["Collect"].extend(docs)
        if len(docs) < limit:
            print("Collect", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {CollectProtocol_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("CollectProtocol", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["CollectProtocol"].extend(docs)
        if len(docs) < limit:
            print("CollectProtocol", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {Flash_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Flash", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["Flash"].extend(docs)
        if len(docs) < limit:
            print("Flash", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {IncreaseObservationCardinalityNext_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log(
                "IncreaseObservationCardinalityNext", event
            )
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["IncreaseObservationCardinalityNext"].extend(docs)
        if len(docs) < limit:
            print("IncreaseObservationCardinalityNext", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {Initialize_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Initialize", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["Initialize"].extend(docs)
        if len(docs) < limit:
            print("Initialize", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {Mint_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Mint", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["Mint"].extend(docs)
        if len(docs) < limit:
            print("Mint", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {SetFeeProtocol_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("SetFeeProtocol", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["SetFeeProtocol"].extend(docs)
        if len(docs) < limit:
            print("SetFeeProtocol", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = eth.events(
            f"address = {pool_address} and topic0 = {Swap_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Swap", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["Swap"].extend(docs)
        if len(docs) < limit:
            print("Swap", len(docs))
            break
        else:
            offset += limit

    return data


def dump_pool(pool):
    pool_address = pool["pool"]  # pool contract address, get events from them
    if os.path.exists(f"output/uniswap_v3_pools/{pool_address}.json"):
        print(pool["pool"], "already done")
        return
    data = extract_pool_events(pool_address)
    with open(f"output/uniswap_v3_pools/{pool_address}.json", "w") as f:
        json.dump(data, f)
    print(pool["pool"], "done")


if __name__ == "__main__":
    from tqdm import tqdm

    os.makedirs("output/uniswap_v3_pools", exist_ok=True)
    pools = ensure_all_pools_fetched_from_factory()

    list(tqdm(map(dump_pool, pools), total=len(pools)))
