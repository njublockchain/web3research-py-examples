import os
import json
from kylink.eth.base import EthereumProvider
from web3 import Web3
import pickle
from kylink.evm import ContractDecoder
from binascii import hexlify
import dotenv
import eth_utils
import kylink

dotenv.load_dotenv()

w3 = Web3()

# for internet
ky = kylink.Kylink(api_token=os.environ["KYLINK_API_TOKEN"])
ky_eth = ky.eth

# for localhost
# ky_eth = EthereumProvider(api_token=os.environ["KYLINK_API_TOKEN"], host="localhost", port=18123, interface="http")

sushiswap_v3_facyory_address = '0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F'.removeprefix("0x")


def ensure_all_pools_fetched_from_factory():
    pools = []
    
    if not os.path.exists("output/sushiswap_v3_pools.json"):
        factory_abi = json.load(open("sushiswap/abi/sushiswap_v3_factory_abi.json"))
        factory_decoder = ContractDecoder(w3, factory_abi)
        PoolCreated_hash = hexlify(
            eth_utils.event_abi_to_log_topic(factory_decoder.get_event_abi("PoolCreated"))
        ).decode()

        limit = 100
        offset = 0

        while True:
            events = ky_eth.events(f"address = unhex('{sushiswap_v3_facyory_address}') and topic0 = unhex('{PoolCreated_hash}') ", limit=limit, offset=offset)
            for event in events:
                log = factory_decoder.decode_event_log("PoolCreated", log=event)
                log["timestamp"] = event["blockTimestamp"]
                log["blockNumber"] = event["blockNumber"]
                log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
                pools.append(log)
            if len(events) < limit:
                break
            else:
                offset += limit

        json.dump(pools, open("output/sushiswap_v3_pools.json", "w"), indent=2)

    else:
        pools = json.load(open("output/sushiswap_v3_pools.json"))

    return sorted(pools, key=lambda x: x["blockNumber"])


def extract_pool_events(pool_address: str):
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

    pool_abi = json.load(open("sushiswap/abi/sushiswap_v3_pool_abi.json"))
    pool_decoder = ContractDecoder(w3, pool_abi)

    Burn_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Burn"))
    ).decode()
    Collect_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Collect"))
    ).decode()
    CollectProtocol_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("CollectProtocol"))
    ).decode()
    Flash_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Flash"))
    ).decode()
    IncreaseObservationCardinalityNext_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("IncreaseObservationCardinalityNext"))
    ).decode()
    Initialize_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Initialize"))
    ).decode()
    Mint_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Mint"))
    ).decode()
    SetFeeProtocol_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("SetFeeProtocol"))
    ).decode()
    Swap_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pool_decoder.get_event_abi("Swap"))
    ).decode()


    limit = 100_000
    offset = 0
    while True:
        where = f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{Burn_topic}') "
        # print(where)
        events = ky_eth.events(where=where, limit=limit, offset=offset)
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Burn", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["Burn"].extend(docs)
        if len(events) < limit:
            print("Burn", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{Collect_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Collect", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["Collect"].extend(docs)
        if len(events) < limit:
            print("Collect", len(docs))
            break
        else:
            offset += limit
    
    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{CollectProtocol_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("CollectProtocol", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["CollectProtocol"].extend(docs)
        if len(events) < limit:
            print("CollectProtocol", len(docs))
            break
        else:
            offset += limit
    
    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{Flash_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Flash", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["Flash"].extend(docs)
        if len(events) < limit:
            print("Flash", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{IncreaseObservationCardinalityNext_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("IncreaseObservationCardinalityNext", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["IncreaseObservationCardinalityNext"].extend(docs)
        if len(events) < limit:
            print("IncreaseObservationCardinalityNext", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{Initialize_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Initialize", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["Initialize"].extend(docs)
        if len(events) < limit:
            print("Initialize", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{Mint_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Mint", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["Mint"].extend(docs)
        if len(events) < limit:
            print("Mint", len(docs))
            break
        else:
            offset += limit

    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{SetFeeProtocol_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("SetFeeProtocol", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["SetFeeProtocol"].extend(docs)
        if len(events) < limit:
            print("SetFeeProtocol", len(docs))
            break
        else:
            offset += limit
    
    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pool_address.removeprefix('0x') }') and topic0 = unhex('{Swap_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pool_decoder.decode_event_log("Swap", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["Swap"].extend(docs)
        if len(events) < limit:
            print("Swap", len(docs))
            break
        else:
            offset += limit

    return data


def dump_pool(pool):
    pool_address = pool["pool"]  # pool contract address, get events from them
    if os.path.exists(f"output/sushiswap_v3_pools/{pool_address}.pkl"):
        print(f"{pool["pool"]} already done", )
        return
    data = extract_pool_events(pool_address)
    with open(f"output/sushiswap_v3_pools/{pool_address}.pkl", "wb") as f:
        pickle.dump(data, f)
    print(f"{pool["pool"]} done", )

if __name__ == "__main__":
    from tqdm import tqdm

    os.makedirs("output/sushiswap_v3_pools", exist_ok=True)
    pools = ensure_all_pools_fetched_from_factory()

    list(tqdm(map(dump_pool, pools), total=len(pools)))
