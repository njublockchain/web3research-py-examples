# %%
import os
import json
from kylink.eth.base import EthereumProvider
from web3 import Web3
import pickle
from kylink.evm import ContractDecoder
from binascii import hexlify
import dotenv
import eth_utils

dotenv.load_dotenv()

w3 = Web3()

# for internet
ky = kylink.Kylink(api_token=os.environ["KYLINK_API_TOKEN"])
ky_eth = ky.eth

# for localhost
# ky_eth = EthereumProvider(api_token=os.environ["KYLINK_API_TOKEN"], host="localhost", port=18123, interface="http")


def ensure_all_pairs_fetched_from_factory():
    pairs = []
    if not os.path.exists("output/uniswap_v2_pairs.json"):
        factory_address = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".removeprefix("0x")
        PairCreated_hash = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9".removeprefix(
            "0x"
        )

        factory_abi = json.load(open("uniswap/abi/uniswap_v2_factory_abi.json"))
        factory_decoder = ContractDecoder(w3, factory_abi)
        limit = 10_000
        offset = 0

        while True:
            events = ky_eth.events(
                f"address = unhex('{factory_address}') and topic0 = unhex('{PairCreated_hash}') ",
                limit=limit,
                offset=offset,
            )
            for event in events:
                log = factory_decoder.decode_event_log("PairCreated", log=event)
                log["id"] = log.pop("")
                log["timestamp"] = event["blockTimestamp"]
                log["blockNumber"] = event["blockNumber"]
                log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
                pairs.append(log)
            if len(events) < limit:
                break
            else:
                offset += limit

        json.dump(pairs, open("output/uniswap_v2_pairs.json", "w"), indent=2)
    else:
        pairs = json.load(open("output/uniswap_v2_pairs.json"))
        pairs = sorted(pairs, key=lambda x: x["id"])

    return pairs

def extract_pair_events(pair_address: str):
    # extract Mint, Burn, Swap, Sync events from pair_address
    data = {
        "Mint": [],
        "Swap": [],
        "Burn": [],
        "Sync": [],
    }

    pair_abi = json.load(open("uniswap/abi/uniswap_v2_pair_abi.json"))
    pair_decoder = ContractDecoder(w3, pair_abi)

    Mint_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.event_abi_dict["Mint"])
    ).decode()
    Swap_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.event_abi_dict["Swap"])
    ).decode()
    Burn_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.event_abi_dict["Burn"])
    ).decode()
    Sync_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.event_abi_dict["Sync"])
    ).decode()

    limit = 100_000
    offset = 0
    while True:
        where = f"address = unhex('{pair_address.removeprefix('0x') }') and topic0 = unhex('{Mint_topic}') "
        # print(where)
        events = ky_eth.events(where=where, limit=limit, offset=offset)
        docs = []
        for event in events:
            event["topics"] = []
            if event["topic0"]:
                event["topics"].append(event["topic0"])
            if event["topic1"]:
                event["topics"].append(event["topic1"])
            if event["topic2"]:
                event["topics"].append(event["topic2"])
            if event["topic3"]:
                event["topics"].append(event["topic3"])
            log = pair_decoder.decode_event_log("Mint", log=event)
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
            f"address = unhex('{pair_address.removeprefix('0x') }') and topic0 = unhex('{Swap_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            event["topics"] = []
            # if event["topic0"]:
            if event["topic0"]:
                event["topics"].append(event["topic0"])
            # if event["topic1"]:
            if event["topic1"]:
                event["topics"].append(event["topic1"])
            # if event["topic2"]:
            if event["topic2"]:
                event["topics"].append(event["topic2"])            
            if event["topic3"]:
                event["topics"].append(event["topic3"])
            
            try:
                log = pair_decoder.decode_event_log("Swap", log=event)
            except Exception as e:
                print(event)
                print(hexlify(event["transactionHash"]))
                raise e
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

    offset = 0
    while True:
        events = ky_eth.events(
            f"address = unhex('{pair_address.removeprefix('0x') }') and topic0 = unhex('{Burn_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            event["topics"] = []
            if event["topic0"]:
                event["topics"].append(event["topic0"])
            if event["topic1"]:
                event["topics"].append(event["topic1"])
            if event["topic2"]:
                event["topics"].append(event["topic2"])
            if event["topic3"]:
                event["topics"].append(event["topic3"])
            log = pair_decoder.decode_event_log("Burn", log=event)
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
            f"address = unhex('{pair_address.removeprefix('0x') }') and topic0 = unhex('{Sync_topic}') ",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pair_decoder.decode_event_log("Sync", log=event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = "0x" + hexlify(event["transactionHash"]).decode()
            docs.append(log)

        data["Sync"].extend(docs)
        if len(events) < limit:
            print("Sync", len(docs))
            break
        else:
            offset += limit

    return data

def dump_pair(pair):
    pair_address = pair["pair"]  # pair contract address, get events from them

    if os.path.exists(f"examples/uniswap_v2_pairs/{pair_address}.pkl"):
        print(f"{pair["id"]} exists")
        return

    data = extract_pair_events(pair_address)

    with open(f"examples/uniswap_v2_pairs/{pair_address}.pkl", "wb") as f:
        pickle.dump(data, f)

    print(f"{pair["id"]}", )

if __name__ == "__main__":
    from tqdm import tqdm

    os.makedirs("output/uniswap_v2_pairs", exist_ok=True)
    pairs = ensure_all_pairs_fetched_from_factory()

    list(tqdm(map(dump_pair, pairs), total=len(pairs)))

