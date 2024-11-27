# %%
from binascii import hexlify
import os
import json
import dotenv
import eth_utils
from web3research import Web3Research
from web3research.common import Address, Hash
from web3research.evm import ContractDecoder

dotenv.load_dotenv()

from web3 import Web3

w3 = Web3()
w3r = Web3Research(api_token=os.environ["W3R_API"])
if os.environ["W3R_BACKEND"]:
    eth = w3r.eth(backend=os.environ["W3R_BACKEND"])
else:
    eth = w3r.eth()

TOP_BLOCK = 21000000


def ensure_all_pairs_fetched_from_factory():
    pairs = []
    if not os.path.exists("output/uniswap_v2_pairs.json"):
        factory_address = Address("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f")
        PairCreated_hash = Hash(
            "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
        )

        factory_abi = json.load(open("uniswap/abi/uniswap_v2_factory_abi.json"))
        factory_decoder = ContractDecoder(w3, factory_abi)
        limit = 10_000
        offset = 0

        while True:
            events = eth.events(
                f"address = {factory_address} and topic0 = {PairCreated_hash} ",
                limit=limit,
                offset=offset,
            )
            count = 0
            for event in events:
                log = factory_decoder.decode_event_log("PairCreated", event)
                log["id"] = log.pop("")
                log["timestamp"] = event["blockTimestamp"]
                log["blockNumber"] = event["blockNumber"]
                log["transactionHash"] = event["transactionHash"]
                pairs.append(log)
                count += 1

            if count < limit:
                break
            else:
                offset += limit

        json.dump(pairs, open("output/uniswap_v2_pairs.json", "w"), indent=2)
    else:
        pairs = json.load(open("output/uniswap_v2_pairs.json"))
        pairs = sorted(pairs, key=lambda x: x["id"])

    return pairs


def extract_pair_events(pair_address: str):
    pair_address = Address(pair_address)

    # extract Mint, Burn, Swap, Sync events from pair_address
    data = {
        "Mint": [],
        "Swap": [],
        "Burn": [],
        "Sync": [],
    }

    pair_abi = json.load(open("uniswap/abi/uniswap_v2_pair_abi.json"))
    pair_decoder = ContractDecoder(w3, pair_abi)

    Mint_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Mint"))
        ).decode()
    )
    Swap_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Swap"))
        ).decode()
    )
    Burn_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Burn"))
        ).decode()
    )
    Sync_topic = Hash(
        hexlify(
            eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Sync"))
        ).decode()
    )

    limit = 100_000
    offset = 0
    while True:
        where = f"address = {pair_address} and topic0 = {Mint_topic} and blockNumber <= {TOP_BLOCK} "
        # print(where)
        events = eth.events(where=where, limit=limit, offset=offset)
        docs = []
        for event in events:
            log = pair_decoder.decode_event_log("Mint", event)
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
            f"address = {pair_address} and topic0 = {Swap_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            try:
                log = pair_decoder.decode_event_log("Swap", event)
            except Exception as e:
                print(event)
                print(hexlify(event["transactionHash"]))
                raise e
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

    offset = 0
    while True:
        events = eth.events(
            f"address = {pair_address} and topic0 = {Burn_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pair_decoder.decode_event_log("Burn", event)
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
            f"address = {pair_address} and topic0 = {Sync_topic} and blockNumber <= {TOP_BLOCK}",
            limit=limit,
            offset=offset,
        )
        docs = []
        for event in events:
            log = pair_decoder.decode_event_log("Sync", event)
            log["timestamp"] = event["blockTimestamp"]
            log["blockNumber"] = event["blockNumber"]
            log["transactionIndex"] = event["transactionIndex"]
            log["logIndex"] = event["logIndex"]
            log["transactionHash"] = event["transactionHash"]
            docs.append(log)

        data["Sync"].extend(docs)
        if len(docs) < limit:
            print("Sync", len(docs))
            break
        else:
            offset += limit

    return data


def dump_pair(pair):
    pair_address = pair["pair"]  # pair contract address, get events from them

    if os.path.exists(f"output/uniswap_v2_pairs/{pair_address}.json"):
        print(pair["id"], "exists")
        return

    data = extract_pair_events(pair_address)

    with open(f"output/uniswap_v2_pairs/{pair_address}.json", "w") as f:
        json.dump(data, f)

    print(pair["id"], "done")


if __name__ == "__main__":
    from tqdm import tqdm

    os.makedirs("output/uniswap_v2_pairs", exist_ok=True)
    pairs = ensure_all_pairs_fetched_from_factory()

    list(tqdm(map(dump_pair, pairs), total=len(pairs)))
