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

# for localhost
from web3 import Web3

w3 = Web3()
w3r = Web3Research(api_token=os.environ["W3R_API_KEY"])
eth = w3r.eth(backend=os.environ["W3R_BACKEND"])

TOP_BLOCK = 21000000


def ensure_all_pairs_fetched_from_factory():
    pairs = []
    if not os.path.exists("output/sushiswap_v2_pairs.json"):
        factory_address = Address("0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac")
        factory_abi = json.load(open("sushiswap/abi/sushiswap_v2_factory_abi.json"))
        factory_decoder = ContractDecoder(w3, factory_abi)

        PairCreated_hash = hexlify(
            eth_utils.event_abi_to_log_topic(
                factory_decoder.get_event_abi("PairCreated")
            )
        ).decode()

        limit = 10_000
        offset = 0

        while True:
            events = eth.events(
                f"address = {factory_address} and topic0 = {Hash(PairCreated_hash)} and blockNumber <= {TOP_BLOCK}",
                limit=limit,
                offset=offset,
            )
            count = 0
            for event in events:
                log = factory_decoder.decode_event_log("PairCreated", event)
                log["id"] = log.pop("")
                log["timestamp"] = event["blockTimestamp"]
                log["blockNumber"] = event["blockNumber"]
                pairs.append(log)
                count += 1

            if count < limit:
                break
            else:
                offset += limit

        # dedup
        pairs = list({p["pair"]: p for p in pairs}.values())
        
        json.dump(pairs, open("output/sushiswap_v2_pairs.json", "w"), indent=2)
    else:
        pairs = json.load(open("output/sushiswap_v2_pairs.json"))
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

    pair_abi = json.load(open("sushiswap/abi/sushiswap_v2_pair_abi.json"))
    pair_decoder = ContractDecoder(w3, pair_abi)

    Mint_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Mint"))
    ).decode()
    Swap_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Swap"))
    ).decode()
    Burn_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Burn"))
    ).decode()
    Sync_topic = hexlify(
        eth_utils.event_abi_to_log_topic(pair_decoder.get_event_abi("Sync"))
    ).decode()

    limit = 100_000
    offset = 0
    while True:
        where = f"address = {Address(pair_address)} and topic0 = {Hash(Mint_topic)} and blockNumber <= {TOP_BLOCK} "
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
            f"address = {Address(pair_address)} and topic0 = {Hash(Swap_topic)} and blockNumber <= {TOP_BLOCK}",
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
            f"address = {Address(pair_address)} and topic0 = {Hash(Burn_topic)} and blockNumber <= {TOP_BLOCK}",
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
            f"address = {Address(pair_address)} and topic0 = {Hash(Sync_topic)} and blockNumber <= {TOP_BLOCK}",
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

    # dedup
    for k, v in data.items():
        data[k] = list({x["transactionHash"] + str(x["logIndex"]): x for x in v}.values())

    return data


def dump_pair(pair):
    pair_address = pair["pair"]  # pair contract address, get events from them

    if os.path.exists(f"output/sushiswap_v2_pairs/{pair_address}.json"):
        print(pair["id"], "exists")
        return

    data = extract_pair_events(pair_address)

    with open(f"output/sushiswap_v2_pairs/{pair_address}.json", "w") as f:
        json.dump(data, f)

    print(pair["id"])


if __name__ == "__main__":
    from tqdm import tqdm

    os.makedirs("output/sushiswap_v2_pairs", exist_ok=True)
    pairs = ensure_all_pairs_fetched_from_factory()

    list(tqdm(map(dump_pair, pairs), total=len(pairs)))
