#!/usr/bin/env python3
"""
Simple script to merge Polymarket positions directly on-chain.
Requires gas (MATIC) for transaction fees.

Install dependencies:
pip install web3

Usage:
export PRIVATE_KEY=your_private_key_here
python merge_positions_simple.py
"""

import os
import json
import time
import requests
from decimal import Decimal
from web3 import Web3
from web3.exceptions import Web3RPCError, TimeExhausted
from eth_account import Account

# Load environment variables from .env file
def load_env():
    env_file = ".env"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

load_env()

# Configuration
PRIVATE_KEY = os.getenv("PRIVATE_KEY") or os.getenv("POLYMARKET_PRIVATE_KEY")
POLYGON_RPC = os.getenv("POLYGON_RPC_URL") or "https://polygon-rpc.com"

# Contract addresses (Polygon mainnet)
USDC_E = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
STANDARD_CTF = Web3.to_checksum_address("0x4D97DcD97Ec945F40CF65F87097aCe5EA0476045")
NEGRISK_CTF = Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a")

DT_API_POSITIONS = "https://data-api.polymarket.com/positions"

# CTF contract ABI (only mergePositions function)
CTF_ABI = [
    {
        "name": "mergePositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"}
        ],
        "outputs": []
    },
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"}
        ],
        "outputs": [
            {"name": "", "type": "uint256"}
        ]
    }
]

def safe_contract_call(call_fn, retries: int = 5, delay: int = 5):
    """Helper to retry contract calls when RPC rate limits are hit."""
    for attempt in range(1, retries + 1):
        try:
            return call_fn()
        except Web3RPCError as err:
            if attempt == retries:
                raise
            print(f"RPC rate limit hit (attempt {attempt}/{retries}). Retrying in {delay}s... [{err}]")
            time.sleep(delay)


def wait_for_receipt_with_retry(w3: Web3, tx_hash, retries: int = 6, delay: int = 10):
    """Retry wait_for_transaction_receipt to survive temporary rate limits."""
    for attempt in range(1, retries + 1):
        try:
            return w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        except (Web3RPCError, TimeExhausted) as err:
            if attempt == retries:
                print(f"RPC wait error: {err}")
                return None
            print(f"RPC wait error (attempt {attempt}/{retries}): {err}. Retrying in {delay}s...")
            time.sleep(delay)

def fetch_mergeable_pairs(address: str):
    """Fetch mergeable Yes/No sets from Polymarket data-api."""
    try:
        resp = requests.get(
            DT_API_POSITIONS,
            params={"user": address, "sizeThreshold": 0.0001},
            timeout=15,
        )
        resp.raise_for_status()
    except requests.RequestException as err:
        print(f"❌ Failed to fetch positions: {err}")
        return []

    try:
        data = resp.json()
    except ValueError:
        print("❌ Data API returned invalid JSON")
        return []

    if not isinstance(data, list):
        print("❌ Unexpected data format from positions API")
        return []

    asset_map = {item.get("asset"): item for item in data if item.get("asset")}
    visited = set()
    pairs = []

    for item in data:
        asset = item.get("asset")
        if not asset or asset in visited:
            continue

        opp_asset = item.get("oppositeAsset")
        if not opp_asset:
            continue

        opp_item = asset_map.get(opp_asset)
        if not opp_item:
            continue

        if (not item.get("mergeable")) or (not opp_item.get("mergeable")):
            continue

        size_a = Decimal(str(item.get("size") or 0))
        size_b = Decimal(str(opp_item.get("size") or 0))
        available_pairs = int((min(size_a, size_b)).to_integral_value(rounding="ROUND_FLOOR"))
        if available_pairs <= 0:
            continue

        # Identify which entry is the YES side
        outcome_a = (item.get("outcome") or "").lower()
        outcome_b = (opp_item.get("outcome") or "").lower()
        if outcome_a == "yes" or (outcome_a != "no" and outcome_b == "no"):
            yes_item, no_item = item, opp_item
        else:
            yes_item, no_item = opp_item, item

        pairs.append({
            "title": yes_item.get("title") or no_item.get("title") or "Unknown", 
            "condition_id": item.get("conditionId"),
            "yes_size": float(yes_item.get("size") or 0),
            "no_size": float(no_item.get("size") or 0),
            "available_pairs": available_pairs,
            "negative_risk": bool(item.get("negativeRisk") or opp_item.get("negativeRisk")),
            "yes_asset": yes_item.get("asset"),
            "no_asset": no_item.get("asset"),
        })

        visited.add(asset)
        visited.add(opp_asset)

    return pairs


def main():
    if not PRIVATE_KEY:
        print("ERROR: Set PRIVATE_KEY environment variable")
        print("export PRIVATE_KEY=your_private_key_here")
        return
    
    # Initialize Web3 and account
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
    account = Account.from_key(PRIVATE_KEY)
    address = account.address
    print(f"Wallet: {address}")
    
    # Check balances
    print("\nChecking balances...")
    
    # MATIC balance (for gas)
    matic_balance = w3.eth.get_balance(address)
    print(f"MATIC balance: {matic_balance / 1e18:.4f}")
    
    # USDC.e balance
    usdc_contract = w3.eth.contract(address=USDC_E, abi=[
        {"name": "balanceOf", "type": "function", "inputs": [{"name": "account", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]}
    ])
    usdc_balance = safe_contract_call(lambda: usdc_contract.functions.balanceOf(address).call())
    print(f"USDC.e balance: ${usdc_balance / 1_000_000:.2f}")
    
    if matic_balance < 0.01 * 1e18:  # Less than 0.01 MATIC
        print("\n⚠️  WARNING: Low MATIC balance. You may need gas for transactions.")
        print("Get MATIC from: https://faucet.polygon.technology/")
    
    mergeable_pairs = fetch_mergeable_pairs(address)
    if not mergeable_pairs:
        print("\nNo mergeable Yes/No sets found. Nothing to do.")
        return

    # For each mergeable position, merge Yes+No pairs
    for pair in mergeable_pairs:
        api_pairs = pair["available_pairs"]

        print(f"\n{'='*60}")
        print(f"Position: {pair['title']}")
        print(f"Yes tokens: {pair['yes_size']:.4f}")
        print(f"No tokens: {pair['no_size']:.4f}")
        print(f"Pairs to merge (API est.): {api_pairs}")
        target_ctf = NEGRISK_CTF if pair.get("negative_risk") else STANDARD_CTF
        print(f"CTF contract: {target_ctf}")
        ctf_contract = w3.eth.contract(address=target_ctf, abi=CTF_ABI)

        yes_asset = pair.get("yes_asset")
        no_asset = pair.get("no_asset")
        if not yes_asset or not no_asset:
            print("Missing asset IDs, skipping.")
            continue

        try:
            yes_balance_raw = safe_contract_call(lambda: ctf_contract.functions.balanceOf(address, int(yes_asset)).call())
            no_balance_raw = safe_contract_call(lambda: ctf_contract.functions.balanceOf(address, int(no_asset)).call())
        except Exception as e:
            print(f"❌ Failed to fetch on-chain balances: {e}")
            continue

        yes_tokens_onchain = yes_balance_raw / 1_000_000
        no_tokens_onchain = no_balance_raw / 1_000_000
        onchain_pairs_tokens = min(yes_tokens_onchain, no_tokens_onchain)
        merge_units = min(yes_balance_raw, no_balance_raw)

        print(f"On-chain Yes tokens: {yes_tokens_onchain:.6f}")
        print(f"On-chain No tokens: {no_tokens_onchain:.6f}")
        print(f"Pairs to merge (on-chain): {onchain_pairs_tokens:.6f}")

        if merge_units <= 0:
            print("No on-chain pairs available, skipping.")
            continue

        expected_usdc = onchain_pairs_tokens
        print(f"Expected USDC.e to receive: ~${expected_usdc:.2f}")

        # Prepare merge transaction
        try:
            # For binary markets, partition is [1, 2] representing Yes + No
            partition = [1, 2]
            # Polymarket ERC1155 tokens use 6 decimal places (1 token = 1_000_000 units)
            amount = merge_units

            # parentCollectionId is null (all zeros) for Polymarket
            parent_collection_id = "0x0000000000000000000000000000000000000000000000000000000000000000"

            # Build transaction
            # Add this block right before the transaction building code
            # Simulate transaction locally to catch reverts
            try:
                result = ctf_contract.functions.mergePositions(
                    USDC_E,
                    parent_collection_id,
                    pair["condition_id"],
                    partition,
                    amount
                ).call({"from": address})
                print(f"✅ Simulation successful: {result}")
            except Exception as e:
                print(f"❌ Simulation failed: {e}")
                continue
            tx = ctf_contract.functions.mergePositions(
                USDC_E,
                parent_collection_id,
                pair["condition_id"],
                partition,
                amount
            ).build_transaction({
                'from': address,
                'gas': 500000,  # Gas limit estimate
                'gasPrice': w3.eth.gas_price,
                'nonce': w3.eth.get_transaction_count(address),
            })
            
            # Sign transaction
            signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            
            print(f"\nTransaction details:")
            print(f"Gas limit: {tx['gas']:,}")
            print(f"Gas price: {w3.from_wei(tx['gasPrice'], 'gwei')} gwei")
            print(f"Estimated cost: ~{w3.from_wei(tx['gas'] * tx['gasPrice'], 'ether')} MATIC")
            
            # Ask for confirmation
            confirm = input("\nProceed with merge? (y/N): ").strip().lower()
            if confirm != 'y':
                print("Cancelled.")
                continue
            
            # Send transaction
            print("Sending transaction...")
            raw_tx = getattr(signed_tx, "rawTransaction", None) or getattr(signed_tx, "raw_transaction")
            tx_hash = w3.eth.send_raw_transaction(raw_tx)
            print(f"Transaction hash: {tx_hash.hex()}")
            
            # Wait for confirmation
            print("Waiting for confirmation...")
            receipt = wait_for_receipt_with_retry(w3, tx_hash)
            if receipt is None:
                print("⚠️  Could not confirm transaction due to RPC rate limits. Check the hash on Polygonscan and rerun later.")
                continue
            
            if receipt.status == 1:
                print(f"✅ SUCCESS! Merge completed in block {receipt.block_number}")
                print(f"Gas used: {receipt.gasUsed:,}")
                
                # Check new USDC.e balance
                new_balance = safe_contract_call(lambda: usdc_contract.functions.balanceOf(address).call())
                new_usdc = new_balance / 1_000_000
                gained = new_usdc - (usdc_balance / 1_000_000)
                print(f"USDC.e balance: ${new_usdc:.2f} (+${gained:.2f})")
                usdc_balance = new_balance
            else:
                print("❌ Transaction failed!")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
    
    print(f"\n{'='*60}")
    final_balance = safe_contract_call(lambda: usdc_contract.functions.balanceOf(address).call())
    final_usdc = final_balance / 1_000_000
    print(f"Final USDC.e balance: ${final_usdc:.2f}")

if __name__ == "__main__":
    main()
