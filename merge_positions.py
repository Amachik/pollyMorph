#!/usr/bin/env python3
"""
Merge Polymarket positions using the relayer client.
Merges Yes+No pairs back to USDC.e on Polygon.

Install dependencies:
pip install web3 requests

Usage:
python merge_positions.py
"""

import os
import json
import requests
from web3 import Web3
from eth_account import Account
from eth_utils import to_checksum_address

# Configuration
PRIVATE_KEY = os.getenv("PRIVATE_KEY")  # Your wallet private key
POLYGON_RPC = "https://polygon-rpc.com"
RELAYER_URL = "https://relayer.polymarket.com"

# Contract addresses (Polygon mainnet)
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_CONTRACT = "0x4D97Dcd97eC945f40cF65F87097ACe5EA0476045"

# Your positions from data-api
POSITIONS = [
    {
        "title": "Will Trump deport 250,000-500,000 people?",
        "conditionId": "0xaf9d0e448129a9f657f851d49495ba4742055d80e0ef1166ba0ee81d4d594214",
        "yes_size": 15.0,  # Yes tokens
        "no_size": 10.0,   # No tokens
        "mergeable": True
    }
]

def main():
    if not PRIVATE_KEY:
        print("ERROR: Set PRIVATE_KEY environment variable")
        return
    
    # Initialize account
    account = Account.from_key(PRIVATE_KEY)
    address = account.address
    print(f"Wallet: {address}")
    
    # Check current USDC.e balance
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
    usdc_contract = w3.eth.contract(address=USDC_E, abi=[
        {"name": "balanceOf", "type": "function", "inputs": [{"name": "account", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]}
    ])
    
    balance = usdc_contract.functions.balanceOf(address).call()
    balance_usdc = balance / 1_000_000
    print(f"Current USDC.e balance: ${balance_usdc:.2f}")
    
    # For each mergeable position, merge Yes+No pairs
    for pos in POSITIONS:
        if not pos["mergeable"]:
            print(f"\nSkipping {pos['title']} (not mergeable)")
            continue
        
        # Calculate how many pairs we can merge
        pairs = min(pos["yes_size"], pos["no_size"])
        if pairs <= 0:
            print(f"\nSkipping {pos['title']} (no pairs to merge)")
            continue
        
        print(f"\nMerging {pairs} Yes+No pairs from: {pos['title']}")
        print(f"Will receive ~${pairs:.2f} USDC.e")
        
        # Prepare merge transaction
        merge_tx = {
            "to": CTF_CONTRACT,
            "data": encode_merge_positions(
                collateral_token=USDC_E,
                parent_collection_id="0x0000000000000000000000000000000000000000000000000000000000000000",  # Null for Polymarket
                condition_id=pos["conditionId"],
                partition=[1, 2],  # Yes + No for binary markets
                amount=int(pairs * 1e18)  # Amount in wei (CTF uses 18 decimals internally)
            ),
            "value": "0"
        }
        
        # Submit via relayer
        try:
            response = submit_relayer_tx(merge_tx, account, f"Merge {pairs} pairs")
            if response:
                print(f"✅ Merge submitted! Transaction: {response.get('hash', 'unknown')}")
                print("Check your wallet in ~1 minute for USDC.e")
        except Exception as e:
            print(f"❌ Merge failed: {e}")
    
    # Check final balance
    final_balance = usdc_contract.functions.balanceOf(address).call()
    final_usdc = final_balance / 1_000_000
    print(f"\nFinal USDC.e balance: ${final_usdc:.2f} (+${final_usdc - balance_usdc:.2f})")

def encode_merge_positions(collateral_token, parent_collection_id, condition_id, partition, amount):
    """Encode mergePositions function call"""
    # Function selector for mergePositions(uint256,address,bytes32,bytes32,uint256[],uint256)
    selector = Web3.keccak(text="mergePositions(uint256,address,bytes32,bytes32,uint256[],uint256)")[:4]
    
    # Pad parameters to 32 bytes each
    params = b""
    
    # amount (uint256)
    params += amount.to_bytes(32, byteorder='big')
    
    # collateral_token (address)
    params += bytes.fromhex(collateral_token.replace('0x', '').zfill(40))
    params += b'\x00' * 12  # Pad to 32 bytes
    
    # parent_collection_id (bytes32)
    params += bytes.fromhex(parent_collection_id.replace('0x', ''))
    
    # condition_id (bytes32)
    params += bytes.fromhex(condition_id.replace('0x', ''))
    
    # partition (uint256[])
    # First encode array length
    params += len(partition).to_bytes(32, byteorder='big')
    # Then encode each partition element
    for p in partition:
        params += p.to_bytes(32, byteorder='big')
    
    return "0x" + (selector + params).hex()

def submit_relayer_tx(tx, account, description):
    """Submit transaction via Polymarket relayer"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('POLY_BUILDER_API_KEY', '')}"
    }
    
    payload = {
        "transactions": [tx],
        "description": description,
        "signature": sign_relayer_payload(tx, account)
    }
    
    response = requests.post(f"{RELAYER_URL}/execute", json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def sign_relayer_payload(tx, account):
    """Sign the relayer payload (simplified - you'd need to implement proper EIP-712 signing)"""
    # This is a placeholder - proper implementation requires EIP-712 domain and struct hashing
    message_hash = Web3.keccak(text=json.dumps(tx, separators=(',', ':')))
    signature = account.sign_message(message_hash)
    return "0x" + signature.signature.hex()

if __name__ == "__main__":
    main()
