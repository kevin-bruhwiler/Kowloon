import hashlib
import json
import math
import requests
import pickle
import sys

import boto3
from boto3.dynamodb.conditions import Key

from time import time, sleep
from urllib.parse import urlparse

from sign import verify


class Blockgrid(object):
    def __init__(self):
        with open("accesskey", "r") as ak, open("./secretkey", "r") as sk:
            self.dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-2.amazonaws.com",
                                           region_name='us-east-2',
                                           aws_access_key_id=ak.read(),
                                           aws_secret_access_key=sk.read())
        self.table = self.dynamodb.Table('Grid')
        self.dynamodb_client = boto3.client('dynamodb', region_name='us-east-2')
        self.grid = self.load_grid()
        self.nodes = set()
        self.asset_bundles = dict()

        # Create the genesis block
        if len(self.grid) == 0:
            self.new_block(previous_hash=0, index=(0, 0, 0), previous_index=(0, 0, 0))

    def load_grid(self):
        """
        Read the grid from dynamodb
        :return: <dict> The grid
        """
        grid = {}
        for i in range(10):
            for j in range(10):
                for k in range(10):
                    block = ""
                    ix = 0
                    while True:
                        out = self.persistent_query(Key('index').eq(str((i, j, k)) + "_" + str(ix)))['Items']
                        if len(out) == 0:
                            break
                        block += out[0]["block"]
                        ix += 1

                    if len(block) > 0:
                        grid[(i, j, k)] = json.loads(block)
        return grid

    def save_block(self, idx, block):
        """
        Save a block to dynamodb
        :param idx: <tuple> The index of the block being saved
        :param block: <string> The contents of the block being saved
        :return:
        """
        x = 400000 // 4
        block = json.dumps(block)
        chunks = [block[y - x:y] for y in range(x, len(block) + x, x)]
        ix = 0

        for chunk in chunks:
            self.persistent_put({"index": str(idx) + "_" + str(ix), "block": chunk})
            ix += 1
            sleep(3.0)

    def persistent_put(self, item):
        result = None
        while result is None:
            try:
                self.table.put_item(Item=item)
            except self.dynamodb_client.exceptions.ProvisionedThroughputExceededException:
                sleep(3.0)
                continue
            result = True

    def persistent_query(self, kce):
        result = None
        while result is None:
            try:
                result = self.table.query(KeyConditionExpression=kce)
            except self.dynamodb_client.exceptions.ProvisionedThroughputExceededException:
                sleep(3.0)
                pass
        return result

    def new_block(self, index, previous_hash, previous_index):
        """
        Create a new Block in the Blockgrid
        :param index: <int> The index of the block being added
        :param previous_hash: <str> Hash of the previous Block
        :param previous_index: <tuple> Index of the previous Block
        :return: <dict> New Block
        """

        block = {
            'index': tuple(index),
            'timestamp': int(time()),
            'updated': int(time()),
            'data': [],
            'proof': None,
            'owner': None,
            'previous_hash': previous_hash,
            'previous_index': tuple(previous_index)
        }

        self.grid[index] = block
        self.save_block(index, block)
        return block

    def new_transaction(self, index, data, signature, millis, approved):
        """
        Creates a new transaction to go into the next mined Block
        :param index: <str> Index of the block
        :param data: <str> Data being stored in the block
        :param signature: <str> Signature of the owner of the block
        :return: <int> The index of the Block that will hold this transaction
        """

        self.grid[index]["data"].append({
            'data': data,
            'signature': signature,
            'updated': millis,
            'approved': approved
        })

        self.grid[index]["updated"] = millis
        for item in self.grid[index]["data"]:
            for k, v in item.items():
                print(k, sys.getsizeof(v))
        self.save_block(index, self.grid[index])

        return index

    def sign_block(self, index, proof, owner):
        """
        Adds a proof of work and an owner to an empty block
        :param index: <str> Index of the block
        :param proof: <str> The proof of work for the block
        :param owner: <str> The public key of the block miner
        :return: <int> The index of the Block that will hold this transaction
        """
        self.grid[index]["owner"] = owner
        self.grid[index]["proof"] = proof
        self.save_block(index, self.grid[index])
        previous_hash = self.hash(self.grid[index])

        # Add adjacent unsigned blocks
        for i in range(len(index)):
            for j in (-1, 1):
                l_index = list(index)
                l_index[i] += j
                new_index = tuple(l_index)
                if new_index not in self.grid:
                    self.new_block(new_index, previous_hash, tuple(index))

    def register_node(self, address):
        """
        Add a new node to the list of nodes
        :param address: <str> Address of node. Eg. 'http://192.168.0.5:5000'
        :return: None
        """

        parsed_url = urlparse(address)
        self.nodes.add(parsed_url.netloc)

    def replace_grid(self, other_grid):
        """
        Replaces the current grid with a new gird - primarily used for testing
        :param other_grid: <dict> The new grid
        :return: None
        """
        self.grid = other_grid

    def update_grid(self, longer_grid, shorter_grid):
        """
        Determine if a given Blockgrid is valid
        :param longer_grid: <list> The longer of two Blockgrids
        :param shorter_grid: <list> The shorter of two Blockgrids
        :return: <dict> The longer Blockgrid, with any updated data from the shorter grid
        """

        for idx, block in shorter_grid.items():
            # If the block is in our chain
            if idx in longer_grid:
                # If the block's proof is valid
                if self.valid_proof(self.hash_without_proof(block), block['proof'], idx):
                    # If the block in the other chain has the same owner as ours
                    if block["owner"] == longer_grid[idx]["owner"]:
                        # If that block's data has been updated more recently
                        if block["updated"] > longer_grid[idx]["updated"]:
                            longer_grid[idx]["data"] = block["data"]
                            longer_grid[idx]["updated"] = block["updated"]
                            self.save_block(idx, longer_grid[idx])
            # If the block is not in our grid, but is in the shorter valid grid
            else:
                longer_grid[idx] = block
                self.save_block(idx, block)
        return longer_grid

    def valid_gird(self, other_grid):
        """
        Determine if a given Blockgrid is valid
        :param other_grid: <list> A Blockgrid
        :return: <bool> True if valid, False if not
        """

        for k, v in other_grid.items():
            block = v

            if tuple(block["index"]) == (0, 0, 0):
                continue

            prev = tuple(block["previous_index"])
            # Check that the hash of the block is correct
            if block['previous_hash'] != self.hash(other_grid[prev]):
                return False

            # If the block has no owner we're done
            if block['owner'] is None and len(block["data"]) == 0:
                continue

            # Check that the Proof of Work is correct
            if not self.valid_proof(self.hash_without_proof(block), block['proof'], k):
                return False

            for d in block["data"]:
                if not verify(block["owner"], d["data"], d["signature"]):
                    return False

        return True

    def compare_grids(self, other_grid):
        """
        Compares two grids to determine if ours is authoritative
        :return: <bool> True if the other grid is authoritative, False if not
        """
        if self.valid_gird(other_grid) and len(other_grid) > len(self.grid):
            return True
        return False

    def resolve_conflicts(self):
        """
        This is our Consensus Algorithm, it resolves conflicts
        by replacing our chain with the longest one in the network.
        :return: <bool> True if our chain was replaced, False if not
        """

        neighbours = self.nodes
        new_grid = None

        # We're only looking for chains longer than ours
        max_length = len(self.grid)

        # Grab and verify the chains from all the nodes in our network
        for node in neighbours:
            response = requests.get(f'http://{node}/chain')

            if response.status_code == 200:
                length = response.json()['length']
                grid = response.json()['grid']

                if self.compare_grids(grid):
                    if length > max_length:
                        max_length = length
                        grid = self.update_grid(grid, self.grid)
                        new_grid = grid
                    else:
                        self.grid = self.update_grid(self.grid, grid)

        # Replace our chain if we discovered a new, valid chain longer than ours
        if new_grid:
            self.grid = new_grid
            return True

        return False

    def save(self, filename):
        with open(filename, "wb") as f:
            pickle.dump(self.grid, f, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self, filename):
        with open(filename, "rb") as f:
            self.grid = pickle.load(f)

    @staticmethod
    def hash(block):
        """
        Creates a SHA-256 hash of a Block
        :param block: <dict> Block
        :return: <str>
        """

        # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
        block_string = json.dumps({k: v for k, v in block.items() if k != "data" and k != "updated"},
                                  sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    @staticmethod
    def hash_without_proof(block):
        """
        Creates a SHA-256 hash of a Block without the proof field (used for proof-of-work)
        :param block: <dict> Block
        :return: <str>
        """

        # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
        block_string = json.dumps(
            {k: v for k, v in block.items() if k == "owner" and k == "index" and k == "previous_hash"},
            sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    def last_index(self, index):
        """
        Return the previous block in the grid for the specified index
        :param index: <tuple>
        :return: <block>
        """
        if index == (0, 0, 0):
            return self.grid[index]

        index_max = max(range(len(index)), key=lambda i: abs(index[i]))
        last_index = tuple(x if i != index_max else x - 1 * math.copysign(1, x) for i, x in enumerate(index))
        return last_index

    def proof_of_work(self, last_proof, index):
        """
        Simple Proof of Work Algorithm:
         - Find a number p' such that hash(pp') contains leading 4 zeroes, where p is the previous p'
         - p is the previous proof, and p' is the new proof
        :param last_proof: <int>
        :param index: <int>
        :return: <int>
        """

        proof = 0
        while self.valid_proof(last_proof, proof, index) is False:
            proof += 1

        return proof

    @staticmethod
    def valid_proof(last_proof, proof, index):
        """
        Validates the Proof: Does hash(last_proof, proof) contain max(index) leading zeroes?
        :param last_proof: <int> Previous Proof
        :param proof: <int> Current Proof
        :param index: <int> Current index
        :return: <bool> True if correct, False if not.
        """

        guess = f'{last_proof}{proof}'.encode()
        guess_hash = hashlib.sha256(guess).hexdigest()
        diff = max(map(abs, index))
        return guess_hash[:diff] == "0" * diff
