import hashlib
import json
import math
import requests

from time import time
from urllib.parse import urlparse


class Blockgrid(object):
    def __init__(self):
        self.grid = {}
        self.nodes = set()

        # Create the genesis block
        self.new_block(previous_hash=0, index=(0, 0, 0))

    def new_block(self, index, previous_hash):
        """
        Create a new Block in the Blockgrid
        :param proof: <int> The proof given by the Proof of Work algorithm
        :param index: <int> The index of the block being added
        :param owner: <int> The public key of the block owner
        :param previous_hash: (Optional) <str> Hash of previous Block
        :return: <dict> New Block
        """

        block = {
            'index': index,
            'timestamp': time(),
            'data': [],
            'owner': None,
            'previous_hash': previous_hash,
        }

        self.grid[index] = block
        return block

    def new_transaction(self, index, data, signature):
        """
        Creates a new transaction to go into the next mined Block
        :param index: <str> Index of the block
        :param data: <str> Data being stored in the block
        :param signature: <str> Signature of the owner of the block
        :return: <int> The index of the Block that will hold this transaction
        """

        self.grid[index].data.append({
            'data': data,
            'signature': signature,
        })

        return self.last_block(index)['index']

    def register_node(self, address):
        """
        Add a new node to the list of nodes
        :param address: <str> Address of node. Eg. 'http://192.168.0.5:5000'
        :return: None
        """

        parsed_url = urlparse(address)
        self.nodes.add(parsed_url.netloc)

    def valid_chain(self, grid):
        """
        Determine if a given Blockgrid is valid
        :param grid: <list> A Blockgrid
        :return: <bool> True if valid, False if not
        """

        for k, v in grid.iteritems():
            block = v
            print(f'{grid.last_block(k)}')
            print(f'{block}')
            print("\n-----------\n")
            # Check that the hash of the block is correct
            if block['previous_hash'] != self.hash(grid.last_block(k)):
                return False

            # Check that the Proof of Work is correct
            if not self.valid_proof(grid.last_block(k)['proof'], block['proof'], k):
                return False

        return True

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

                # Check if the length is longer and the chain is valid
                if length > max_length and self.valid_chain(grid):
                    max_length = length
                    new_grid = grid

        # Replace our chain if we discovered a new, valid chain longer than ours
        if new_grid:
            self.grid = new_grid
            return True

        return False

    @staticmethod
    def hash(block):
        """
        Creates a SHA-256 hash of a Block
        :param block: <dict> Block
        :return: <str>
        """

        # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
        block_string = json.dumps({k: v for k, v in block.items() if k != "data"}, sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    @staticmethod
    def hash_without_proof(block):
        """
        Creates a SHA-256 hash of a Block without the proof field (used for proof-of-work)
        :param block: <dict> Block
        :return: <str>
        """

        # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
        block_string = json.dumps({k: v for k, v in block.items() if k != "data" and k != "proof"}, sort_keys=True).encode()
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
