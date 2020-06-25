from uuid import uuid4
from flask import Flask, jsonify, request

from blockgrid import Blockgrid

app = Flask(__name__)
node_identifier = str(uuid4()).replace('-', '')
blockgrid = Blockgrid()


@app.route('/mine', methods=['GET'])
def mine():
    # We run the proof of work algorithm to get the next proof...
    values = request.get_json()

    index = tuple(values["index"])
    last_block = blockgrid.last_block(index)
    last_proof = last_block['proof']
    proof = blockgrid.proof_of_work(last_proof, index)

    # We must receive a reward for finding the proof.
    # The sender is "0" to signify that this node has mined a new coin.
    blockgrid.new_transaction(
        index=index,
        sender="0",
        recipient=node_identifier,
        amount=1,
    )

    # Forge the new Block by adding it to the chain
    previous_hash = blockgrid.hash(last_block)
    block = blockgrid.new_block(proof, index, previous_hash)

    response = {
        'message': "New Block Forged",
        'index': block['index'],
        'transactions': block['transactions'],
        'proof': block['proof'],
        'previous_hash': block['previous_hash'],
    }
    return jsonify(response), 200


@app.route('/transactions/new', methods=['POST'])
def new_transaction():
    values = request.get_json()

    # Check that the required fields are in the POST'ed data
    required = ['sender', 'recipient', 'amount']
    if not all(k in values for k in required):
        return 'Missing values', 400

    # Create a new Transaction
    index = blockgrid.new_transaction(values['sender'], values['recipient'], values['amount'])

    response = {'message': f'Transaction will be added to Block {index}'}
    return jsonify(response), 200


@app.route('/grid', methods=['GET'])
def full_chain():
    response = {
        'chain': blockgrid.grid,
        'length': len(blockgrid.grid),
    }
    return jsonify(response), 200


@app.route('/nodes/register', methods=['POST'])
def register_nodes():
    values = request.get_json()

    nodes = values.get('nodes')
    if nodes is None:
        return "Error: Please supply a valid list of nodes", 400

    for node in nodes:
        blockgrid.register_node(node)

    response = {
        'message': 'New nodes have been added',
        'total_nodes': list(blockgrid.nodes),
    }
    return jsonify(response), 201


@app.route('/nodes/resolve', methods=['GET'])
def consensus():
    replaced = blockgrid.resolve_conflicts()

    if replaced:
        response = {
            'message': 'Our chain was replaced',
            'new_chain': blockgrid.grid
        }
    else:
        response = {
            'message': 'Our chain is authoritative',
            'chain': blockgrid.grid
        }

    return jsonify(response), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
