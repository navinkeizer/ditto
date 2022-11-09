# DittoSearch July 2022
# developed by Navin v. Keizer
import kshingle
from datasketch import MinHash


class node:

    # initialise connections and set up parameters such as PeerID, peers, etc.
    def __init__(self):
        print()

    # main functions
    # -------------------------------------------------------------------------

    # generate a signature and send it to the closest N peers we know,
    # if our PID is within threshold value store it locally
    def insert(self):
        print()

    # generate the query from the document we have, send it to N closest peers
    # along with parameters like hops
    # wait for incoming results
    def query(self):
        print()

    # supporting functions
    # -------------------------------------------------------------------------

    # function generates the LSH signature based on shingling and minhashing
    def generate_signature(self, content):
        shingles = kshingle.shingleset_k(content,k=4)
        m = MinHash(num_perm=128)
        for d in shingles:
            m.update(d.encode('utf8'))
        return m

    def create_id(self):
        print()



    def receive(self):
        print()

    def forward(self):
        print()

    def connect_peer(self):
        print

    def store(self):
        print()

    # this function can be used to verify a returned result is correct
    # we can use this is our security evaluations
    def verify_results(self):
        print()

# can produce the signatures of the wiki data separately and then compare performance to centralised solution

# set up parameters for the simulations
threshold = 0
range = 0
signature_length=0


def main():
    # create a bunch of node instances
    # connect to top N closest peers
    # start inserting the training data and measure latencies and performance
    # start querying the test data and log the results
    # may add networking delays, effects etc.
    print()