# Ditto July 2022
# developed by Navin v. Keizer

# 1. compare the performance of distance metrics and verify jaccard is suitable in our setting

import time
from scipy.spatial import distance
import kshingle
import wikidata
from datasketch import MinHash, MinHashLSH
import numpy as np
import hashlib, base58

# get the wikipedia top 100 dataset and article titles
wd = wikidata.wikidata()
titles = wd.get_titles()

signatures = np.zeros(shape=[len(titles), 2], dtype=object)

# compute the signature for each article
i = 0
for title in titles:
    try:
        txt_file = wd.get_content_txt(title)
        shingles = kshingle.shingleset_k(txt_file, k=4)
        m = MinHash(num_perm=128)
        for d in shingles:
            m.update(d.encode('utf8'))

        h1 = hashlib.new('sha256')
        h1.update(m.digest())
        id1 = base58.b58encode(h1.digest())
        signatures[i] = id1.decode('utf-8'), m
    except:
        print("issue in retrieving '" + title + "' , continuing...")
        continue
    i += 1

number_of_repetitions = 20
np.random.seed(2000)
time_jaccard = 0.
time_xor = 0.
time_cosine = 0.
time_eu = 0.
time_hamming = 0.
div = 0

for n in range(0, number_of_repetitions):
    print("\nRepetition", n, "...\n")
    np.random.shuffle(signatures)
    for i in range(0, len(signatures) - 2):
        try:
            sig1 = signatures[i]
            s = sig1[1]
            sig2 = signatures[i + 1]
            s2 = sig2[1]
            print("Comparing:", sig1[0], "and", sig2[0], "...")
            t0 = time.time()
            jaccard = s.jaccard(s2)
            t1 = time.time()
            print("Jaccard:", jaccard, "calculated in:", t1 - t0, "secconds")
            t2 = time.time()
            xor = (s.digest()) ^ (s2.digest())
            t3 = time.time()
            print("XOR calculated in:", t3 - t2, "seconds")
            time_xor = time_xor + t3 - t2
            time_jaccard = time_jaccard + t1 - t0
            t4=time.time()
            cosine = distance.cosine(s.digest(), s2.digest())
            t5 = time.time()
            hamming = distance.hamming(s.digest(), s2.digest())
            t6 = time.time()
            eu = distance.euclidean(s.digest(), s2.digest())
            t7 = time.time()
            print("Cosine:", cosine, "calculated in:",t5-t4, "seconds")
            print("Hamming", hamming, "calculated in:", t6-t5, "seconds")
            print("Euclidean", eu, "calculated in:", t7-t6, "seconds")
            # print()
            time_cosine = time_cosine + t5-t4
            time_eu = time_eu + t7-t6
            time_hamming = time_hamming + t6-t5

            div += 1
        except:
            print("could not compare", sig1[0], "and", sig2[0], ", continuing...")
            continue

print()
print("Average jaccard delay:", time_jaccard / div)
print("Average XOR delay:", time_xor / div)
print("Average cosine similarity delay:", time_cosine / div)
print("Average Hamming delay:", time_hamming / div)
print("Average Euclidean delay:", time_eu / div)

# hamming, euclidian
# todo add comparison to actual jaccard (can use graph from workshop paper)
# todo increase size of dataset and number of iterations