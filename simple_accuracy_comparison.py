# DittoSearch July 2022
# developed by Navin v. Keizer

# 2. generate SIDs, check latency, and verify the correct representation of closeness using based on Jaccard similarity

import hashlib, base58
import time
import kshingle
from datasketch import MinHash, MinHashLSH
import wikidata
import numpy as np

wd = wikidata.wikidata()

# todo we currently get a length of the byte array based on the num_perm. This results in a very large base58
#  decoded signature. We could cut this down by hashing internal states based on buckets, however this may be
#  better for future work


def sig_delay(train_titles):
    success = 0
    i = -1
    av_sig_delay = 0.

    # Create LSH index
    lsh = MinHashLSH(threshold=0.45, num_perm=128)

    for title in train_titles:
        try:
            txt_file = wd.get_content_txt(title)

            # words = set(txt_file.split())
            t0 = time.time()
            shingles = kshingle.shingleset_k(txt_file, k=4)
            m = MinHash(num_perm=128)
            for d in shingles:
                m.update(d.encode('utf8'))
            # h1 = hashlib.new('sha256')
            # h1.update(m.digest())
            # id = base58.b58encode(h1.digest())
            t1 = time.time()
            # print(t1 - t0)
            av_sig_delay = av_sig_delay + t1 - t0
            lsh.insert(title, m)
            # print(id)
            success = success + 1

        except:
            print("issue in retrieving '" + title + "' , continuing...")
            continue

    print(str(success) + " articles found out of " + str(len(train_titles)) + " items.")
    print("average signature generation delay:", str(av_sig_delay / success), "seconds.")

def generate_sig(t):
    txt_file = wd.get_content_txt(t)
    shingles = kshingle.shingleset_k(txt_file, k=4)
    m = MinHash(num_perm=64)
    for d in shingles:
        m.update(d.encode('utf8'))
    #     this part below may be unnecessary
    ba = bytearray(m.digest())
    # h1 = hashlib.new('sha256')
    # h1.update(m.digest())
    print(m.digest())
    print(base58.b58encode(ba))
    return m, shingles, base58.b58encode(ba)

def get_change(current, previous):
    if current == previous:
        return 100.0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return 0

def accuracy(titles):
    # randomly take subset of 10% and compare accuracy
    np.random.shuffle(titles)
    subset = titles[:int(len(titles) * 0.1)]
    av_dif=0.
    n=0
    for s in subset:
        for a in subset:
            if s != a:
                m1, s1, h1 = generate_sig(s)
                m2, s2, h2 = generate_sig(a)
                print(len(h1))
                estimate = m1.jaccard(m2)
                real = float(len(s1.intersection(s2)))/float(len(s1.union(s2)))
                # h_dif = float(len(h1.intersection(h2)))/float(len(h2.union(s2)))
                dif = get_change(real,estimate)
                # print("est",estimate)
                # print("real",real)
                # print(dif)
                av_dif = av_dif + dif
                n=n+1
    print("Average percentage difference of jaccard vs real similarity", av_dif/n, "%")
    return av_dif/n



def main():
    titles = wd.get_titles()

    number_query_datapoints = 10
    np.random.seed(2000)
    np.random.shuffle(titles)

    # test signature delay
    # sig_delay(titles)

    # test accuracy of SID in capturing Jaccard similarity
    accuracy(titles)



if __name__ == '__main__':
    main()

# todo redo without additional hashing (and with space optimisations)
# todo change parameters to observe how plots change e.g. signature length, window length, num_perm, threshold
