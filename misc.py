# import hashlib, base58
# import time
import kshingle
# from datasketch import MinHash, MinHashLSH, lean_minhash
import wikidata
# import numpy as np
#
wd = wikidata.wikidata()
titles = wd.get_titles()

txt_file1 = wd.get_content_txt(titles[1])
shingles1 = kshingle.shingleset_k(txt_file1, k=4)
txt_file = wd.get_content_txt(titles[2])
shingles = kshingle.shingleset_k(txt_file, k=4)
print(shingles)
print(shingles1)

# distance = float(len(s1.intersection(shingles))) / float(len(s1.union(shingles)))

# print(distance)
#
# m = MinHash(num_perm=32)
#
# for d in shingles:
#     m.update(d.encode('utf8'))
#
# lm = lean_minhash.LeanMinHash(m)
#
# ba = bytearray(lm.digest())
# id = base58.b58encode(ba)
#
# print(lm.digest())
# print(id)

# check vs leanminhash
import array

# closest = ['']
# closest += ['3']
# print(closest)

a = 0.19741379310344828
b = 0.1517979345526938
print(a-b)

if a > b:
    print("yes")
else:
    print("no")
