import time
import kshingle
from datasketch import MinHash, MinHashLSH
import wikidata
import numpy as np
from sys import getsizeof
import wikipedia
import re
import csv
import progressbar
import yake


# global variables
number_of_results = 3
minhash_threshold = 0.3
minhash_num_perm = 128
shingle_size = 6
query_datapoints = 500
keywords_in_sig = 25

widgets = [
    ' [', progressbar.Timer(), '] ',
    progressbar.Bar(),
    ' (', progressbar.ETA(), ') ',
]

# ---------------------------------------------------
# helper functions

# works on the subset of wikipedia in Frysk

def setup():
    print("Getting titles and parameter setup...")
    wd = wikidata.wikidata("fy_all_nopic.zim")
    titles = wd.get_titles_large()
    wikipedia.set_lang("fy")
    np.random.shuffle(titles)
    kw_extractor = yake.KeywordExtractor(top=keywords_in_sig, stopwords=None, lan='fy')
    return titles, wd, kw_extractor


def print_params():
    print()
    print("____________________________________________________________")
    print('\033[1m' + 'Running with parameters:')
    print('\033[0m' + "############################################################")
    print("Language Frysk (West Frisian)")
    print("Query data: " + str(query_datapoints))
    print("Minhash permutations: " + str(minhash_num_perm))
    print("Minhash threshold: " + str(minhash_threshold))
    print("Shingle size: " + str(shingle_size))
    print("Keywords extracted for short signature: " + str(keywords_in_sig))
    print("Number of top results for recall: " + str(number_of_results))
    print("############################################################")
    print("____________________________________________________________")
    print()


def intersection(list_a, list_b):
    return [e for e in list_a if e in list_b]


def set_dataset(titles):
    train_titles = titles
    np.random.shuffle(titles)
    query_titles = titles[query_datapoints:]
    return train_titles, query_titles


# ---------------------------------------------------
# main functions


def train_func(train_titles, wd, kw_extractor):
    # Create LSH index
    print("Retrieving and adding content...")
    lsh = MinHashLSH(threshold=minhash_threshold, num_perm=minhash_num_perm)
    success = 0
    train_data = np.zeros(shape=[len(train_titles), 4], dtype=object)

    o = 0
    with progressbar.ProgressBar(max_value=len(train_titles), widgets=widgets) as bar:
        for title in train_titles:
            bar.update(o)
            o += 1
            try:
                txt_file = wd.get_content_txt(title)
                s = txt_file.replace("\n", " ")
                s = re.sub(' +', ' ', s)
                title = title.replace("A/", "")

                fulltext = title + ", " + s

                shingles = kshingle.shingleset_k(s, k=shingle_size)
                m = MinHash(num_perm=minhash_num_perm)
                for d in shingles:
                    m.update(d.encode('utf8'))
                lsh.insert(title, m)

                keywords = kw_extractor.extract_keywords(fulltext)
                mkeyword = MinHash(num_perm=minhash_num_perm)
                for k, _ in keywords:
                    mkeyword.update(k.encode('utf8'))

                train_data[success] = title, shingles, m, mkeyword
                success = success + 1
            except:
                print("issue in retrieving '" + title + "' , continuing...")
                continue

    print(str(success) + " articles found out of " + str(len(train_titles)) + " items.")
    return lsh, train_data


def query_func(query_titles, wd, lsh, train_data, shing, kwe, writer1):
    print("Starting queries...")
    ac = 0
    sc = 0
    skw = 0

    ooo = 0
    with progressbar.ProgressBar(max_value=len(query_titles), widgets=widgets) as bar:
        for query in query_titles:

            bar.update(ooo)
            ooo += 1

            try:
                # ---------------------------------------------------
                # get lsh neighbours
                txt_file = wd.get_content_txt(query)
                s = txt_file.replace("\n", " ")
                s = re.sub(' +', ' ', s)
                query = query.replace("A/", "")

                shingles = kshingle.shingleset_k(s, k=shing)
                m = MinHash(num_perm=minhash_num_perm)
                for d in shingles:
                    m.update(d.encode('utf8'))

                t0 = time.time()
                result_lsh = lsh.query(m)
                t1 = time.time()
                lsh_band_delay = t1 - t0

                # ---------------------------------------------------
                # get actual closest neighbours
                t2 = time.time()
                closest = []
                for n in range(0, number_of_results):
                    closest_distance = 0.
                    closest += ['']

                    for item in train_data:
                        try:
                            if item[0] != 0:

                                distance = float(len(shingles.intersection(item[1]))) / float(
                                    len(shingles.union(item[1])))

                                if closest[n] == '':
                                    if item[0] not in closest:
                                        closest[n] = item[0]
                                        closest_distance = distance
                                elif closest_distance < distance:
                                    if item[0] not in closest:
                                        closest[n] = item[0]
                                        closest_distance = distance
                        except:
                            print("error occured finding actual closest")
                            ac += 1
                            continue

                t3 = time.time()
                shingle_delay = t3 - t2

                shingle_size = (getsizeof(shingles))

                # ---------------------------------------------------
                #   get signature based closest
                t4 = time.time()
                closest_lsh = []
                for n in range(0, number_of_results):
                    closest_distance = 0.
                    closest_lsh += ['']

                    for item in train_data:
                        try:
                            if item[0] != 0:
                                distance = m.jaccard(item[2])
                                if closest_lsh[n] == '':
                                    if item[0] not in closest_lsh:
                                        closest_lsh[n] = item[0]
                                        closest_distance = distance
                                elif closest_distance < distance:
                                    if item[0] not in closest_lsh:
                                        closest_lsh[n] = item[0]
                                        closest_distance = distance
                        except:
                            print("error occurred getting signature closest")
                            sc += 1
                            continue

                t5 = time.time()
                sig_size = (getsizeof(m))
                sig_delay = t5 - t4

                # ---------------------------------------------------
                #   get keyword signature based closest

                fulltext = query + ", " + s
                keywords = kwe.extract_keywords(fulltext)
                m2 = MinHash(num_perm=minhash_num_perm)
                for k, _ in keywords:
                    m2.update(k.encode('utf8'))

                t6 = time.time()
                closest_key = []
                for n in range(0, number_of_results):
                    closest_distance = 0.
                    closest_key += ['']

                    for item in train_data:
                        try:
                            if item[0] != 0:
                                distance = m2.jaccard(item[3])
                                if closest_key[n] == '':
                                    if item[0] not in closest_key:
                                        closest_key[n] = item[0]
                                        closest_distance = distance
                                elif closest_distance < distance:
                                    if item[0] not in closest_key:
                                        closest_key[n] = item[0]
                                        closest_distance = distance
                        except:
                            print("error occurred getting keyword signature closest")
                            skw += 1
                            continue

                t7 = time.time()
                keyword_delay = t7 - t6
                key_sig_size = (getsizeof(m2))

                # ---------------------------------------------------
                #   get wikipedia search based closest
                t8 = time.time()
                wsd = wikipedia.search(query.title(), results=number_of_results)
                t9 = time.time()
                wd_delay = t7 - t6
                wsd = [w.replace(' ', '_') for w in wsd]

                # ---------------------------------------------------
                # Getting recall scores

                lsh_band_recall = len(intersection(closest, result_lsh)) / len(result_lsh)
                sig_recall = len(intersection(closest, closest_lsh)) / len(closest_lsh)
                keyword_recall = len(intersection(closest, closest_key)) / len(closest_key)
                wd_recall = len(intersection(closest, wsd)) / len(wsd)

                row = ["Frysk",shing, minhash_num_perm, minhash_threshold,
                       number_of_results, keywords_in_sig , query, shingle_delay,
                       lsh_band_delay, sig_delay, keyword_delay, wd_delay, lsh_band_recall, sig_recall, keyword_recall,
                       wd_recall,shingle_size, sig_size, key_sig_size]
                writer1.writerow(row)
            except:
                print("issue with query: " + query)


    print()
    print("_____________________________________________________")
    print('\033[1m' + "Errors occured")
    print('\033[0m' + "Getting real closest:", ac)
    print("Getting signature closest: ", sc)
    print("Getting keyword closest: ", skw)
    print()


# ---------------------------------------------------


def main():
    topn = [3,5,7]

    titles, wd, kwe = setup()
    print_params()
    print("Training the content network...")
    train, query = set_dataset(titles)
    lsh, train_ds = train_func(train, wd, kwe)

    for n in topn:
        print("Starting run for topn: " + str(n) + "...")
        f1 = open('frysk_wiki.csv', 'a')
        writer1 = csv.writer(f1)
        query_func(query, wd, lsh, train_ds, shingle_size, kwe, writer1)
        f1.close()


if __name__ == '__main__':
    main()
