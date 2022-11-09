# DittoSearch July 2022
# developed by Navin V. Keizer

# todo add new parameters in launch

#import array
#import hashlib, base58
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


# global variables
number_of_results = 3
minhash_threshold = 0.3
minhash_num_perm = 128
shingle_size = 6
number_query_datapoints = 10
widgets = [
    ' [', progressbar.Timer(), '] ',
    progressbar.Bar(),
    ' (', progressbar.ETA(), ') ',
]


def setup():
    print("Getting titles and parameter setup...")
    wd = wikidata.wikidata("wiki_nl_all.zim")
    titles = wd.get_titles_large()
    wikipedia.set_lang("nl")
    # split dataset into train and test data
    # todo remove seed in future runs
#     np.random.seed(2000)
    np.random.shuffle(titles)
    train_titles = titles[:len(titles)-number_query_datapoints]
    query_titles = titles[len(titles)-number_query_datapoints:]
    return train_titles, query_titles, wd


def train_func(train_titles, wd):
    # Create LSH index
    print("Retrieving and adding training content...")
    lsh = MinHashLSH(threshold=minhash_threshold, num_perm=minhash_num_perm)
    success = 0

    # todo calculate once and use in future simulations
    train_data = np.zeros(shape=[len(train_titles),3], dtype=object)

    o = 0
    with progressbar.ProgressBar(max_value=len(train_titles),  widgets=widgets) as bar:
        for title in train_titles:
            bar.update(o)
            o +=1
            try:
                txt_file = wd.get_content_txt(title)
                # process text to exclude special characters
                s = txt_file.replace("\n", " ")
                s = re.sub(' +', ' ', s)
                shingles = kshingle.shingleset_k(s, k=shingle_size)
                m = MinHash(num_perm=minhash_num_perm)
                for d in shingles:
                    m.update(d.encode('utf8'))
                lsh.insert(title, m)
                train_data[success] = title, shingles, m
                success = success + 1
            except:
                print("issue in retrieving '" + title + "' , continuing...")
                continue


    print(str(success) + " articles found out of " + str(len(train_titles)) + " items.")
    return lsh, train_data


def query_func(query_titles, wd, lsh, train_data, writer1, writer2, shinglesz):
    print("Starting queries...")
    av_shingle_size = 0
    av_sig_size = 0
    av_lsh_band_delay = 0.
    av_shingle_delay = 0.
    av_sig_delay = 0.
    av_wd_delay = 0.
    av_wd_recall = 0.
    av_sig_recall = 0.
    av_lsh_band_recall = 0.
    z = 0
    z1 = 0
    z2 = 0
    z3 = 0
    z4 = 0

    ooo=0
    with progressbar.ProgressBar(max_value=len(query_titles),  widgets=widgets) as bar:
        for query in query_titles:

            bar.update(ooo)
            ooo+=1
            # print()
            # print("Qeury : ", '\033[1m' + query)
            # print('\033[0m' + "_____________________________________________________")
            # print('\033[1m' + "Closest neighbours")

            try:
                # get lsh neighbours
                txt_file = wd.get_content_txt(query)
                s = txt_file.replace("\n", " ")
                s = re.sub(' +', ' ', s)
                shingles = kshingle.shingleset_k(s, k=shinglesz)
                m = MinHash(num_perm=minhash_num_perm)
                for d in shingles:
                    m.update(d.encode('utf8'))
                t0 = time.time()
                result_lsh = lsh.query(m)
                t1 = time.time()
                # todo fix size off for lsh banding (?)
                lsh_band_delay = t1-t0
                av_lsh_band_delay += lsh_band_delay
                z2 += 1

                # get actual closest neighbours
                closest = []
                closest_lsh = []

                t2 = time.time()
                for n in range (0,number_of_results):
                    # t2 = time.time()
                    closest_distance=0.
                    closest += ['']


                    for item in train_data:
                        try:
                            if item[0] != 0:
                                    distance = float(len(shingles.intersection(item[1])))/float(len(shingles.union(item[1])))

                                    if closest[n] == '':
                                        if item[0] not in closest:
                                            closest[n] = item[0]
                                            closest_distance = distance
                                    elif closest_distance < distance:
                                        if item[0] not in closest:
                                            closest[n] = item[0]
                                            closest_distance=distance
                        except:
                            print("error occured")
                            continue

                t3 = time.time()
                shingle_delay = t3 - t2
                av_shingle_delay += shingle_delay
                shingle_size = (getsizeof(shingles))
                av_shingle_size += shingle_size
                z += 1
                # print('\033[0m' + "Real (shingle jaccard): ", closest)
                # print("Band LSH : ", result_lsh)

            #   get signature based closest
                t4 = time.time()
                for n in range (0,number_of_results):
                    closest_distance=0.
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
                                        closest_distance=distance
                        except:
                            print("error occurred")
                            continue

                t5 = time.time()
                sig_size = (getsizeof(m))
                av_sig_size += sig_size
                sig_delay = t5 - t4
                av_sig_delay += sig_delay
                z1 += 1
                # print("Signature jaccard: ", closest_lsh)


                t6 = time.time()
                wsd = wikipedia.search(query.title(), results=number_of_results +1)[1:]
                t7 = time.time()
                wsd = [w.replace(' ', '_') for w in wsd]

                wd_delay = t7-t6
                av_wd_delay +=wd_delay
                z4 +=1

                lsh_band_recall = len(intersection(closest, result_lsh)) / len(result_lsh)
                av_lsh_band_recall += lsh_band_recall
                sig_recall = len(intersection(closest, closest_lsh)) / len(closest_lsh)
                av_sig_recall += sig_recall
                wd_recall = len(intersection(closest, wsd)) / len(wsd)
                av_wd_recall += wd_recall
                z3+=1

                # print("Wikipedia Search : ", wsd)
                # print("_____________________________________________________")
                # print('\033[1m' + "Delay")
                # print('\033[0m' + "Real (shingle jaccard) : ", shingle_delay , "seconds")
                # print("LSH bands : ", lsh_band_delay, "seconds")
                # print("Signature jaccard : ", sig_delay , "seconds")
                # print("Wikipedia Search : ", wd_delay, "seconds")
                # print("_____________________________________________________")
                # print('\033[1m' + "Size in memory")
                # print('\033[0m' + "Real (shingle jaccard)", int(shingle_size))
                # print("Signature jaccard", int(sig_size))
                # print("_____________________________________________________")
                # print('\033[1m' + "Recall (vs Real)")
                # print('\033[0m' + "LSH bands : ", lsh_band_recall)
                # print("Signature jaccard : ", sig_recall)
                # print("Wikipedia Search : ", wd_recall)
                # print()
                row = [shinglesz, minhash_num_perm, minhash_threshold, number_of_results, query, shingle_delay, lsh_band_delay, sig_delay,
                       wd_delay, lsh_band_recall, sig_recall, wd_recall]
                writer2.writerow(row)

            except:
                print("issue with query: " + query)

    try:
        # print()
        # print("_____________________________________________________")
        # print('\033[1m' + "Global stats")
        # print('\033[0m' +"_____________________________________________________")
        # print('\033[1m' + "Average delay")
        # print('\033[0m' + "Real (shingle jaccard) : ", av_shingle_delay / z, "seconds")
        # print("LSH bands : ", av_lsh_band_delay / z2, "seconds")
        # print("Signature jaccard : ", av_sig_delay / z1, "seconds")
        # print("Wikipedia Search : ", av_wd_delay / z4, "seconds")
        # print("_____________________________________________________")
        # print('\033[1m' + "Average size in memory")
        # print('\033[0m' + "Real (shingle jaccard)", int(av_shingle_size / z))
        # print("Signature jaccard", int(av_sig_size / z1))
        # print("_____________________________________________________")
        # print('\033[1m' + "Recall (vs Real)")
        # print('\033[0m' + "LSH bands : ", av_lsh_band_recall / z3)
        # print("Signature jaccard : ", av_sig_recall / z3)
        # print("Wikipedia Search : ", av_wd_recall / z3)
        # print()
        row = [shinglesz, minhash_num_perm, minhash_threshold, number_of_results, av_shingle_delay / z, av_lsh_band_delay / z2, av_sig_delay / z1, av_wd_delay / z4,int(av_shingle_size / z),
               int(av_sig_size / z1),av_lsh_band_recall / z3, av_sig_recall / z3 ,av_wd_recall / z3]
        writer1.writerow(row)
    except:
        print("issue writing global stats")


def query_test(query_titles, wd, lsh, train_data, shinglesz):
    print("Starting queries...")
    av_shingle_size = 0
    av_sig_size = 0
    av_lsh_band_delay = 0.
    av_shingle_delay = 0.
    av_sig_delay = 0.
    av_wd_delay = 0.
    av_wd_recall = 0.
    av_sig_recall = 0.
    av_lsh_band_recall = 0.
    z = 0
    z1 = 0
    z2 = 0
    z3 = 0
    z4 = 0

    oo=0
    with progressbar.ProgressBar(max_value=len(query_titles),  widgets=widgets) as bar:
        for query in query_titles:
            bar.update(00)
            oo +=1

            print()
            print("Qeury : ", '\033[1m' + query)
            print('\033[0m' + "_____________________________________________________")
            print('\033[1m' + "Closest neighbours")

            try:
                # get lsh neighbours
                txt_file = wd.get_content_txt(query)
                s = txt_file.replace("\n", " ")
                s = re.sub(' +', ' ', s)
                shingles = kshingle.shingleset_k(s, k=shinglesz)
                m = MinHash(num_perm=minhash_num_perm)
                for d in shingles:
                    m.update(d.encode('utf8'))
                t0 = time.time()
                result_lsh = lsh.query(m)
                t1 = time.time()

                lsh_band_delay = t1-t0
                av_lsh_band_delay += lsh_band_delay
                z2 += 1

                # get actual closest neighbours
                closest = []
                closest_lsh = []

                t2 = time.time()
                for n in range (0,number_of_results):
                    # t2 = time.time()
                    closest_distance=0.
                    closest += ['']


                    for item in train_data:
                        try:
                            if item[0] != 0:
                                    distance = float(len(shingles.intersection(item[1])))/float(len(shingles.union(item[1])))

                                    if closest[n] == '':
                                        if item[0] not in closest:
                                            closest[n] = item[0]
                                            closest_distance = distance
                                    elif closest_distance < distance:
                                        if item[0] not in closest:
                                            closest[n] = item[0]
                                            closest_distance=distance
                        except:
                            print("error occured")
                            continue

                t3 = time.time()
                shingle_delay = t3 - t2
                av_shingle_delay += shingle_delay
                shingle_size = (getsizeof(shingles))
                av_shingle_size += shingle_size
                z += 1
                print('\033[0m' + "Real (shingle jaccard): ", closest)
                print("Band LSH : ", result_lsh)

            #   get signature based closest
                t4 = time.time()
                for n in range (0,number_of_results):
                    closest_distance=0.
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
                                        closest_distance=distance
                        except:
                            print("error occurred")
                            continue

                t5 = time.time()
                sig_size = (getsizeof(m))
                av_sig_size += sig_size
                sig_delay = t5 - t4
                av_sig_delay += sig_delay
                z1 += 1
                print("Signature jaccard: ", closest_lsh)


                t6 = time.time()
                wsd = wikipedia.search(query.title(), results=4)[1:]
                t7 = time.time()
                wsd = [w.replace(' ', '_') for w in wsd]

                wd_delay = t7-t6
                av_wd_delay +=wd_delay
                z4 +=1

                lsh_band_recall = len(intersection(closest, result_lsh)) / len(result_lsh)
                av_lsh_band_recall += lsh_band_recall
                sig_recall = len(intersection(closest, closest_lsh)) / len(closest_lsh)
                av_sig_recall += sig_recall
                wd_recall = len(intersection(closest, wsd)) / len(wsd)
                av_wd_recall += wd_recall
                z3+=1

                print("Wikipedia Search : ", wsd)
                print("_____________________________________________________")
                print('\033[1m' + "Delay")
                print('\033[0m' + "Real (shingle jaccard) : ", shingle_delay , "seconds")
                print("LSH bands : ", lsh_band_delay, "seconds")
                print("Signature jaccard : ", sig_delay , "seconds")
                print("Wikipedia Search : ", wd_delay, "seconds")
                print("_____________________________________________________")
                print('\033[1m' + "Size in memory")
                print('\033[0m' + "Real (shingle jaccard)", int(shingle_size))
                print("Signature jaccard", int(sig_size))
                print("_____________________________________________________")
                print('\033[1m' + "Recall (vs Real)")
                print('\033[0m' + "LSH bands : ", lsh_band_recall)
                print("Signature jaccard : ", sig_recall)
                print("Wikipedia Search : ", wd_recall)
                print()
                row = [shinglesz, minhash_num_perm, minhash_threshold, number_of_results, query, shingle_delay, lsh_band_delay, sig_delay,
                       wd_delay, lsh_band_recall, sig_recall, wd_recall]
                print(row)
            except:
                print("issue with query: " + query)

    try:
        print()
        print("_____________________________________________________")
        print('\033[1m' + "Global stats")
        print('\033[0m' +"_____________________________________________________")
        print('\033[1m' + "Average delay")
        print('\033[0m' + "Real (shingle jaccard) : ", av_shingle_delay / z, "seconds")
        print("LSH bands : ", av_lsh_band_delay / z2, "seconds")
        print("Signature jaccard : ", av_sig_delay / z1, "seconds")
        print("Wikipedia Search : ", av_wd_delay / z4, "seconds")
        print("_____________________________________________________")
        print('\033[1m' + "Average size in memory")
        print('\033[0m' + "Real (shingle jaccard)", int(av_shingle_size / z))
        print("Signature jaccard", int(av_sig_size / z1))
        print("_____________________________________________________")
        print('\033[1m' + "Recall (vs Real)")
        print('\033[0m' + "LSH bands : ", av_lsh_band_recall / z3)
        print("Signature jaccard : ", av_sig_recall / z3)
        print("Wikipedia Search : ", av_wd_recall / z3)
        print()
        row = [shinglesz, minhash_num_perm, minhash_threshold, number_of_results, av_shingle_delay / z, av_lsh_band_delay / z2, av_sig_delay / z1, av_wd_delay / z4,int(av_shingle_size / z),
               int(av_sig_size / z1),av_lsh_band_recall / z3, av_sig_recall / z3 ,av_wd_recall / z3]
        print(row)
    except:
        print("issue writing global stats")


# Wikipedia uses CirrusSearch, a MediaWiki extension that uses Elasticsearch to provide enhanced search features
def get_wikipedia_search_results(query, limit):
    wiki_results = np.zeros(shape=[len(query),2], dtype=object)
    i = 0
    for q in query:
        wiki_results[i] =q.title(), wikipedia.search(q.title(), results=limit)[1:]
        i+=1
    return wiki_results


def intersection(list_a, list_b):
    return [ e for e in list_a if e in list_b ]


def main():
    train, query, wd = setup()
    lsh, train_ds = train_func(train, wd)


    f1 = open('globalstats_NL.csv', 'a')
    writer1 = csv.writer(f1)
    f2 = open('alldata_NL.csv', 'a')
    writer2 = csv.writer(f2)
    # header0 = ["Parameters", "Delay Real","Delay LSH bands","Delay signature LSH","Delay wiki search","Size shingls",
    #        "Size jaccard", "Recall LSH", "Recall signature", "Recall wiki search"]
    # header = ["Shingle size", "Signature length","Minhash threshold", "TopN results", "Delay Real","Delay LSH bands","Delay signature LSH","Delay wiki search","Size shingls",
    #        "Size jaccard", "Recall LSH", "Recall signature", "Recall wiki search"]
    # header2 = ["Shingle size", "Signature length","Minhash threshold", "TopN results", "Title", "Delay Real","Delay LSH bands","Delay signature LSH","Delay wiki search",
    #            "Recall LSH", "Recall signature", "Recall wiki search"]
    # writer1.writerow(header)
    # writer2.writerow(header2)
    #
    query_func(query,wd,lsh,train_ds, writer1, writer2, shingle_size)

    global number_of_results
    number_of_results = 5

    query_func(query,wd,lsh,train_ds, writer1, writer2, shingle_size)



    f1.close()
    f2.close()




if __name__ == '__main__':
    main()




# todo get results by varying: bands parameters, topN, number of datapoints in the network, LSH parameters,
#  shingle size, signature length, vary top N for real and sig differently
#  vary dataset size also (?)


