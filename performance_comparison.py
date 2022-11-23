
# todo: 1. implement the jaccard and accuracy comparisons from earlier runs on larger
#  dataset containing 1) ipfs crawled data and 2) wikipedia data

# todo: 2. implement modified kademlia with minhash in id space
import operator
import wikidata
import numpy as np
import progressbar
import kshingle
from datasketch import MinHash
import re, csv, time, os
import pandas as pd
import ipfshttpclient
from scipy.spatial import distance
import subprocess
from threading import Timer

widgets = [
    ' [', progressbar.Timer(), '] ',
    progressbar.Bar(),
    ' (', progressbar.ETA(), ') ',
]

# ----------------------------------
# helper functions


def save_ipfs_cids():

    # load cid crawl results
    df1 = pd.read_csv('cm.csv')
    df2 = pd.read_csv('cm2.csv')
    df3 = pd.read_csv('cm3.csv')
    df4 = pd.read_csv('cm4.csv')
    dfs = [df1, df2, df3, df4]

    cidlist = []
    i = 0
    file = open('cidtxt.csv', 'a')
    writer1 = csv.writer(file)

    for df in dfs:
        d = (df.loc[df['ext'] == ".txt"])
        for cid in (d['cid']):
            i += 1
            cidlist.append(cid)
            print(type(cid))
            writer1.writerow([cid])

    print(i, " '.txt' cids found")
    file.close()


def generate_sig(t, wd,  ks, num_perm):
    try:
        t1 = time.time()
        txt_file = wd.get_content_txt(t)
        shingles = kshingle.shingleset_k(txt_file, k=ks)
        m = MinHash(num_perm=num_perm)
        for d in shingles:
            m.update(d.encode('utf8'))
        t2 = time.time()
        delay = t2-t1
    except:
        print("Issue getting", t, "...")
        m=''
        shingles = ''
        delay = ''
    return m, shingles, delay


def get_change(current, previous):
    if current == previous:
        return 100.0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return 0


# ----------------------------------
# main functions


def get_wiki_signatures(number_of_results, w1):
    wd = wikidata.wikidata("wiki_nl_all.zim")
    titles = wd.get_titles_large()
    np.random.shuffle(titles)
    if number_of_results:
        titles = titles[:number_of_results]

    signatures = []
    av_sig_delay = 0.
    success = 0

    o = 0
    with progressbar.ProgressBar(max_value=len(titles),  widgets=widgets) as bar:
        for title in titles:
            bar.update(o)
            o +=1
            try:
                txt_file = wd.get_content_txt(title)
                # process text to exclude special characters
                s = txt_file.replace("\n", " ")
                s = re.sub(' +', ' ', s)
                t0 = time.time()
                shingles = kshingle.shingleset_k(s, k=4)
                m = MinHash(num_perm=128)
                for d in shingles:
                    m.update(d.encode('utf8'))
                t1 = time.time()
                av_sig_delay = av_sig_delay + t1 - t0
                signatures.append(m)
                success = success + 1
                row = ["wikidata",len(titles), t1 - t0]
                w1.writerow(row)
            except:
                print("issue in retrieving '" + title + "' , continuing...")
                continue

    return signatures, av_sig_delay / success


def get_ipfs_signatures(number):
    # todo: add signature latency
    with open('cidtxt.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\n')
        k = 0

        for row in csv_reader:
            if k>number:
                break

            print("Retrieving " + row[0], "...")
            try:
                kill = lambda process: process.kill()
                cmd = ['ipfs','cat', row[0]]
                retrieve = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                my_timer = Timer(15, kill, [retrieve])
                # try:
                    # my_timer.start()

                stdout, stderr = retrieve.communicate()
                # finally:
                    # my_timer.cancel()
                output = str(stderr.decode())

                if "Error" in output or output == '':
                    print("Not Found")
                    print(output)
                else:
                    print("Success")
                    print(output)

                k += 1
            except:
                print("Error retrieving " + row[0], "...")
                continue


def compare_accuracy(number,w, kshingle, num_perm):
    wd = wikidata.wikidata("wiki_nl_all.zim")
    titles = wd.get_titles_large()
    np.random.shuffle(titles)
    subset = titles[:number]
    np.random.shuffle(titles)
    subset2 = titles[:number]
    av_dif=0.
    n=0

    oo = 0
    with progressbar.ProgressBar(max_value=int(number*number), widgets=widgets) as bar:
        for s in subset:
            for a in subset2:
                bar.update(oo)
                oo += 1
                if s != a:
                    m1, s1, d1 = generate_sig(s, wd, kshingle, num_perm)
                    m2, s2, d2 = generate_sig(a, wd, kshingle, num_perm)
                    if m1 != '' and m2 != '':
                        estimate = m1.jaccard(m2)
                        # xor = (m1.digest()) ^ (m2.digest())
                        real = float(len(s1.intersection(s2)))/float(len(s1.union(s2)))
                        dif = get_change(real,estimate)
                        # difxor = get_change(real,xor)
                        av_dif = av_dif + dif
                        delay = (d1+d2)/2
                        n=n+1
                        # row = [number, kshingle, num_perm, dif, delay, difxor]
                        row = [number, kshingle, num_perm, dif, delay]
                        w.writerow(row)

    return av_dif/n


def compare_metrics(signatures, w1 ,w2, dataset):
    time_jaccard = 0.
    time_xor = 0.
    time_cosine = 0.
    time_eu = 0.
    time_hamming = 0.
    div = 0

    np.random.shuffle(signatures)
    for i in range(0, len(signatures) - 2):
        try:
            s = signatures[i]
            s2 = signatures[i + 1]

            t0 = time.time()
            jaccard = s.jaccard(s2)
            t1 = time.time()

            t2 = time.time()
            xor = (s.digest()) ^ (s2.digest())
            t3 = time.time()

            t4=time.time()
            cosine = distance.cosine(s.digest(), s2.digest())
            t5 = time.time()

            hamming = distance.hamming(s.digest(), s2.digest())
            t6 = time.time()

            eu = distance.euclidean(s.digest(), s2.digest())
            t7 = time.time()

            time_xor = time_xor + t3 - t2
            time_jaccard = time_jaccard + t1 - t0
            time_cosine = time_cosine + t5-t4
            time_eu = time_eu + t7-t6
            time_hamming = time_hamming + t6-t5
            row = [dataset,len(signatures), t1 - t0, t3 - t2, t5-t4, t6-t5, t7-t6]
            w1.writerow(row)
            div += 1

        except:
            print("Issue in comparing signatures...")
            continue
    row = [dataset,len(signatures),time_jaccard / div,time_xor / div, time_cosine / div, time_hamming / div, time_eu / div]
    w2.writerow(row)


def main():

    # content_number = 100000
    # print("Getting wikipedia signatures...")
    #
    # f0 = open('delay_signatures.csv', 'a')
    # writer0 = csv.writer(f0)
    # s, delay = get_wiki_signatures(content_number, writer0)
    # print("Average delay signatures", delay, "seconds")
    # f0.close()
    #
    # print()
    # print("Comparing signature distance metrics wikidata...")
    #
    # f1 = open('compare_metrics_all.csv', 'a')
    # writer1 = csv.writer(f1)
    # f2 = open('compare_metrics_global.csv', 'a')
    # writer2 = csv.writer(f2)
    # compare_metrics(s, writer1, writer2, "wikidata")
    # f1.close()
    # f2.close()


    # write to the same files as wiki
    # print()
    # print("Getting ipfs signatures...")
    # print()
    # print("Comparing signature distance metrics ipfs...")

    # todo run with nump = 4, 8, 256

    nmr = 50
    
    k = 6
    nump = [4,8,16,32,64,128,256]
    for np in nump:
        print()
        print("Checking jaccard accuracy vs raw data...")
        print(" Kshingle", str(k), "\n", "num_perm", str(np), "\n", "number of runs", str(nmr))
        f3 = open('accuracy_comparison3.csv', 'a')
        writer3 = csv.writer(f3)
        av_dif = compare_accuracy(nmr, writer3, k, np)

        print("Average difference signature vs raw (for wikidata): ", av_dif)
        f3.close()
        
        
    np = 128
    kss =[2, 4, 6, 8, 10, 12]
    for k in kss:
        print()
        print("Checking jaccard accuracy vs raw data...")
        print(" Kshingle", str(k), "\n", "num_perm", str(np), "\n", "number of runs", str(nmr))
        f3 = open('accuracy_comparison3.csv', 'a')
        writer3 = csv.writer(f3)
        av_dif = compare_accuracy(nmr, writer3, k, np)

        print("Average difference signature vs raw (for wikidata): ", av_dif)
        f3.close()
    
        



    # k = 4
    # np = 16
    #
    # print()
    # print("Checking jaccard accuracy vs raw data...")
    # print(" Kshingle", str(k), "\n", "num_perm", str(np), "\n", "number of runs", str(nmr))
    # f3 = open('accuracy_comparison2.csv', 'a')
    # writer3 = csv.writer(f3)
    # av_dif = compare_accuracy(nmr, writer3, k, np)
    #
    # print("Average difference signature vs raw (for wikidata): ", av_dif)
    # f3.close()


    # 
    # k=4
    # np = 4
    # 
    # print()
    # print("Checking jaccard accuracy vs raw data...")
    # print(" Kshingle", str(k), "\n", "num_perm", str(np),"\n","number of runs", str(nmr))
    # f3 = open('accuracy_comparison2.csv', 'a')
    # writer3 = csv.writer(f3)
    # av_dif = compare_accuracy(nmr, writer3, k, np)
    # 
    # print("Average difference signature vs raw (for wikidata): ",av_dif)
    # f3.close()


    # k=4
    # np = 8
    #
    # print()
    # print("Checking jaccard accuracy vs raw data...")
    # print(" Kshingle", str(k), "\n", "num_perm", str(np),"\n","number of runs", str(nmr))
    # f3 = open('accuracy_comparison2.csv', 'a')
    # writer3 = csv.writer(f3)
    # av_dif = compare_accuracy(nmr, writer3, k, np)
    #
    # print("Average difference signature vs raw (for wikidata): ",av_dif)
    # f3.close()
    #
    # k = 4
    # np = 256
    #
    # print()
    # print("Checking jaccard accuracy vs raw data...")
    # print(" Kshingle", str(k), "\n", "num_perm", str(np), "\n", "number of runs", str(nmr))
    # f3 = open('accuracy_comparison2.csv', 'a')
    # writer3 = csv.writer(f3)
    # av_dif = compare_accuracy(nmr, writer3, k, np)
    #
    # print("Average difference signature vs raw (for wikidata): ", av_dif)
    # f3.close()




if __name__ == '__main__':
    main()
    # get_ipfs_signatures(1000)
