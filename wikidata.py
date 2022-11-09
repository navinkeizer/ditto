# Ditto July 2022
# developed by Navin v. Keizer
import time

import libzim
from bs4 import BeautifulSoup
# from libzim.reader import Archive
# from alive_progress import alive_bar
import progressbar


class wikidata:

    # CONFIG
    # zimLocation = "wikitest.zim"
    # zimLocation = "wiki_nl_all.zim"
    def __init__(self, zimLocation):
        self.zim = libzim.reader.Archive(zimLocation)

    # todo add progressbar
    def get_titles_large(self):
        print("Getting titles...")
        titles = []
        j = 0
        f = 0
        e=0
        for i in range(0, self.zim.entry_count, 1):
            try:
                entry = self.zim._get_entry_by_id(i)
                if "A/" in str(entry):
                    j+=1
                    entry = str(entry).split("url=")[1]
                    entry = entry.split(",")[0]
                    titles.append(entry)
                else:
                    f +=1

            except:
                print("Issue retrieving title")
                e +=1
        print("--------------------------------------------")
        print(str(self.zim.entry_count), "entries found in NL index...")

        print(str(j), ": Articles found in NL...")
        print(str(f), ": No content pages...")
        print(str(e), ": Failed to fetch article name...")
        print("--------------------------------------------")

        return titles


    def get_titles(self):

        # get the main entry
        entry = self.zim.main_entry.get_item()

        contentbylines = bytes(entry.content).decode("UTF-8").splitlines()
        # print(f"Entry {entry.title} at {entry.path} is {entry.size}b.")
        # print(contentbylines)

        # get the titles of the pages
        titles = []
        k = 0
        l = 0
        for i in range(1, len(contentbylines)):
            if k == 0:
                if contentbylines[i] == '</head>':
                    k = 1
            else:
                if l == 0:
                    l = 1
                    continue
                elif l == 1:
                    title = contentbylines[i].split('href="')
                    title = title[1].split('"')
                    titles.append(title[0])
                    l = 2
                else:
                    if contentbylines[i] == '</a></ul></body></html>':
                        k = 0
                        break
                    elif contentbylines[i] != '':
                        title = contentbylines[i].split('"')
                        # print(title[1])
                        titles.append(title[1])
        return titles

    def get_content(self, title):
            entry = self.zim.get_entry_by_path("A/" + title).get_item()
            return entry

    def get_content_txt(self, title):
        # may need to add some language processing
        # eg remove the \n in the text
        html = self.get_content_html(title)
        soup = BeautifulSoup(html, "html.parser")
        html_text = soup.get_text()
        return html_text

    def get_content_html(self, title):
        entry = self.zim.get_entry_by_path(title).get_item()
        return bytes(entry.content).decode("UTF-8")

    def main(self):
        titles = self.get_titles_large()
        success = 0
        for title in titles:
            try:
                e = self.zim.get_entry_by_path(title).get_item()
                ff = self.get_content_txt(title)
                print(e)
                print(ff)
                success = success + 1
            except:
                print("Error retrieving content", title, "...")
                continue
        print(str(success)+" articles found out of "+str(len(titles)) + " items.")


if __name__ == '__main__':
    # wd = wikidata("wiki_nl_all.zim")
    # wd.testfunctie()
    # wd.main()
    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        ' (', progressbar.ETA(), ') ',
    ]
    with progressbar.ProgressBar(max_value=10,  widgets=widgets) as bar:
        for i in range(10):
            time.sleep(0.1)
            bar.update(i)





# todo: compare minpage/maxpage/nopic wikipedia entries performance in LSH, limit to NL entries
# todo: implement different LSH libraries
# todo: implement (different) DHT mapping
# todo: convert to Peersim for (unstructured) simulation
# todo: get distribution from ipfs provider records (?)

# todo: how are buckets mapped to dht, when there is no orchestrator or authoroty