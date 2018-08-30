import os
import csv
import time
import difflib
import itertools
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup, NavigableString

hamrobazaar = "http://hamrobazaar.com/m/search.php"
auto_catgory = 62
mobile_category = 31
# base_url = "{}?&do_search=1&city_search=&order=siteid&e_2=2&catid_search=62&offset=3040".format(hamrobazaar)
base_url = "{}?do_search=Search&catid_search={}&e_2=2&&order=siteid&way=0&do_search=Search".format(
    hamrobazaar, auto_catgory
)

AUTO_MAPS = {
    "bike": {
        "honda": [],
        "hero": [],
        "yamaha": [],
        "ktm": [],
        "benelli": [],
        "royal enfield": [],
        "electric": [],
        "suzuki": [],
    },
    "scooter": {
        "hero": [],
        "yamaha": [],
        "suzuki": [],
        "bently": [],
        "honda": [],
        "electric": [],
        "aprilla": [],
    },
}


def write_to_csv(data):
    filename = "hbcsv.csv"
    write_header = not os.path.exists(filename)
    if not data:
        return
    with open("hbcsv.csv", "a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Brand",
                "Name",
                "Cost",
                "Type",
                "Condition",
                "Anchal",
                "Lot No",
                "Make Year",
                "Kilometers",
            ],
            extrasaction="ignore",
        )
        if write_header:
            writer.writeheader()
        writer.writerows(data)


def get_required_info(concerned_td):
    data = {}

    def get_anchal(t):
        k = {}
        if t.startswith("Anchal"):
            val = t.split("|")
            for v in val:
                d = v.split(":")
                if len(d) == 2:
                    k[d[0].strip()] = d[1].strip()
        return k

    def get_condition_and_cost(t):
        k = {}
        if t.startswith("(") and t.endswith(")"):
            k["Condition"] = t.replace("(", "").replace(")", "").strip()
            if t.startswith("Rs."):
                try:
                    k["Cost"] = int(t[4:].replace(",", "").strip())
                except:
                    k["Cost"] = -1
        return k

    def get_name_and_type(t):
        k = {"Name": "UnKnown", "Type": "UnKnown", "Brand": "UnKnown"}
        tokens = t.split()
        for bt, opts in AUTO_MAPS.items():
            for brand, options in opts.items():
                for opt in options:
                    matcher = difflib.get_close_matches(opt, tokens)
                    if matcher:
                        k["Name"] = opt
                        k["Brand"] = brand
                        k["Type"] = bt
                        break
        return k

    concerned_td = concerned_td or []
    for td in concerned_td:
        for val in td.descendants:
            if isinstance(val, NavigableString):
                data.update(get_anchal(val))
                data.update(get_cost_and_condition(val))
                data.update(get_name_and_type(val))
    return data


def scrape_from_hb(url=None, offset_start=None, stopper=0):
    if url.startswith("?"):
        url = "{}{}".format(hamrobazaar, url)
    if offset_start:
        url = url.replace("do_search=Search", "do_search=1")
        url = url + "&offset={}".format(offset_start)
    if stopper > 24:
        return
    response = requests.get(url)
    if not response.ok:
        return
    print(url)
    soup = BeautifulSoup(response.text, "lxml")

    def concerned_tag(tag):
        spec_tag = tag.name == "font" and tag.attrs.get("color") == "#565d60"
        next_tag = tag.name == "u"
        if spec_tag or next_tag:
            is_concerned_tag = tag.string == "Next"
            if is_concerned_tag:
                return True
            for t in tag.descendants:
                is_concerned_tag = isinstance(t, NavigableString) and t.startswith("Anchal")
                if is_concerned_tag:
                    break
            return is_concerned_tag
        return False

    all_tags = soup.find_all(concerned_tag)
    next_url = None
    datas = []
    for tag in all_tags:
        if tag.string == "Next":
            next_url = tag.find_parent("a").get("href")
        else:
            parent_tr = tag.find_parent("tr")
            all_tds = parent_tr.find_all("td")
            if not all_tds:
                continue
            data = get_required_info((all_tds[1], all_tds[2], all_tds[-2]))
            if data:
                datas.append(k)
    stopper = stopper + 1
    if next_url:
        time.sleep(0.5)
        d = scrape_from_hb(url=next_url, stopper=stopper)
        if d:
            datas.extend(d)
    return data


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=5) as executor:
        mapped = executor.map(
            scrape_from_hb, itertools.repeat(base_url), range(0, 4000, 500), itertools.repeat(0)
        )
    for m in mapped:
        write_to_csv(m)
