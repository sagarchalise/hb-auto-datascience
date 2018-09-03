import os
import csv
import time
import difflib
import itertools
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup, NavigableString

hamrobazaar = "http://hamrobazaar.com/"
hamrobazaar_search = "{}m/search.php".format(hamrobazaar)
auto_catgory = 62
mobile_category = 31
# base_url = "{}?&do_search=1&city_search=&order=siteid&e_2=2&catid_search=62&offset=3040".format(hamrobazaar)
search_url = "{}?do_search=Search&catid_search={}&e_2=2&&order=siteid&way=0&do_search=Search".format(
    hamrobazaar_search, auto_catgory
)

HB_FIELDS = [
    "Brand",
    "Name",
    "Anchal",
    "Type",
    "Condition",
    "Used For",
    "Lot No",
    "Price",
    "Mileage",
    "Engine",
    "Make Year",
    "Kilometers",
]

AUTO_MAPS = {
    "hero": ["splendor", "xtreme", "cbz", "karizma", "glamour", "pleasure"],
    "honda": [
        "hornet",
        "unicorn",
        "shine",
        "dio",
        "cbr",
        "crf",
        "aviator",
        "activa",
        "stunner",
        "xr",
    ],
    "bajaj": ["pulsar", "discover", "ns", "avenger", "platina"],
    "yamaha": ["fz", "r15", "ray", "fascino"],
    "tvs": ["apache", "pep", "jupiter"],
    "enfield": ["classic"],
    "hartford": ["vr"],
    "ktm": ["duke"],
    "suzuki": ["gixxer", "gn", "access"],
    "um": ["renegade"],
    "mahindra": ["centuro", "rodeo", "duro", "gusto", "flyte"],
    "crossfire": [],
    "benelli": ["tnt"],
    "electric": ["electric"],
    "apollo": [],
    "reiju": [],
    "vespa": [],
    "aprilla": [],
    "ducati": [],
}


def write_to_csv(data):
    filename = "hbcsv.csv"
    write_header = not os.path.exists(filename)
    if not data:
        return
    with open("hbcsv.csv", "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HB_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(data)


def convert_to_int(val):
    try:
        return int(val)
    except ValueError:
        if val.startswith("Rs."):
            val = val[3:].replace(",", "").strip()
            if val:
                new_val = convert_to_int(val)
                if new_val == val:
                    return val
                if new_val//1000:
                    return new_val
                return new_val*1000
        return val
    except TypeError:
        return val


def get_fuzzy_score(vals, checks, cutoff=0.8):
    for v in vals:
        matcher = difflib.get_close_matches(v, checks, cutoff=cutoff)
        if matcher:
            return v


def get_name_and_brand(t):
    k = {"Name": "UnKnown", "Brand": "UnKnown"}
    t = t.strip().lower()
    tokens = t.split()
    for brand, options in AUTO_MAPS.items():
        if brand in t:
            k["Brand"] = brand
            name = get_fuzzy_score(options, tokens)
            if name:
                k["Name"] = name
            break
        matcher = get_fuzzy_score([brand], tokens)
        if matcher:
            k["Brand"] = brand
            name = get_fuzzy_score(options, tokens)
            if name:
                k["Name"] = name
            break
        matcher = get_fuzzy_score(options, tokens)
        if matcher:
            k["Brand"] = brand
            k["Name"] = matcher
            break
    else:  # nobreak
        print("Couldnot determine name", t)
    return k


def request_and_get_soup(url):
    response = requests.get(url)
    if not response.ok:
        return
    print(url)
    return BeautifulSoup(response.text, "lxml")


def scrape_from_page(url):
    if not url.startswith("http"):
        url = "{}{}".format(hamrobazaar, url)
    soup = request_and_get_soup(url)
    if not soup:
        return {}
    name = soup.find("span", {"class": "title"})
    data = get_name_and_brand((name and name.string or "").strip())
    parent_td = soup.find("td", {"valign": "top", "align": "left"})
    if not parent_td:
        return data
    all_tds = parent_td.find_all("td", {"id": "white"})
    concerned = HB_FIELDS[2:]
    INT_FIELDS = HB_FIELDS[6:]
    for ind, td in enumerate(all_tds):
        if not td.string:
            continue
        key = td.string.strip().lower()
        if key == "price negotiable:":
            continue
        for check in concerned:
            if key.startswith(check.lower()) and key.endswith(":"):
                key = check
                break
        else:
            continue
        val = all_tds[ind + 1].string
        if key in INT_FIELDS:
            val = convert_to_int(val)
        data[key] = val
    return data


def get_per_bike_urls_list(url=None, offset_start=None, stopper=0):
    if url.startswith("?"):
        url = "{}{}".format(hamrobazaar_search, url)
    if offset_start:
        url = url.replace("do_search=Search", "do_search=1")
        url = url + "&offset={}".format(offset_start)
    if stopper > 24:
        return
    soup = request_and_get_soup(url)
    if not soup:
        return

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
    page_urls = []
    for tag in all_tags:
        if tag.string == "Next":
            next_url = tag.find_parent("a").get("href")
        else:
            parent_tr = tag.find_parent("tr")
            all_tds = parent_tr.find_all("td")
            if not all_tds:
                continue
            bike_page = all_tds[2].find("a")
            if bike_page:
                page_urls.append(bike_page.get("href"))
    stopper = stopper + 1
    if next_url:
        time.sleep(0.5)
        d = get_per_bike_urls_list(url=next_url, stopper=stopper)
        if d:
            page_urls.extend(d)
    return page_urls


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=7) as executor:
        mapped_urls = executor.map(
            get_per_bike_urls_list,
            itertools.repeat(search_url),
            range(0, 4000, 500),
            itertools.repeat(0),
        )
    mapped = []
    for urls in mapped_urls:
        with ThreadPoolExecutor(max_workers=7) as executor:
            mapped.append(executor.map(scrape_from_page, urls))
    write_to_csv(itertools.chain.from_iterable(mapped))
