"""
Master list of companies to track.
CIK is the SEC Central Index Key — zero-padded to 10 digits.
Find any company's CIK at: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=NAME&type=10-K
"""

COMPANIES = [
    # ── Mega Cap Tech ──────────────────────────────────────────────────────────
    {"ticker": "AAPL",  "name": "Apple",             "cik": "0000320193",  "sector": "Mega Cap", "ipo_year": 1980},
    {"ticker": "MSFT",  "name": "Microsoft",          "cik": "0000789019",  "sector": "Mega Cap", "ipo_year": 1986},
    {"ticker": "GOOGL", "name": "Alphabet",           "cik": "0001652044",  "sector": "Mega Cap", "ipo_year": 2004},
    {"ticker": "META",  "name": "Meta Platforms",     "cik": "0001326801",  "sector": "Mega Cap", "ipo_year": 2012},
    {"ticker": "AMZN",  "name": "Amazon",             "cik": "0001018724",  "sector": "Mega Cap", "ipo_year": 1997},
    {"ticker": "NFLX",  "name": "Netflix",            "cik": "0001065280",  "sector": "Mega Cap", "ipo_year": 2002},
    {"ticker": "TSLA",  "name": "Tesla",              "cik": "0001318605",  "sector": "Mega Cap", "ipo_year": 2010},

    # ── Enterprise SaaS (Clouded Judgement / Meritech universe) ───────────────
    {"ticker": "CRM",   "name": "Salesforce",         "cik": "0001108524",  "sector": "SaaS", "ipo_year": 2004},
    {"ticker": "WDAY",  "name": "Workday",            "cik": "0001327811",  "sector": "SaaS", "ipo_year": 2012},
    {"ticker": "NOW",   "name": "ServiceNow",         "cik": "0001373715",  "sector": "SaaS", "ipo_year": 2012},
    {"ticker": "SNOW",  "name": "Snowflake",          "cik": "0001640147",  "sector": "SaaS", "ipo_year": 2020},
    {"ticker": "DDOG",  "name": "Datadog",            "cik": "0001561550",  "sector": "SaaS", "ipo_year": 2019},
    {"ticker": "MDB",   "name": "MongoDB",            "cik": "0001441816",  "sector": "SaaS", "ipo_year": 2017},
    {"ticker": "CRWD",  "name": "CrowdStrike",        "cik": "0001535527",  "sector": "SaaS", "ipo_year": 2019},
    {"ticker": "ZS",    "name": "Zscaler",            "cik": "0001713683",  "sector": "SaaS", "ipo_year": 2018},
    {"ticker": "HUBS",  "name": "HubSpot",            "cik": "0001404655",  "sector": "SaaS", "ipo_year": 2014},
    {"ticker": "OKTA",  "name": "Okta",               "cik": "0001660134",  "sector": "SaaS", "ipo_year": 2017},
    {"ticker": "VEEV",  "name": "Veeva Systems",      "cik": "0001372514",  "sector": "SaaS", "ipo_year": 2013},
    {"ticker": "CFLT",  "name": "Confluent",          "cik": "0001764046",  "sector": "SaaS", "ipo_year": 2021},
    {"ticker": "GTLB",  "name": "GitLab",             "cik": "0001653558",  "sector": "SaaS", "ipo_year": 2021},
    {"ticker": "ESTC",  "name": "Elastic",            "cik": "0001707753",  "sector": "SaaS", "ipo_year": 2018},
    {"ticker": "DOCN",  "name": "DigitalOcean",       "cik": "0001582961",  "sector": "SaaS", "ipo_year": 2021},
    {"ticker": "TWLO",  "name": "Twilio",             "cik": "0001447362",  "sector": "SaaS", "ipo_year": 2016},
    {"ticker": "ZM",    "name": "Zoom",               "cik": "0001585521",  "sector": "SaaS", "ipo_year": 2019},
    {"ticker": "TEAM",  "name": "Atlassian",          "cik": "0001650372",  "sector": "SaaS", "ipo_year": 2015},
    {"ticker": "COUP",  "name": "Coupa Software",     "cik": "0001385867",  "sector": "SaaS", "ipo_year": 2016},

    # ── Security / Infrastructure ──────────────────────────────────────────────
    {"ticker": "PANW",  "name": "Palo Alto Networks", "cik": "0001327567",  "sector": "Security", "ipo_year": 2012},
    {"ticker": "FTNT",  "name": "Fortinet",           "cik": "0001262039",  "sector": "Security", "ipo_year": 2009},
    {"ticker": "NET",   "name": "Cloudflare",         "cik": "0001477333",  "sector": "Infrastructure", "ipo_year": 2019},
    {"ticker": "FSLY",  "name": "Fastly",             "cik": "0001517413",  "sector": "Infrastructure", "ipo_year": 2019},

    # ── AI / Neo-Cloud ─────────────────────────────────────────────────────────
    {"ticker": "PLTR",  "name": "Palantir",           "cik": "0001321655",  "sector": "AI / Neo-Cloud", "ipo_year": 2020},
    {"ticker": "AI",    "name": "C3.ai",              "cik": "0001577552",  "sector": "AI / Neo-Cloud", "ipo_year": 2020},
    {"ticker": "SOUN",  "name": "SoundHound AI",      "cik": "0001840292",  "sector": "AI / Neo-Cloud", "ipo_year": 2022},
    {"ticker": "CRWV",  "name": "CoreWeave",          "cik": "0001769628",  "sector": "AI / Neo-Cloud", "ipo_year": 2025},
    {"ticker": "BBAI",  "name": "BigBear.ai",         "cik": "0001836935",  "sector": "AI / Neo-Cloud", "ipo_year": 2021},

    # ── Notable SBC cases (for editorial context) ──────────────────────────────
    {"ticker": "SNAP",  "name": "Snap",               "cik": "0001564408",  "sector": "Social / Consumer", "ipo_year": 2017},
    {"ticker": "PINS",  "name": "Pinterest",          "cik": "0001506439",  "sector": "Social / Consumer", "ipo_year": 2019},
    {"ticker": "LYFT",  "name": "Lyft",               "cik": "0001759509",  "sector": "Social / Consumer", "ipo_year": 2019},
    {"ticker": "UBER",  "name": "Uber",               "cik": "0001543151",  "sector": "Social / Consumer", "ipo_year": 2019},
    {"ticker": "ABNB",  "name": "Airbnb",             "cik": "0001559720",  "sector": "Social / Consumer", "ipo_year": 2020},
    {"ticker": "DASH",  "name": "DoorDash",           "cik": "0001792789",  "sector": "Social / Consumer", "ipo_year": 2020},
]
