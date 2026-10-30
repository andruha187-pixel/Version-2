"""
Часовые пояса городов: «день» температурного рынка — местные сутки города
(станция считает сутки по местному времени), поэтому время входа считаем от
местной полуночи даты рынка.

Город ищем по названию из заголовка события («Highest temperature in London on
…»); не нашли — по стране из ссылки Weather Underground в правилах
(…/history/daily/gb/london/EGLC). С конца августа 2026 у многих городов в
правилах вместо ссылки — «recorded by NOAA at … Airport Station», страны там нет,
поэтому города нужны здесь по названию. Не нашли — UTC с пометкой «пояс?» в
🏙 Городах: такой город стоит дописать сюда.
"""
from __future__ import annotations
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CITY_TZ = {
    # США и Канада
    "nyc": "America/New_York", "new york": "America/New_York", "new york city": "America/New_York",
    "boston": "America/New_York", "philadelphia": "America/New_York", "washington": "America/New_York",
    "washington dc": "America/New_York", "dc": "America/New_York", "miami": "America/New_York",
    "atlanta": "America/New_York", "orlando": "America/New_York", "tampa": "America/New_York",
    "charlotte": "America/New_York", "detroit": "America/Detroit", "toronto": "America/Toronto",
    "montreal": "America/Toronto", "ottawa": "America/Toronto",
    "chicago": "America/Chicago", "dallas": "America/Chicago", "houston": "America/Chicago",
    "austin": "America/Chicago", "san antonio": "America/Chicago", "minneapolis": "America/Chicago",
    "nashville": "America/Chicago", "new orleans": "America/Chicago", "kansas city": "America/Chicago",
    "st louis": "America/Chicago", "st. louis": "America/Chicago", "oklahoma city": "America/Chicago",
    "denver": "America/Denver", "salt lake city": "America/Denver", "calgary": "America/Edmonton",
    "phoenix": "America/Phoenix",
    "seattle": "America/Los_Angeles", "los angeles": "America/Los_Angeles", "la": "America/Los_Angeles",
    "san francisco": "America/Los_Angeles", "sf": "America/Los_Angeles", "san diego": "America/Los_Angeles",
    "las vegas": "America/Los_Angeles", "portland": "America/Los_Angeles", "sacramento": "America/Los_Angeles",
    "vancouver": "America/Vancouver", "honolulu": "Pacific/Honolulu", "anchorage": "America/Anchorage",
    # Латинская Америка
    "mexico city": "America/Mexico_City", "buenos aires": "America/Argentina/Buenos_Aires",
    "sao paulo": "America/Sao_Paulo", "são paulo": "America/Sao_Paulo", "rio de janeiro": "America/Sao_Paulo",
    "lima": "America/Lima", "bogota": "America/Bogota", "bogotá": "America/Bogota", "santiago": "America/Santiago",
    "panama city": "America/Panama", "panama": "America/Panama", "guatemala city": "America/Guatemala",
    "havana": "America/Havana", "santo domingo": "America/Santo_Domingo",
    "san juan": "America/Puerto_Rico", "caracas": "America/Caracas", "quito": "America/Guayaquil",
    "montevideo": "America/Montevideo", "asuncion": "America/Asuncion", "la paz": "America/La_Paz",
    "brasilia": "America/Sao_Paulo", "belo horizonte": "America/Sao_Paulo", "porto alegre": "America/Sao_Paulo",
    "curitiba": "America/Sao_Paulo", "salvador": "America/Bahia", "recife": "America/Recife",
    "fortaleza": "America/Fortaleza", "manaus": "America/Manaus", "monterrey": "America/Monterrey",
    "guadalajara": "America/Mexico_City", "cancun": "America/Cancun", "medellin": "America/Bogota",
    # Европа
    "london": "Europe/London", "manchester": "Europe/London", "dublin": "Europe/Dublin",
    "paris": "Europe/Paris", "berlin": "Europe/Berlin", "munich": "Europe/Berlin", "frankfurt": "Europe/Berlin",
    "hamburg": "Europe/Berlin", "madrid": "Europe/Madrid", "barcelona": "Europe/Madrid", "lisbon": "Europe/Lisbon",
    "rome": "Europe/Rome", "milan": "Europe/Rome", "amsterdam": "Europe/Amsterdam", "brussels": "Europe/Brussels",
    "vienna": "Europe/Vienna", "zurich": "Europe/Zurich", "geneva": "Europe/Zurich", "warsaw": "Europe/Warsaw",
    "prague": "Europe/Prague", "budapest": "Europe/Budapest", "stockholm": "Europe/Stockholm",
    "oslo": "Europe/Oslo", "copenhagen": "Europe/Copenhagen", "helsinki": "Europe/Helsinki",
    "athens": "Europe/Athens", "istanbul": "Europe/Istanbul", "ankara": "Europe/Istanbul",
    "moscow": "Europe/Moscow", "kyiv": "Europe/Kyiv", "kiev": "Europe/Kyiv", "bucharest": "Europe/Bucharest",
    "st petersburg": "Europe/Moscow", "saint petersburg": "Europe/Moscow", "minsk": "Europe/Minsk",
    "edinburgh": "Europe/London", "glasgow": "Europe/London",
    "lyon": "Europe/Paris", "marseille": "Europe/Paris", "nice": "Europe/Paris", "toulouse": "Europe/Paris",
    "seville": "Europe/Madrid", "valencia": "Europe/Madrid", "porto": "Europe/Lisbon", "naples": "Europe/Rome",
    "turin": "Europe/Rome", "venice": "Europe/Rome", "florence": "Europe/Rome", "cologne": "Europe/Berlin",
    "stuttgart": "Europe/Berlin", "dusseldorf": "Europe/Berlin", "rotterdam": "Europe/Amsterdam",
    "krakow": "Europe/Warsaw", "belgrade": "Europe/Belgrade", "sofia": "Europe/Sofia", "zagreb": "Europe/Zagreb",
    "riga": "Europe/Riga", "vilnius": "Europe/Vilnius", "tallinn": "Europe/Tallinn", "reykjavik": "Atlantic/Reykjavik",
    "izmir": "Europe/Istanbul", "antalya": "Europe/Istanbul",
    # Ближний Восток и Африка
    "dubai": "Asia/Dubai", "abu dhabi": "Asia/Dubai", "doha": "Asia/Qatar", "riyadh": "Asia/Riyadh",
    "tel aviv": "Asia/Jerusalem", "jerusalem": "Asia/Jerusalem", "cairo": "Africa/Cairo",
    "johannesburg": "Africa/Johannesburg", "cape town": "Africa/Johannesburg", "lagos": "Africa/Lagos",
    "nairobi": "Africa/Nairobi", "jeddah": "Asia/Riyadh", "mecca": "Asia/Riyadh", "medina": "Asia/Riyadh",
    "dammam": "Asia/Riyadh", "kuwait city": "Asia/Kuwait", "kuwait": "Asia/Kuwait", "manama": "Asia/Bahrain",
    "muscat": "Asia/Muscat", "amman": "Asia/Amman", "beirut": "Asia/Beirut", "baghdad": "Asia/Baghdad",
    "tehran": "Asia/Tehran", "casablanca": "Africa/Casablanca", "algiers": "Africa/Algiers", "tunis": "Africa/Tunis",
    "accra": "Africa/Accra", "addis ababa": "Africa/Addis_Ababa", "dar es salaam": "Africa/Dar_es_Salaam",
    "kinshasa": "Africa/Kinshasa", "luanda": "Africa/Luanda", "durban": "Africa/Johannesburg",
    # Азия и Океания
    "seoul": "Asia/Seoul", "busan": "Asia/Seoul", "tokyo": "Asia/Tokyo", "osaka": "Asia/Tokyo",
    "beijing": "Asia/Shanghai", "shanghai": "Asia/Shanghai", "shenzhen": "Asia/Shanghai",
    "guangzhou": "Asia/Shanghai", "chengdu": "Asia/Shanghai", "wuhan": "Asia/Shanghai",
    "chongqing": "Asia/Shanghai", "qingdao": "Asia/Shanghai", "zhengzhou": "Asia/Shanghai", "jinan": "Asia/Shanghai",
    "tianjin": "Asia/Shanghai", "nanjing": "Asia/Shanghai", "hangzhou": "Asia/Shanghai", "xi'an": "Asia/Shanghai",
    "xian": "Asia/Shanghai", "harbin": "Asia/Shanghai", "shenyang": "Asia/Shanghai", "dalian": "Asia/Shanghai",
    "xiamen": "Asia/Shanghai", "kunming": "Asia/Shanghai", "changsha": "Asia/Shanghai", "hefei": "Asia/Shanghai",
    "suzhou": "Asia/Shanghai", "fuzhou": "Asia/Shanghai", "nanning": "Asia/Shanghai", "urumqi": "Asia/Shanghai",
    "macau": "Asia/Macau", "incheon": "Asia/Seoul", "daegu": "Asia/Seoul", "sapporo": "Asia/Tokyo",
    "fukuoka": "Asia/Tokyo", "nagoya": "Asia/Tokyo", "kaohsiung": "Asia/Taipei", "ulaanbaatar": "Asia/Ulaanbaatar",
    "phnom penh": "Asia/Phnom_Penh", "yangon": "Asia/Yangon", "colombo": "Asia/Colombo",
    "kathmandu": "Asia/Kathmandu", "lahore": "Asia/Karachi", "islamabad": "Asia/Karachi",
    "hyderabad": "Asia/Kolkata", "ahmedabad": "Asia/Kolkata", "pune": "Asia/Kolkata", "jaipur": "Asia/Kolkata",
    "almaty": "Asia/Almaty", "tashkent": "Asia/Tashkent", "baku": "Asia/Baku", "tbilisi": "Asia/Tbilisi",
    "yerevan": "Asia/Yerevan", "cebu": "Asia/Manila", "surabaya": "Asia/Jakarta", "bali": "Asia/Makassar",
    "denpasar": "Asia/Makassar", "phuket": "Asia/Bangkok", "chiang mai": "Asia/Bangkok",
    "hong kong": "Asia/Hong_Kong", "taipei": "Asia/Taipei", "singapore": "Asia/Singapore",
    "jakarta": "Asia/Jakarta", "bangkok": "Asia/Bangkok", "manila": "Asia/Manila",
    "kuala lumpur": "Asia/Kuala_Lumpur", "hanoi": "Asia/Ho_Chi_Minh", "ho chi minh city": "Asia/Ho_Chi_Minh",
    "mumbai": "Asia/Kolkata", "delhi": "Asia/Kolkata", "new delhi": "Asia/Kolkata", "lucknow": "Asia/Kolkata",
    "bangalore": "Asia/Kolkata", "bengaluru": "Asia/Kolkata", "chennai": "Asia/Kolkata", "kolkata": "Asia/Kolkata",
    "karachi": "Asia/Karachi", "dhaka": "Asia/Dhaka",
    "sydney": "Australia/Sydney", "melbourne": "Australia/Melbourne", "brisbane": "Australia/Brisbane",
    "perth": "Australia/Perth", "adelaide": "Australia/Adelaide", "canberra": "Australia/Sydney",
    "hobart": "Australia/Hobart", "darwin": "Australia/Darwin", "gold coast": "Australia/Brisbane",
    "auckland": "Pacific/Auckland", "wellington": "Pacific/Auckland",
}

# Страна из ссылки Weather Underground (…/history/daily/<страна>/…) — если город неизвестен.
# Страны с несколькими поясами (us, ca, br, au, ru, mx, id) сюда не входят: там нужен город.
COUNTRY_TZ = {
    "gb": "Europe/London", "ie": "Europe/Dublin", "fr": "Europe/Paris", "de": "Europe/Berlin",
    "es": "Europe/Madrid", "pt": "Europe/Lisbon", "it": "Europe/Rome", "nl": "Europe/Amsterdam",
    "be": "Europe/Brussels", "at": "Europe/Vienna", "ch": "Europe/Zurich", "pl": "Europe/Warsaw",
    "cz": "Europe/Prague", "hu": "Europe/Budapest", "se": "Europe/Stockholm", "no": "Europe/Oslo",
    "dk": "Europe/Copenhagen", "fi": "Europe/Helsinki", "gr": "Europe/Athens", "tr": "Europe/Istanbul",
    "ua": "Europe/Kyiv", "ro": "Europe/Bucharest", "kr": "Asia/Seoul", "jp": "Asia/Tokyo",
    "cn": "Asia/Shanghai", "hk": "Asia/Hong_Kong", "tw": "Asia/Taipei", "sg": "Asia/Singapore",
    "th": "Asia/Bangkok", "ph": "Asia/Manila", "my": "Asia/Kuala_Lumpur", "vn": "Asia/Ho_Chi_Minh",
    "in": "Asia/Kolkata", "pk": "Asia/Karachi", "bd": "Asia/Dhaka", "ae": "Asia/Dubai", "qa": "Asia/Qatar",
    "sa": "Asia/Riyadh", "il": "Asia/Jerusalem", "eg": "Africa/Cairo", "za": "Africa/Johannesburg",
    "ng": "Africa/Lagos", "ke": "Africa/Nairobi", "nz": "Pacific/Auckland", "ar": "America/Argentina/Buenos_Aires",
    "pe": "America/Lima", "co": "America/Bogota", "cl": "America/Santiago",
    "pa": "America/Panama", "uy": "America/Montevideo", "py": "America/Asuncion", "bo": "America/La_Paz",
    "ec": "America/Guayaquil", "ve": "America/Caracas", "cr": "America/Costa_Rica", "gt": "America/Guatemala",
    "cu": "America/Havana", "do": "America/Santo_Domingo", "pr": "America/Puerto_Rico",
    "kw": "Asia/Kuwait", "bh": "Asia/Bahrain", "om": "Asia/Muscat", "jo": "Asia/Amman", "lb": "Asia/Beirut",
    "iq": "Asia/Baghdad", "ir": "Asia/Tehran", "ma": "Africa/Casablanca", "dz": "Africa/Algiers",
    "tn": "Africa/Tunis", "gh": "Africa/Accra", "et": "Africa/Addis_Ababa", "tz": "Africa/Dar_es_Salaam",
    "lk": "Asia/Colombo", "np": "Asia/Kathmandu", "mm": "Asia/Yangon", "kh": "Asia/Phnom_Penh",
    "mn": "Asia/Ulaanbaatar", "uz": "Asia/Tashkent", "az": "Asia/Baku", "ge": "Asia/Tbilisi", "am": "Asia/Yerevan",
    "by": "Europe/Minsk", "rs": "Europe/Belgrade", "bg": "Europe/Sofia", "hr": "Europe/Zagreb", "lv": "Europe/Riga",
    "lt": "Europe/Vilnius", "ee": "Europe/Tallinn", "is": "Atlantic/Reykjavik", "mo": "Asia/Macau",
}


ALIASES = {"new york city": "nyc", "new york": "nyc", "la": "los angeles", "sf": "san francisco",
           "washington dc": "washington", "washington d.c.": "washington", "dc": "washington",
           "são paulo": "sao paulo", "bogotá": "bogota", "kiev": "kyiv", "st. louis": "st louis",
           "bengaluru": "bangalore", "new delhi": "delhi",
           # одна и та же станция (RKSI) — с конца июля рынок называется «Seoul (Incheon)»
           "seoul incheon": "seoul", "seoul-incheon": "seoul"}


# Штат/провинция из ссылки Weather Underground — для городов, которых нет в CITY_TZ.
# В штатах с двумя поясами — пояс крупных городов.
STATE_TZ = {
    "us": {"al": "America/Chicago", "ak": "America/Anchorage", "az": "America/Phoenix", "ar": "America/Chicago",
           "ca": "America/Los_Angeles", "co": "America/Denver", "ct": "America/New_York", "de": "America/New_York",
           "dc": "America/New_York", "fl": "America/New_York", "ga": "America/New_York", "hi": "Pacific/Honolulu",
           "id": "America/Boise", "il": "America/Chicago", "in": "America/Indiana/Indianapolis",
           "ia": "America/Chicago", "ks": "America/Chicago", "ky": "America/New_York", "la": "America/Chicago",
           "me": "America/New_York", "md": "America/New_York", "ma": "America/New_York", "mi": "America/Detroit",
           "mn": "America/Chicago", "ms": "America/Chicago", "mo": "America/Chicago", "mt": "America/Denver",
           "ne": "America/Chicago", "nv": "America/Los_Angeles", "nh": "America/New_York", "nj": "America/New_York",
           "nm": "America/Denver", "ny": "America/New_York", "nc": "America/New_York", "nd": "America/Chicago",
           "oh": "America/New_York", "ok": "America/Chicago", "or": "America/Los_Angeles", "pa": "America/New_York",
           "ri": "America/New_York", "sc": "America/New_York", "sd": "America/Chicago", "tn": "America/Chicago",
           "tx": "America/Chicago", "ut": "America/Denver", "vt": "America/New_York", "va": "America/New_York",
           "wa": "America/Los_Angeles", "wv": "America/New_York", "wi": "America/Chicago", "wy": "America/Denver",
           "pr": "America/Puerto_Rico"},
    "ca": {"on": "America/Toronto", "qc": "America/Toronto", "bc": "America/Vancouver", "ab": "America/Edmonton",
           "mb": "America/Winnipeg", "sk": "America/Regina", "ns": "America/Halifax", "nb": "America/Moncton",
           "nl": "America/St_Johns", "pe": "America/Halifax"},
    "au": {"nsw": "Australia/Sydney", "act": "Australia/Sydney", "vic": "Australia/Melbourne",
           "qld": "Australia/Brisbane", "wa": "Australia/Perth", "sa": "Australia/Adelaide",
           "tas": "Australia/Hobart", "nt": "Australia/Darwin"},
    "br": {"sp": "America/Sao_Paulo", "rj": "America/Sao_Paulo", "df": "America/Sao_Paulo",
           "mg": "America/Sao_Paulo", "rs": "America/Sao_Paulo", "pr": "America/Sao_Paulo",
           "ba": "America/Bahia", "pe": "America/Recife", "ce": "America/Fortaleza", "am": "America/Manaus"},
}


# Станция города (ICAO аэропорта) — та же, по которой резолвится рынок (из resolutionSource событий).
# По ней берём METAR (📡 metar_feed). 4 города без неё (Hong Kong, Istanbul, Moscow, Tel Aviv): станция
# Polymarket неизвестна — METAR по ним не берём, чтобы не сверять с чужой станцией.
CITY_ICAO = {
    "amsterdam": "EHAM", "ankara": "LTAC", "atlanta": "KATL", "austin": "KAUS", "beijing": "ZBAA",
    "buenos aires": "SAEZ", "busan": "RKPK", "cape town": "FACT", "chengdu": "ZUUU", "chicago": "KORD",
    "chongqing": "ZUCK", "dallas": "KDAL", "denver": "KBKF", "guangzhou": "ZGGG", "helsinki": "EFHK",
    "houston": "KHOU", "jeddah": "OEJN", "jinan": "ZSJN", "karachi": "OPKC", "kuala lumpur": "WMKK",
    "london": "EGLC", "los angeles": "KLAX", "lucknow": "VILK", "madrid": "LEMD", "manila": "RPLL",
    "mexico city": "MMMX", "miami": "KMIA", "milan": "LIMC", "munich": "EDDM", "nyc": "KLGA",
    "panama city": "MPMG", "paris": "LFPB", "qingdao": "ZSQD", "san francisco": "KSFO", "sao paulo": "SBGR",
    "seattle": "KSEA", "seoul": "RKSI", "shanghai": "ZSPD", "shenzhen": "ZGSZ", "singapore": "WSSS",
    "taipei": "RCSS", "tokyo": "RJTT", "toronto": "CYYZ", "warsaw": "EPWA", "wellington": "NZWN",
    "wuhan": "ZHHH", "zhengzhou": "ZHCC",
}


def city_key(name: str) -> str:
    """Ключ города: «New York City» и «NYC» → «nyc» (одни и те же рынки под разными названиями)."""
    s = re.sub(r"[^\w\s.\-']", " ", (name or "").strip().lower())
    s = re.sub(r"\s+", " ", s).strip()
    return ALIASES.get(s, s)


# Координаты станции города (прибл.) — для прогноза Open-Meteo. Та же точка, что у ICAO выше.
CITY_LATLON = {
    "amsterdam": (52.31, 4.76), "ankara": (40.12, 32.99), "atlanta": (33.64, -84.43), "austin": (30.19, -97.67),
    "beijing": (40.08, 116.58), "buenos aires": (-34.82, -58.54), "busan": (35.17, 128.94),
    "cape town": (-33.97, 18.60), "chengdu": (30.58, 103.95), "chicago": (41.98, -87.90),
    "chongqing": (29.72, 106.64), "dallas": (32.85, -96.85), "denver": (39.70, -104.75),
    "guangzhou": (23.39, 113.30), "helsinki": (60.32, 24.96), "houston": (29.65, -95.28), "jeddah": (21.68, 39.16),
    "jinan": (36.86, 117.22), "karachi": (24.91, 67.16), "kuala lumpur": (2.75, 101.71), "london": (51.50, 0.05),
    "los angeles": (33.94, -118.41), "lucknow": (26.76, 80.89), "madrid": (40.47, -3.56), "manila": (14.51, 121.02),
    "mexico city": (19.44, -99.07), "miami": (25.79, -80.29), "milan": (45.63, 8.72), "munich": (48.35, 11.79),
    "nyc": (40.78, -73.87), "panama city": (8.97, -79.56), "paris": (48.97, 2.44), "qingdao": (36.27, 120.37),
    "san francisco": (37.62, -122.37), "sao paulo": (-23.43, -46.47), "seattle": (47.45, -122.31),
    "seoul": (37.46, 126.44), "shanghai": (31.14, 121.81), "shenzhen": (22.64, 113.81), "singapore": (1.36, 103.99),
    "taipei": (25.07, 121.55), "tokyo": (35.55, 139.78), "toronto": (43.68, -79.63), "warsaw": (52.17, 20.97),
    "wellington": (-41.33, 174.81), "wuhan": (30.78, 114.21), "zhengzhou": (34.52, 113.84),
}


def latlon_for(city: str) -> tuple[float, float] | None:
    """(широта, долгота) станции города — для прогноза; None — нет координат."""
    return CITY_LATLON.get(city_key(city))


def icao_for(city: str) -> str | None:
    """ICAO станции города (или None — станция неизвестна, METAR не берём)."""
    return CITY_ICAO.get(city_key(city))


def tz_for(city: str, country: str | None = None, state: str | None = None) -> tuple[str, bool]:
    """(IANA-пояс, известен ли он точно): по городу, потом по стране и штату из ссылки
    Weather Underground, потом по стране. Неизвестно — UTC, False."""
    key = city_key(city)
    if key in CITY_TZ:
        return CITY_TZ[key], True
    c = (country or "").lower()
    if state and state.lower() in STATE_TZ.get(c, {}):
        return STATE_TZ[c][state.lower()], True
    if c in COUNTRY_TZ:
        return COUNTRY_TZ[c], True
    return "UTC", False


def local_midnight_ts(date_iso: str, tz_name: str) -> int:
    """Unix-время местной полуночи даты «2026-10-24» в поясе города."""
    y, m, d = (int(x) for x in date_iso[:10].split("-"))
    try:
        tz = ZoneInfo(tz_name)
    except Exception:  # noqa: BLE001 — нет такого пояса (нет tzdata) — UTC
        tz = timezone.utc
    return int(datetime(y, m, d, tzinfo=tz).timestamp())
