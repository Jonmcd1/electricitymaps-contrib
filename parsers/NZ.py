#!/usr/bin/env python3

# The arrow library is used to handle datetimes
import json
from datetime import datetime, timezone
from logging import Logger, getLogger

import arrow
from bs4 import BeautifulSoup

# The request library is used to fetch content through HTTP
from requests import Session

time_zone = "Pacific/Auckland"

NZ_PRICE_REGIONS = set(range(1, 14))
PRODUCTION_URL = "https://www.transpower.co.nz/system-operator/live-system-and-market-data/consolidated-live-data"
PRICE_URL = "https://api.em6.co.nz/ords/em6/data_api/region/price/"

### DEBUGGING CONTROLS ###
print_arrow_comparison = True
save_outputs = False
### END DEBUGGING CONTROLS

def fetch(session: Session | None = None):
    r = session or Session()
    url = PRODUCTION_URL
    response = r.get(url)

    if save_outputs:
        f = open("test/mocks/NZ/response.html", "w")
        f.write(response.text)
        f.close()

    soup = BeautifulSoup(response.text, "html.parser")
    for item in soup.find_all("script"):
        if item.attrs.get("data-drupal-selector"):
            body = item.contents[0]
            obj = json.loads(body)
    return obj


def fetch_price(
    zone_key: str = "NZ",
    session: Session | None = None,
    target_datetime: datetime | None = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """
    Requests the current price of electricity based on the zone key.

    Note that since EM6 breaks the electricity price down into regions,
    the regions are averaged out for each island.
    """
    if target_datetime:
        raise NotImplementedError(
            "This parser is not able to retrieve data for past dates"
        )

    r = session or Session()
    url = PRICE_URL
    response = r.get(url, verify=False)

    if save_outputs:
        with open('test/mocks/NZ/response.json', 'w') as f:
            json.dump(response.json(), f)

    obj = response.json()
    region_prices = []

    regions = NZ_PRICE_REGIONS
    for item in obj.get("items"):
        region = item.get("grid_zone_id")
        if region in regions:
            time = item.get("timestamp")
            price = float(item.get("price"))
            region_prices.append(price)

    avg_price = sum(region_prices) / len(region_prices)
    #date_time = arrow.get(time, tzinfo="UTC") # Old code
    date_time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) # New code

    if print_arrow_comparison:
        print("\n\n\n################ ARROW BEGIN ################")
        print("Time:", time)
        print("arrow.get(time, tzinfo='UTC'):", arrow.get(time, tzinfo="UTC"))
        print("arrow.get(time, tzinfo='UTC').datetime:", arrow.get(time, tzinfo="UTC").datetime, end="\n\n\n")
        print("arrow output:", arrow.get(time, tzinfo="UTC").datetime)
        print("datetime output:", datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc))
        print("################ ARROW END ################\n\n\n")
    

    return {
        "datetime": date_time,
        "price": avg_price,
        "currency": "NZD",
        "source": "api.em6.co.nz",
        "zoneKey": zone_key,
    }


def fetch_production(
    zone_key: str = "NZ",
    session: Session | None = None,
    target_datetime: datetime | None = None,
    logger: Logger = getLogger(__name__),
) -> dict:
    """Requests the last known production mix (in MW) of a given zone."""
    if target_datetime:
        raise NotImplementedError(
            "This parser is not able to retrieve data for past dates"
        )

    obj = fetch(session)

    #date_time = arrow.get(str(obj["soPgenGraph"]["timestamp"]), "X").datetime # Old code
    date_time = datetime.fromtimestamp(obj["soPgenGraph"]["timestamp"], tz=timezone.utc) # New code

    if print_arrow_comparison:
        print("\n\n\n################ ARROW BEGIN ################")
        print("Time object:", obj["soPgenGraph"]["timestamp"])
        print("Time:", str(obj["soPgenGraph"]["timestamp"]))
        print("arrow.get(str(obj['soPgenGraph']['timestamp']), 'X').datetime:", arrow.get(str(obj["soPgenGraph"]["timestamp"]), "X").datetime, end="\n\n\n")
        print("arrow output:", arrow.get(str(obj["soPgenGraph"]["timestamp"]), "X").datetime)
        print("datetime output:", datetime.fromtimestamp(obj["soPgenGraph"]["timestamp"], tz=timezone.utc))
        print("################ ARROW END ################\n\n\n")

    region_key = "New Zealand"
    productions = obj["soPgenGraph"]["data"][region_key]

    data = {
        "zoneKey": zone_key,
        "datetime": date_time,
        "production": {
            "coal": productions.get("Coal", {"generation": None})["generation"],
            "oil": productions.get("Diesel/Oil", {"generation": None})["generation"],
            "gas": productions.get("Gas", {"generation": None})["generation"],
            "geothermal": productions.get("Geothermal", {"generation": None})[
                "generation"
            ],
            "wind": productions.get("Wind", {"generation": None})["generation"],
            "hydro": productions.get("Hydro", {"generation": None})["generation"],
            "solar": productions.get("Solar", {"generation": None})["generation"],
            "unknown": productions.get("Co-Gen", {"generation": None})["generation"],
            "nuclear": 0,  # famous issue in NZ politics
        },
        "capacity": {
            "coal": productions.get("Coal", {"capacity": None})["capacity"],
            "oil": productions.get("Diesel/Oil", {"capacity": None})["capacity"],
            "gas": productions.get("Gas", {"capacity": None})["capacity"],
            "geothermal": productions.get("Geothermal", {"capacity": None})["capacity"],
            "wind": productions.get("Wind", {"capacity": None})["capacity"],
            "hydro": productions.get("Hydro", {"capacity": None})["capacity"],
            "solar": productions.get("Solar", {"capacity": None})["capacity"],
            "battery storage": productions.get("Battery", {"capacity": None})[
                "capacity"
            ],
            "unknown": productions.get("Co-Gen", {"capacity": None})["capacity"],
            "nuclear": 0,  # famous issue in NZ politics
        },
        "storage": {
            "battery": productions.get("Battery", {"generation": None})["generation"],
        },
        "source": "transpower.co.nz",
    }

    return data


if __name__ == "__main__":
    """Main method, never used by the Electricity Map backend, but handy for testing."""
    print("fetch_price(NZ) ->")
    print(fetch_price("NZ"))
    print("fetch_production(NZ) ->")
    print(fetch_production("NZ"))
