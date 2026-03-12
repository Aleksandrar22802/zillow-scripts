Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/zillow_property_scraper.py#1-1), lines 1 to 100

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/zillow_graphql_scraper.py#1-1), lines 1 to 100

Searched for regex `def.*extract|row\s*=\s*{|record\s*=\s*{|output\s*=\s*{|csv.*write|fieldnames|writerow|writer\.` (`**/zillow_graphql_scraper.py`), no results

Searched for regex `def.*extract|row\s*=\s*{|record\s*=\s*{|output\s*=\s*{|csv.*write|fieldnames|writerow|writer\.` (`**/zillow_property_scraper.py`), no results

Searched for text `fieldnames` (`**/zillow_graphql_scraper.py`), no results

Searched for text `fieldnames` (`**/zillow_property_scraper.py`), no results

Searched for text `fieldnames` (`**/zillow_graphql_scraper.py`), no results

Searched for text `fieldnames` (`**/zillow_property_scraper.py`), no results

Ran terminal command: Get-Content "f:\07_Bittensor\Work\Subnet_46\zillow-scripts\zillow_graphql_scraper.py" | Measure-Object -Line

Ran terminal command: Get-Content "f:\07_Bittensor\Work\Subnet_46\zillow-scripts\zillow_property_scraper.py" | Measure-Object -Line

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/zillow_graphql_scraper.py#100-100), lines 100 to 300

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/zillow_property_scraper.py#100-100), lines 100 to 300

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/zillow_graphql_scraper.py#300-300), lines 300 to 600

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/zillow_property_scraper.py#300-300), lines 300 to 600

Now let me read the remaining parts of both files plus the other relevant files:  

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/load_zillow_graphql.py#1-1), lines 1 to 200

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/load_property_extended_search.py#1-1), lines 1 to 200

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/load_zillow_graphql.py#200-200), lines 200 to 500

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/load_zillow_graphql.py#500-500), lines 500 to 684

Now let me read the currently open document and the async scraper: 

Read [](file:///f%3A/07_Bittensor/Work/Subnet_46/zillow-scripts/my_docs/difference%20between%20graphql%20and%20property.md)

Here is the complete output data structure analysis for both files.

---

## Output: File System (both scrapers)

Both write to: `{out_root}/{zipcode}/{zpid}.json`

---

## zillow_graphql_scraper.py — JSON Output Structure

**Source:** `data["data"]["property"]` from Zillow's GraphQL endpoint.  
**Keys explicitly stripped** before writing (zillow_graphql_scraper.py):

| Stripped Key |
|---|
| `topNavJson` |
| `staticMap` |
| `responsivePhotosOriginalRatio` |
| `responsivePhotos` |
| `originalPhotos` |

**Metadata appended** (zillow_graphql_scraper.py):

| Field | Value |
|---|---|
| `zpid` | integer or string |
| `_fetchedAt` | ISO 8601 UTC timestamp |
| `_zipcodeHint` | zipcode string from input CSV |
| `_fetchMethod` | always `"graphql"` |
| `_proxyUsed` | proxy host:port, or `"DIRECT"` |

---

## zillow_property_scraper.py — JSON Output Structure

**Supports two modes** (`--fetch-method`):

### GraphQL mode
Same `data["data"]["property"]` source, but **no keys are stripped** — it is a superset of the above (zillow_property_scraper.py).

### RapidAPI mode
Entire raw `r.json()` from `https://zillow-com1.p.rapidapi.com/property` — a **different top-level schema** (zillow_property_scraper.py).

**Metadata appended** (zillow_property_scraper.py) — same 5 fields, but `_fetchMethod` is dynamic:

| Field | GraphQL mode | RapidAPI mode |
|---|---|---|
| `zpid` | int/str | int/str |
| `_fetchedAt` | ISO 8601 UTC | ISO 8601 UTC |
| `_zipcodeHint` | zipcode string | zipcode string |
| `_fetchMethod` | `"graphql"` | `"rapidapi"` |
| `_proxyUsed` | proxy host:port / `"DIRECT"` | `"DIRECT(RAPIDAPI)"` |

---

## GraphQL `data["data"]["property"]` — Known Fields

Inferred from `convert_to_schema()` in load_zillow_graphql.py:

### Top-level fields

| Field | Type | Notes |
|---|---|---|
| `zpid` | int | Zillow property ID |
| `address` | object | contains `streetAddress` |
| `city` | string | |
| `state` | string | |
| `zipcode` | string | |
| `county` | string | |
| `countyFIPS` | string | |
| `latitude` | float | |
| `longitude` | float | |
| `homeType` | string | e.g. `SINGLE_FAMILY` |
| `yearBuilt` | int | |
| `livingArea` | float | sq ft |
| `lotSize` | float | sq ft |
| `bedrooms` | int | |
| `bathrooms` | float | |
| `price` | int | current listing price |
| `zestimate` | int | Zillow estimate |
| `daysOnZillow` | int | |
| `priceHistory` | array | see below |
| `taxHistory` | array | see below |
| `schools` | array | see below |
| `resoFacts` | object | see below |
| `attributionInfo` | object | contains `mlsId` |

### `priceHistory[]` items

| Field | Type |
|---|---|
| `date` | string `YYYY-MM-DD` |
| `price` | int |
| `pricePerSquareFoot` | float |
| `event` | string (e.g. `"Sold"`, `"Listed for sale"`, `"Price change"`) |
| `postingIsRental` | bool |
| `source` | string |

### `taxHistory[]` items

| Field | Type |
|---|---|
| `time` | int (epoch ms) |
| `value` | int (assessed value) |
| `taxPaid` | int |

### `schools[]` items

| Field | Type |
|---|---|
| `name` | string |
| `rating` | int |
| `distance` | float |
| `grades` | string |

### `resoFacts` object

| Field | Type |
|---|---|
| `parcelNumber` | string |
| `hoaFee` | string/int |
| `taxAssessedValue` | int |
| `interiorFeatures` | string/list |
| `exteriorFeatures` | string/list |
| `bathroomsFull` | int |
| `bathroomsHalf` | int |
| `garageParkingCapacity` | int |
| `parkingCapacity` | int |
| `poolFeatures` | string |
| `fireplaces` | int |
| `stories` | int |
| `heating` | string/list |
| `cooling` | string/list |
| `flooring` | string/list |
| `propertySubType` | string |
| `constructionMaterials` | string/list |
| `sewer` | string |
| `waterSource` | string |
| `highSchoolDistrict` | string |

---

## MongoDB Document Structure (load_zillow_graphql.py)

After loading via `upsert_replace()` (load_zillow_graphql.py):

```json
{
  "zipcode": "90210",
  "zpid": 12345678,
  "uri": "https://www.zillow.com/homedetails/12345678_zpid/",
  "label": "zip:90210",
  "content": { /* convert_to_schema() output — see below */ },
  "contentSize": 4096,
  "datetime": "2026-03-12T00:00:00Z",
  "timeBucketId": 489648
}
```

### `content` (converted schema)

```
metadata
  version, description, collection_date, miner_hot_key

ids
  property: { parcel_number, fips_code }
  zillow:   { zpid }
  mls:      { mls_number }

property
  location: { addresses, street_number, street_name, unit_number,
               city, state, zip_code, zip_code_plus_4, county,
               latitude, longitude }
  features: { interior_features, bedrooms, bathrooms, full_bathrooms,
               half_bathrooms, exterior_features, garage_spaces,
               total_parking_spaces, pool, fireplace, stories,
               hvac_type: {heating, cooling}, flooring_type }
  characteristics: { property_type, property_subtype,
                     construction_material, year_built, year_renovated }
  size: { house_size_sqft, lot_size_acres, lot_size_sqft }
  utilities: { sewer_type, water_source }
  school: { elementary_school, middle_school, high_school, school_district }
  hoa: { hoa_fee_monthly: [{date, value}], hoa_fee_annual: [{date, value}] }

valuation
  assessment: { assessor_tax_values: [{date, value}], assessor_market_values }
  market:     { zestimate_current, zestimate_history,
                price_per_sqft: [{date, value}], comparable_sales }
  rental:     { rent_estimate: [{date, value}] }

market_data
  trends: { days_on_market: [{date, value}] }

home_sales
  sales_history: [{date, value, transaction_type, source}]

market_context
  sale_date, final_sale_price, days_on_market,
  listing_timeline: [{date, event, price}],
  price_changes:    [{date, old_price, new_price, change_percent}]

neighborhood_context
  recent_comparable_sales, market_trends: {median_sale_price_trend}

tax_assessment
  current_assessment,
  assessment_history: [{date, value}],
  annual_taxes:       [{year, amount}]
```

---

## Key Differences Summary

| Aspect | zillow_graphql_scraper.py | zillow_property_scraper.py |
|---|---|---|
| Fetch method | GraphQL only | GraphQL **or** RapidAPI |
| Keys stripped | `topNavJson`, `staticMap`, `responsivePhotos*`, `originalPhotos` | **None** |
| Output schema | Cleaned `data.property` | Full `data.property` (GraphQL) or raw RapidAPI response |
| `_fetchMethod` | Always `"graphql"` | `"graphql"` or `"rapidapi"` |
| `_proxyUsed` | Proxy string / `"DIRECT"` | Same + `"DIRECT(RAPIDAPI)"` for RapidAPI |
