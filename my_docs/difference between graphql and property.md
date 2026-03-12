The output data structures are only the same when zillow_property_scraper.py runs in GraphQL mode. Otherwise they diverge at the root schema.

zillow_graphql_scraper.py always writes a single normalized GraphQL property object. It pulls `data["data"]["property"]` from the Zillow GraphQL response in zillow_graphql_scraper.py, removes several bulky keys in zillow_graphql_scraper.py, then adds local metadata before writing in zillow_graphql_scraper.py. So its output shape is:

```json
{
  "...graphQL property fields...": "...",
  "zpid": 123,
  "_fetchedAt": "2026-03-12T...",
  "_zipcodeHint": "90210",
  "_fetchMethod": "graphql",
  "_proxyUsed": "http://host:port"
}
```

The keys explicitly removed from the GraphQL output are `topNavJson`, `staticMap`, `responsivePhotosOriginalRatio`, `responsivePhotos`, and `originalPhotos` at zillow_graphql_scraper.py. That means this script produces a smaller, more curated JSON object.

zillow_property_scraper.py is structurally different because its output depends on `--fetch-method` in zillow_property_scraper.py. In GraphQL mode, it also writes `data["data"]["property"]`, but unlike the GraphQL-only script it does not remove those large keys before writing; see zillow_property_scraper.py and zillow_property_scraper.py. So GraphQL-mode output from this script is usually a superset of the GraphQL-only output.

In RapidAPI mode, zillow_property_scraper.py writes the entire RapidAPI response object from zillow_property_scraper.py. That is a different schema entirely. It still appends the same local metadata fields in zillow_property_scraper.py, but the business fields come from RapidAPI rather than Zillow GraphQL. Its output shape is therefore:

```json
{
  "...rapidapi property fields...": "...",
  "zpid": 123,
  "_fetchedAt": "2026-03-12T...",
  "_zipcodeHint": "90210",
  "_fetchMethod": "rapidapi",
  "_proxyUsed": "DIRECT(RAPIDAPI)"
}
```

The shared metadata layer is mostly consistent across both files:
`zpid`, `_fetchedAt`, `_zipcodeHint`, `_fetchMethod`, `_proxyUsed`.
See zillow_graphql_scraper.py and zillow_property_scraper.py. The one semantic difference is `_fetchMethod`: it is always `"graphql"` in zillow_graphql_scraper.py at zillow_graphql_scraper.py, but dynamic in zillow_property_scraper.py at zillow_property_scraper.py.

Net result: zillow_graphql_scraper.py outputs a cleaned, GraphQL-specific property schema, while zillow_property_scraper.py outputs either an untrimmed GraphQL property schema or a different RapidAPI schema, with the same metadata wrapper added in both cases.

If useful, I can turn this into a field-by-field schema table showing:
1. keys only in GraphQL output,
2. keys removed by the GraphQL-only scraper,
3. metadata keys common to both.
