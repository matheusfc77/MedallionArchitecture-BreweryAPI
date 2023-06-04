from app.persist.JSONPersist import JSONPersist
from app.extractor.BreweryAPI import BreweryAPI

# extract data of brewery API
api = BreweryAPI(JSONPersist())
api.extract_records('app/data/', 'breweries', per_page=150)#, limit_pages=3)