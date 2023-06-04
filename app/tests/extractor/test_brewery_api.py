from app.persist.JSONPersist import JSONPersist
from app.extractor.BreweryAPI import BreweryAPI


api = BreweryAPI(JSONPersist())

def test_api_online():
    try:
        api._get_total_records()
    except:
        raise SystemError('Error access Brewery API')


def test_data_api():
    assert api._get_total_records() > 0, 'Brewery API without records'


test_api_online()
test_data_api()

