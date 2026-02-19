import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pipeline.weather.models import NwsStationObservation

API_URL = 'https://api.weather.gov'
USER_AGENT = 'SDPipe (contact: camacho_apolinar97@gmail.com)'


def _create_https_session() -> requests.Session:
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session = requests.Session()
    session.mount("https://",adapter)
    session.headers.update({"User-Agent":USER_AGENT})
    return session

def fetch_latest_observation(station_id:str, require_qc:bool = True) -> NwsStationObservation:
    session = _create_https_session()
    url = API_URL + f'/stations/{station_id}/observations/latest'
    params = {"require_qc":str(require_qc).lower()}
    response = session.get(url,params=params,timeout=10)
    response.raise_for_status()
    return NwsStationObservation.model_validate(response.json())