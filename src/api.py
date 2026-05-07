"""Cliente HTTP da API da Camara."""
import time
import logging
import requests
from urllib.parse import urlparse, parse_qs

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "ftk-camara/0.1",
}

log = logging.getLogger(__name__)


class CamaraAPIError(Exception):
    pass


def get_json(path, params=None, retries=3, backoff=1.5):
    """Faz GET com retry simples em 429/5xx."""
    url = f"{BASE_URL}{path}" if path.startswith("/") else f"{BASE_URL}/{path}"
    last_err = None

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=DEFAULT_HEADERS, params=params, timeout=30)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                wait = backoff ** attempt
                log.warning("status %d em %s, retry em %.1fs", r.status_code, url, wait)
                time.sleep(wait)
                last_err = f"HTTP {r.status_code}"
                continue
            # outros erros nao da retry
            raise CamaraAPIError(f"HTTP {r.status_code} em {url}: {r.text[:200]}")
        except requests.RequestException as e:
            log.warning("erro de rede em %s: %s", url, e)
            time.sleep(backoff ** attempt)
            last_err = str(e)

    raise CamaraAPIError(f"falhou apos {retries} tentativas em {url}: {last_err}")


def parse_next_page(link_header):
    """Pega a URL da proxima pagina do header Link, ou None."""
    if not link_header:
        return None
    # formato: <url>; rel="self", <url>; rel="next", ...
    parts = link_header.split(",")
    for p in parts:
        if 'rel="next"' in p:
            url = p.split(";")[0].strip().lstrip("<").rstrip(">")
            return url
    return None


def iter_pages(path, params=None, max_pages=None):
    """Itera sobre paginas de um endpoint paginado.

    Yields tuplas (lista_de_registros, page_num, source_url).
    """
    params = dict(params or {})
    page = 1

    while True:
        if max_pages and page > max_pages:
            break

        params["pagina"] = page
        r = get_json(path, params=params)
        data = r.json()
        records = data.get("dados", [])

        # alguns endpoints retornam dict em vez de lista
        if isinstance(records, dict):
            records = [records]

        yield records, page, r.url

        # checa se tem proxima pagina
        next_url = parse_next_page(r.headers.get("Link"))
        if not next_url:
            break

        # extrai o numero da pagina da URL pra ter certeza
        qs = parse_qs(urlparse(next_url).query)
        next_page = qs.get("pagina", [str(page + 1)])[0]
        if int(next_page) <= page:
            break
        page = int(next_page)

        # rate limit basico, evita 429
        time.sleep(0.25)
