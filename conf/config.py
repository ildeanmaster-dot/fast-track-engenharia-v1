"""Catalogo dos endpoints que vamos usar.

Mantenho como dict Python pra facilitar tab-complete e nao precisar de yaml.
Cada entrada tem o path, params default e (se for fan-out) o pai pra puxar IDs.
"""

# endpoints principais (sao os 8 que cobrem os 6 entregaveis)
ENDPOINTS = {
    # listas base
    "deputados": {
        "path": "/deputados",
        "params": {"itens": 100, "ordem": "ASC", "ordenarPor": "nome"},
        "paginated": True,
    },
    "partidos": {
        "path": "/partidos",
        "params": {"itens": 100},
        "paginated": True,
    },
    "frentes": {
        "path": "/frentes",
        "params": {"idLegislatura": 57, "itens": 100},
        "paginated": True,
    },
    "orgaos": {
        "path": "/orgaos",
        "params": {"itens": 100},
        "paginated": True,
    },
    "eventos": {
        "path": "/eventos",
        "params": {"itens": 100},
        "paginated": True,
    },
    "votacoes": {
        "path": "/votacoes",
        "params": {"itens": 100},
        "paginated": True,
    },

    # fan-outs (precisam de id do pai)
    "frente_membros": {
        "path": "/frentes/{id}/membros",
        "params": {},
        "paginated": False,
        "fanout_from": "frentes",
    },
    "deputado_despesas": {
        "path": "/deputados/{id}/despesas",
        "params": {"itens": 100, "ano": 2024, "ordem": "DESC", "ordenarPor": "ano"},
        "paginated": True,
        "fanout_from": "deputados",
    },
    "evento_deputados": {
        "path": "/eventos/{id}/deputados",
        "params": {},
        "paginated": False,
        "fanout_from": "eventos",
    },
    "votacao_votos": {
        "path": "/votacoes/{id}/votos",
        "params": {},
        "paginated": False,
        "fanout_from": "votacoes",
    },
}

# limites para nao explodir tempo de coleta nos fan-outs
FANOUT_LIMITS = {
    "frente_membros": 30,        # 30 frentes x ~30 membros = ~900 registros
    "deputado_despesas": 20,     # 20 dep x ~50 despesas = ~1000
    "evento_deputados": 10,      # 10 eventos x ~10 deputados = ~100
    "votacao_votos": 30,         # 30 votacoes x ~50 votos = ~1500
}

# diretorio onde salvar JSONL local (pode ser sobrescrito)
SAMPLES_DIR = "data/samples"
