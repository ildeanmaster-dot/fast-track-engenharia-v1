"""Silver: leitura dos JSONL bronze + tipagem + dedup.

Aqui faco o trabalho de "limpar" os dados sem mudar a logica de negocio.
Renomeio campos pra snake_case, parseio datas e timestamps, tiro duplicatas
pela chave primaria de cada entidade.
"""
import json
from pathlib import Path

import pandas as pd

from conf.config import SAMPLES_DIR


# mapa de renomes por entidade. campos que nao aparecem aqui sao mantidos.
RENAMES = {
    "deputados": {
        "id": "id_deputado",
        "siglaPartido": "sigla_partido",
        "siglaUf": "sigla_uf",
        "idLegislatura": "id_legislatura",
        "urlFoto": "url_foto",
    },
    "partidos": {
        "id": "id_partido",
        "sigla": "sigla_partido",
        "nome": "nome_partido",
    },
    "frentes": {
        "id": "id_frente",
        "titulo": "nome_frente",
        "idLegislatura": "id_legislatura",
    },
    "frente_membros": {
        "id": "id_deputado",
        "_parent_id": "id_frente",
        "nome": "nome_deputado",
        "siglaPartido": "sigla_partido",
        "siglaUf": "sigla_uf",
    },
    "orgaos": {
        "id": "id_orgao",
        "sigla": "sigla_orgao",
        "nome": "nome_orgao",
        "codTipoOrgao": "cod_tipo_orgao",
        "tipoOrgao": "tipo_orgao",
        "dataInicio": "data_inicio",
        "dataFim": "data_fim",
    },
    "eventos": {
        "id": "id_evento",
        "dataHoraInicio": "data_hora_inicio",
        "dataHoraFim": "data_hora_fim",
        "descricaoTipo": "descricao_tipo",
    },
    "evento_deputados": {
        "id": "id_deputado",
        "_parent_id": "id_evento",
        "siglaPartido": "sigla_partido",
        "siglaUf": "sigla_uf",
    },
    "votacoes": {
        "id": "id_votacao",
        "dataHoraRegistro": "data_hora_registro",
    },
    "votacao_votos": {
        "_parent_id": "id_votacao",
        "tipoVoto": "tipo_voto",
        "dataRegistroVoto": "data_registro_voto",
    },
    "deputado_despesas": {
        "_parent_id": "id_deputado",
        "tipoDespesa": "tipo_despesa",
        "codDocumento": "cod_documento",
        "valorDocumento": "valor_documento",
        "valorLiquido": "valor_liquido",
        "valorGlosa": "valor_glosa",
        "dataDocumento": "data_documento",
        "nomeFornecedor": "nome_fornecedor",
        "cnpjCpfFornecedor": "cnpj_fornecedor",
    },
}

# chaves primarias pra dedup
PKS = {
    "deputados": ["id_deputado"],
    "partidos": ["id_partido"],
    "frentes": ["id_frente"],
    "frente_membros": ["id_frente", "id_deputado"],
    "orgaos": ["id_orgao"],
    "eventos": ["id_evento"],
    "evento_deputados": ["id_evento", "id_deputado"],
    "votacoes": ["id_votacao"],
    "votacao_votos": ["id_votacao", "id_deputado"],
    "deputado_despesas": ["id_deputado", "cod_documento"],
}

# colunas de data/timestamp por entidade
DATE_COLS = {
    "orgaos": [("data_inicio", "date"), ("data_fim", "date")],
    "eventos": [("data_hora_inicio", "ts"), ("data_hora_fim", "ts")],
    "votacoes": [("data_hora_registro", "ts")],
    "votacao_votos": [("data_registro_voto", "ts")],
    "deputado_despesas": [("data_documento", "ts")],
}


def _read_jsonl(name):
    path = Path(SAMPLES_DIR) / f"{name}.jsonl"
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _flatten_deputado_inner(rec):
    """Em /votacoes/{id}/votos cada registro tem um 'deputado_' aninhado."""
    if "deputado_" in rec and isinstance(rec["deputado_"], dict):
        d = rec.pop("deputado_")
        rec["id_deputado"] = d.get("id")
        rec["nome_deputado"] = d.get("nome")
        rec["sigla_partido"] = d.get("siglaPartido")
        rec["sigla_uf"] = d.get("siglaUf")
    return rec


def to_silver(name):
    """Le bronze, aplica renomes, tipagem e dedup. Retorna DataFrame."""
    raw = _read_jsonl(name)
    if not raw:
        return pd.DataFrame()

    # caso especial: votos vem com deputado_ aninhado
    if name == "votacao_votos":
        raw = [_flatten_deputado_inner(r) for r in raw]

    df = pd.json_normalize(raw, sep="_")

    # remover coluna _audit interna (vira _audit_*)
    df = df.drop(columns=[c for c in df.columns if c.startswith("_audit")], errors="ignore")

    # rename
    df = df.rename(columns=RENAMES.get(name, {}))

    # parse datas
    for col, kind in DATE_COLS.get(name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=(kind == "ts"))

    # dedup pelas PKs (mantem a ultima ocorrencia)
    pk = PKS.get(name)
    if pk and all(c in df.columns for c in pk):
        df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)

    return df


def to_silver_all():
    """Roda silver para todas as entidades. Retorna dict de DataFrames."""
    out = {}
    for name in RENAMES.keys():
        try:
            out[name] = to_silver(name)
        except FileNotFoundError:
            print(f"[warn] silver: bronze de '{name}' nao existe, pulando")
    return out
