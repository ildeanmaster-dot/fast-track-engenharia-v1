"""Gold: star schema + 6 entregaveis analiticos.

Fluxo:
  silver dict -> dim_* + fato_* -> gold_* (entregaveis)

Cada entregavel e uma funcao "build_*" que devolve um DataFrame pandas.
Em prod (Databricks) os DataFrames viram tabelas Delta no Volume.
"""
import pandas as pd


# ----------------------------------------------------------------------
# Dimensoes
# ----------------------------------------------------------------------

def dim_deputado(silver):
    df = silver["deputados"][[
        "id_deputado", "nome", "sigla_partido", "sigla_uf", "id_legislatura"
    ]].copy()
    return df


def dim_partido(silver):
    df = silver["partidos"][["id_partido", "sigla_partido", "nome_partido"]].copy()
    return df


def dim_frente(silver):
    df = silver["frentes"][["id_frente", "nome_frente", "id_legislatura"]].copy()
    return df


def dim_orgao(silver):
    df = silver["orgaos"][[
        "id_orgao", "sigla_orgao", "nome_orgao", "tipo_orgao", "cod_tipo_orgao"
    ]].copy()
    return df


def dim_evento(silver):
    df = silver["eventos"][[
        "id_evento", "data_hora_inicio", "data_hora_fim", "descricao_tipo", "situacao"
    ]].copy()
    return df


# ----------------------------------------------------------------------
# Fatos
# ----------------------------------------------------------------------

def fato_voto(silver):
    """Voto individual de cada deputado em cada votacao."""
    df = silver["votacao_votos"].copy()
    return df[["id_votacao", "id_deputado", "tipo_voto", "data_registro_voto"]]


def fato_presenca(silver):
    """Deputados que apareceram em cada evento."""
    df = silver["evento_deputados"].copy()
    return df[["id_evento", "id_deputado", "sigla_partido", "sigla_uf"]]


def fato_despesa(silver):
    """Despesas CEAP por deputado."""
    df = silver["deputado_despesas"].copy()
    cols = [
        "id_deputado", "ano", "mes", "tipo_despesa", "valor_liquido",
        "nome_fornecedor", "cnpj_fornecedor", "data_documento",
    ]
    cols = [c for c in cols if c in df.columns]
    return df[cols]


# ----------------------------------------------------------------------
# Entregavel 1 - Atlas das Frentes
# ----------------------------------------------------------------------

def gold_atlas_frentes(silver):
    """Junta frentes com membros e enriquece com partido/UF do deputado."""
    fr = silver["frentes"][["id_frente", "nome_frente", "id_legislatura"]]
    fm = silver["frente_membros"][["id_frente", "id_deputado"]]
    dep = silver["deputados"][["id_deputado", "nome", "sigla_partido", "sigla_uf"]]
    atlas = (
        fm.merge(dep, on="id_deputado", how="left")
          .merge(fr, on="id_frente", how="left")
    )
    return atlas


def gold_frente_diversidade_hhi(atlas):
    """Indice de Herfindahl por frente.

    HHI = soma(quadrado da participacao de cada partido).
    Baixo = mais diverso. Alto = concentrado num partido.
    """
    rows = []
    for fid, grp in atlas.groupby("id_frente"):
        total = len(grp)
        if total == 0:
            continue
        partidos = grp["sigla_partido"].fillna("?").value_counts()
        share = partidos / total
        hhi = (share ** 2).sum()
        rows.append({
            "id_frente": fid,
            "nome_frente": grp["nome_frente"].iloc[0],
            "n_membros": int(total),
            "n_partidos": int((partidos > 0).sum()),
            "n_ufs": grp["sigla_uf"].fillna("?").nunique(),
            "hhi": float(hhi),
        })
    return pd.DataFrame(rows).sort_values("hhi").reset_index(drop=True)


def gold_deputados_em_n_frentes(atlas, top=20):
    """Top deputados por numero de frentes em que aparecem."""
    return (
        atlas.groupby(["id_deputado", "nome", "sigla_partido", "sigla_uf"])
             .size()
             .reset_index(name="n_frentes")
             .sort_values("n_frentes", ascending=False)
             .head(top)
             .reset_index(drop=True)
    )
