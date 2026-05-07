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


# ----------------------------------------------------------------------
# Entregavel 2 - Calendario de eventos
# ----------------------------------------------------------------------

def gold_taxa_presenca(silver):
    """Taxa de presenca por deputado: presencas / total de eventos."""
    eventos = silver["eventos"]["id_evento"]
    total_eventos = eventos.nunique()
    if total_eventos == 0:
        return pd.DataFrame()

    presencas = (
        silver["evento_deputados"]
        .groupby("id_deputado")
        .size()
        .reset_index(name="n_presencas")
    )
    presencas["total_eventos"] = total_eventos
    presencas["taxa_presenca"] = presencas["n_presencas"] / total_eventos

    # adiciona nome
    dep = silver["deputados"][["id_deputado", "nome", "sigla_partido", "sigla_uf"]]
    return presencas.merge(dep, on="id_deputado", how="left").sort_values(
        "taxa_presenca", ascending=False
    ).reset_index(drop=True)


def gold_densidade_semanal(silver):
    """Quantos eventos por semana (ano-semana ISO)."""
    df = silver["eventos"].copy()
    df["data_hora_inicio"] = pd.to_datetime(df["data_hora_inicio"], errors="coerce", utc=True)
    df = df.dropna(subset=["data_hora_inicio"])
    df["ano"] = df["data_hora_inicio"].dt.isocalendar().year
    df["semana"] = df["data_hora_inicio"].dt.isocalendar().week
    out = (
        df.groupby(["ano", "semana"])
          .size()
          .reset_index(name="qtd_eventos")
          .sort_values(["ano", "semana"])
          .reset_index(drop=True)
    )
    return out


def gold_eventos_futuros(silver):
    """View de eventos com data futura (calendario publico)."""
    df = silver["eventos"].copy()
    df["data_hora_inicio"] = pd.to_datetime(df["data_hora_inicio"], errors="coerce", utc=True)
    now = pd.Timestamp.utcnow()
    futuros = df[df["data_hora_inicio"] > now].copy()
    return futuros.sort_values("data_hora_inicio").reset_index(drop=True)


# ----------------------------------------------------------------------
# Entregavel 3 - Correlacao frente x votacao
# ----------------------------------------------------------------------

def _alinhamento_por_grupo(votos, group_col):
    """Pra cada votacao+grupo, calcula % do voto majoritario.

    Se 80% dos membros votaram SIM, alinhamento=0.8. So conta votos validos
    (Sim/Nao - ignora abstencao/obstrucao por enquanto).
    """
    df = votos[votos["tipo_voto"].isin(["Sim", "Nao"])].copy()
    if df.empty:
        return pd.DataFrame()

    grupo = df.groupby(["id_votacao", group_col, "tipo_voto"]).size().reset_index(name="n")
    pivot = grupo.pivot_table(
        index=["id_votacao", group_col],
        columns="tipo_voto", values="n", fill_value=0
    ).reset_index()

    # garante colunas Sim/Nao
    for c in ["Sim", "Nao"]:
        if c not in pivot.columns:
            pivot[c] = 0

    pivot["total"] = pivot["Sim"] + pivot["Nao"]
    # so faz sentido se tiver >= 2 votantes
    pivot = pivot[pivot["total"] >= 2].copy()
    pivot["alinhamento"] = pivot[["Sim", "Nao"]].max(axis=1) / pivot["total"]

    return pivot[[c for c in ["id_votacao", group_col, "Sim", "Nao", "total", "alinhamento"] if c in pivot.columns]]


def gold_alinhamento_frente_vs_partido(silver):
    """Compara alinhamento medio dentro de frentes vs dentro de partidos.

    Retorna duas tabelas: por frente e por partido. A ideia e ver se deputados
    que estao na mesma frente votam "mais juntos" do que os do mesmo partido.
    """
    votos = silver["votacao_votos"].copy()

    # alinhamento por partido
    ali_partido = _alinhamento_por_grupo(votos, "sigla_partido")
    resumo_partido = (
        ali_partido.groupby("sigla_partido")
        .agg(alinhamento_medio=("alinhamento", "mean"),
             qtd_votacoes=("id_votacao", "nunique"))
        .reset_index()
        .sort_values("alinhamento_medio", ascending=False)
    )

    # alinhamento por frente: precisa explodir voto por frente do deputado
    fm = silver["frente_membros"][["id_frente", "id_deputado"]]
    votos_frente = votos.merge(fm, on="id_deputado")
    ali_frente = _alinhamento_por_grupo(votos_frente, "id_frente")

    fr = silver["frentes"][["id_frente", "nome_frente"]]
    resumo_frente = (
        ali_frente.groupby("id_frente")
        .agg(alinhamento_medio=("alinhamento", "mean"),
             qtd_votacoes=("id_votacao", "nunique"))
        .reset_index()
        .merge(fr, on="id_frente", how="left")
        .sort_values("alinhamento_medio", ascending=False)
    )

    return resumo_frente, resumo_partido


# ----------------------------------------------------------------------
# Entregavel 4 - Raio-X CEAP
# ----------------------------------------------------------------------

def gold_ceap_top_por_categoria(silver, top_n=10):
    """Top N gastos por (tipo_despesa).

    Versao simples: nao calcula z-score, so pega os maiores valores
    individuais por categoria. Util pra varredura visual.
    """
    df = silver["deputado_despesas"].copy()
    if df.empty:
        return pd.DataFrame()

    # enriquece com nome do deputado
    dep = silver["deputados"][["id_deputado", "nome", "sigla_partido", "sigla_uf"]]
    df = df.merge(dep, on="id_deputado", how="left")

    # ranking por categoria
    df["rank"] = df.groupby("tipo_despesa")["valor_liquido"].rank(method="first", ascending=False)
    out = df[df["rank"] <= top_n].copy()
    return out[[
        "tipo_despesa", "rank", "id_deputado", "nome", "sigla_partido", "sigla_uf",
        "valor_liquido", "nome_fornecedor", "cnpj_fornecedor", "data_documento",
    ]].sort_values(["tipo_despesa", "rank"]).reset_index(drop=True)


def gold_ceap_ranking_fornecedor(silver):
    """Top fornecedores por valor total recebido."""
    df = silver["deputado_despesas"].copy()
    if df.empty:
        return pd.DataFrame()

    out = (
        df.groupby(["cnpj_fornecedor", "nome_fornecedor"], dropna=False)
        .agg(
            valor_total=("valor_liquido", "sum"),
            qtd_documentos=("cod_documento", "count"),
            qtd_deputados=("id_deputado", "nunique"),
        )
        .reset_index()
        .sort_values("valor_total", ascending=False)
    )
    return out.head(100).reset_index(drop=True)


def gold_ceap_mensal_partido(silver):
    """Total gasto por partido, por mes (relatorio mensal)."""
    df = silver["deputado_despesas"].copy()
    if df.empty:
        return pd.DataFrame()
    dep = silver["deputados"][["id_deputado", "sigla_partido"]]
    df = df.merge(dep, on="id_deputado", how="left")
    out = (
        df.groupby(["ano", "mes", "sigla_partido"])
        .agg(
            total=("valor_liquido", "sum"),
            qtd=("cod_documento", "count"),
        )
        .reset_index()
        .sort_values(["ano", "mes", "total"], ascending=[True, True, False])
    )
    return out


# ----------------------------------------------------------------------
# Entregavel 5 - CPIs
# ----------------------------------------------------------------------

def gold_cpis(silver):
    """Identifica orgaos que sao CPIs/CPMIs e calcula duracao.

    Heuristica: filtro por nome contendo 'CPI', 'CPMI' ou 'inquerito'.
    Limitacao conhecida: a API publica so expoe orgaos da legislatura corrente
    ativos, entao CPIs encerradas historicas nao aparecem.
    """
    df = silver["orgaos"].copy()
    mask = df["nome_orgao"].fillna("").str.contains(
        r"CPI|CPMI|Inqu[eé]rito", case=False, regex=True, na=False
    )
    cpis = df[mask].copy()

    # garante colunas opcionais (a API as vezes nao retorna data_inicio/fim)
    for c in ("data_inicio", "data_fim"):
        if c not in cpis.columns:
            cpis[c] = pd.NaT

    if cpis.empty:
        return pd.DataFrame(columns=["id_orgao", "sigla_orgao", "nome_orgao",
                                     "tipo_orgao",
                                     "data_inicio", "data_fim", "duracao_dias",
                                     "excedeu_prazo"])

    cpis["data_inicio"] = pd.to_datetime(cpis["data_inicio"], errors="coerce")
    cpis["data_fim"] = pd.to_datetime(cpis["data_fim"], errors="coerce")
    cpis["duracao_dias"] = (cpis["data_fim"] - cpis["data_inicio"]).dt.days
    # prazo regimental tipico de CPI = 180 dias (regimento)
    cpis["excedeu_prazo"] = cpis["duracao_dias"].fillna(0) > 180

    cols = ["id_orgao", "sigla_orgao", "nome_orgao", "tipo_orgao",
            "data_inicio", "data_fim", "duracao_dias", "excedeu_prazo"]
    cols = [c for c in cols if c in cpis.columns]
    return cpis[cols].reset_index(drop=True)


# ----------------------------------------------------------------------
# Entregavel 6 - Engajamento
# ----------------------------------------------------------------------

def gold_engajamento_deputado(silver):
    """Score simples de engajamento: presencas + votos validos.

    Cada componente normalizado (min-max). Score = media dos componentes.
    Versao basica - nao inclui discursos/requerimentos por enquanto.
    """
    dep = silver["deputados"][["id_deputado", "nome", "sigla_partido", "sigla_uf"]]

    # presencas em eventos
    presencas = (
        silver["evento_deputados"]
        .groupby("id_deputado").size().reset_index(name="n_presencas")
    )

    # votos (qualquer tipo)
    votos = (
        silver["votacao_votos"]
        .groupby("id_deputado").size().reset_index(name="n_votos")
    )

    score = dep.merge(presencas, on="id_deputado", how="left") \
               .merge(votos, on="id_deputado", how="left")
    score[["n_presencas", "n_votos"]] = score[["n_presencas", "n_votos"]].fillna(0)

    # normaliza min-max
    def _norm(s):
        rng = s.max() - s.min()
        if rng == 0:
            return s * 0.0
        return (s - s.min()) / rng

    score["presencas_norm"] = _norm(score["n_presencas"])
    score["votos_norm"] = _norm(score["n_votos"])
    score["engajamento"] = (score["presencas_norm"] + score["votos_norm"]) / 2

    # percentil dentro do conjunto
    score["percentil"] = score["engajamento"].rank(pct=True)

    return score.sort_values("engajamento", ascending=False).reset_index(drop=True)


def gold_absenteismo_votacao(silver):
    """Lista deputados que mais faltaram em votacoes (% de ausencia).

    Considera todas as votacoes do periodo. Se um deputado nao tem voto
    registrado em uma votacao, conta como ausencia.
    """
    dep = silver["deputados"][["id_deputado", "nome", "sigla_partido", "sigla_uf"]]
    total_votacoes = silver["votacoes"]["id_votacao"].nunique()
    if total_votacoes == 0:
        return pd.DataFrame()

    # quantas votacoes cada deputado participou
    votou = (
        silver["votacao_votos"]
        .groupby("id_deputado")["id_votacao"]
        .nunique()
        .reset_index(name="n_votou")
    )

    out = dep.merge(votou, on="id_deputado", how="left")
    out["n_votou"] = out["n_votou"].fillna(0).astype(int)
    out["n_ausencias"] = total_votacoes - out["n_votou"]
    out["taxa_ausencia"] = out["n_ausencias"] / total_votacoes

    return out.sort_values("taxa_ausencia", ascending=False).reset_index(drop=True)
