"""Camada bronze: ingestao da API pra JSONL local.

A versao Databricks (notebooks/01_ingest_bronze.py) le esses JSONL e converte
pra Delta no Volume UC. Aqui o objetivo e gravar a versao crua, sem
transformacao alem das colunas de auditoria (ingest_ts, source_url).
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from conf.config import ENDPOINTS, FANOUT_LIMITS, SAMPLES_DIR
from src.api import iter_pages, get_json


def _audit(endpoint, source_url):
    return {
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "endpoint": endpoint,
        "source_url": source_url,
    }


def collect_simple(name, max_pages=None):
    """Coleta um endpoint paginado simples e salva em JSONL."""
    cfg = ENDPOINTS[name]
    out = Path(SAMPLES_DIR) / f"{name}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)

    n = 0
    with out.open("w", encoding="utf-8") as f:
        for records, page_num, source_url in iter_pages(
            cfg["path"], params=cfg["params"], max_pages=max_pages
        ):
            for rec in records:
                rec["_audit"] = _audit(name, source_url)
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1
    return n, out


def _load_parents(parent_name):
    """Le os IDs do JSONL do pai pra fazer fanout."""
    path = Path(SAMPLES_DIR) / f"{parent_name}.jsonl"
    if not path.exists():
        raise RuntimeError(
            f"pai '{parent_name}' nao foi coletado ainda. "
            f"Rode collect_simple('{parent_name}') primeiro."
        )
    ids = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "id" in obj:
                ids.append(obj["id"])
    return ids


def collect_fanout(name, max_parents=None):
    """Coleta um endpoint que depende de id do pai."""
    cfg = ENDPOINTS[name]
    parent = cfg["fanout_from"]
    parent_ids = _load_parents(parent)

    if max_parents is None:
        max_parents = FANOUT_LIMITS.get(name, 10)
    parent_ids = parent_ids[:max_parents]

    out = Path(SAMPLES_DIR) / f"{name}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    with out.open("w", encoding="utf-8") as f:
        for pid in parent_ids:
            path = cfg["path"].format(id=pid)
            try:
                if cfg["paginated"]:
                    for records, _, source_url in iter_pages(path, params=cfg["params"]):
                        for rec in records:
                            rec["_parent_id"] = pid
                            rec["_audit"] = _audit(name, source_url)
                            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            total += 1
                else:
                    r = get_json(path, params=cfg["params"])
                    payload = r.json()
                    records = payload.get("dados", []) or []
                    if isinstance(records, dict):
                        records = [records]
                    for rec in records:
                        rec["_parent_id"] = pid
                        rec["_audit"] = _audit(name, r.url)
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        total += 1
            except Exception as e:
                # registra mas nao falha a coleta inteira
                print(f"[warn] {name} pid={pid}: {e}")
    return total, out


def collect_all(max_pages_simple=2):
    """Pipeline completo de coleta - usado pelo runner."""
    results = {}

    # primeiro os simples (fontes)
    for name in ["partidos", "deputados", "frentes", "orgaos", "eventos", "votacoes"]:
        n, path = collect_simple(name, max_pages=max_pages_simple)
        results[name] = n
        print(f"  {name:<22s} {n:>5d} -> {path.name}")

    # depois os fanouts
    for name in ["frente_membros", "deputado_despesas", "evento_deputados", "votacao_votos"]:
        n, path = collect_fanout(name)
        results[name] = n
        print(f"  {name:<22s} {n:>5d} -> {path.name}")

    return results
