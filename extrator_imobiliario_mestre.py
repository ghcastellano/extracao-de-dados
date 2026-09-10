#!/usr/bin/env python3
"""
Extrator Imobiliário Mestre — dados territoriais públicos
=========================================================

Cidades suportadas:
  - Itanhaém: camada vetorial pública (TileJSON/PBF) de ARRUAMENTO e ortofoto.
    A camada confirmada contém vias, bairros, loteamentos e restrições urbanísticas;
    ela NÃO contém polígonos de lotes nem inscrições imobiliárias.
  - Mogi das Cruzes: CSV público "Cadastro Imobiliário" no portal CKAN municipal.
    A georreferência pública disponível é a camada de quadras do GeoMogi; cada
    cadastro é associado ao centro da respectiva quadra, nunca a um lote inferido.

O que este programa gera:
  - SQLite de checkpoint: permite parar e retomar sem baixar/processar novamente.
  - JSON com atributos públicos normalizados.
  - GeoJSON para consumo em QGIS, ArcGIS, Mapbox e similares.
  - Importação local de valores venais de certidões emitidas legitimamente pelo usuário.

Escopo intencional:
  - Não coleta CPF de pessoas físicas, nome de proprietário, boletos, código de barras,
    vencimentos, pagamentos ou outros dados tributários/pessoais não necessários ao
    cadastro territorial público.
  - A importação venal rejeita arquivos que contenham campos pessoais, de boleto ou
    de pagamento; ela não consulta nem automatiza os portais de certidão.
  - Integrações autorizadas podem ser tratadas separadamente, com credenciais e base
    legal documentadas pelo titular/órgão responsável.

Dependências:
  pip install requests mapbox-vector-tile

"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import re
import sqlite3
import sys
import time
import unicodedata
from datetime import datetime, timezone
from http.cookiejar import MozillaCookieJar
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import mapbox_vector_tile
except ImportError:  # só necessário no modo Itanhaém
    mapbox_vector_tile = None


APP_NAME = "ExtratorImobiliarioMestre/1.1"

ITANHAEM_TILEJSON = (
    "https://www2.itanhaem.sp.gov.br/ortofoto2012/mapa/"
    "tileserver.php?/arruamento.json"
)

MOGI_PACKAGE_API = (
    "https://dados.mogidascruzes.sp.gov.br/api/3/action/"
    "package_show?id=cadastro-imobiliario"
)

MOGI_QUADRAS_API = "https://geomogi.mogidascruzes.sp.gov.br/mapa/quadra"
MOGI_LOGRADOUROS_API = "https://geomogi.mogidascruzes.sp.gov.br/mapa/logradouro"


# -------------------------- Camada de persistência --------------------------

class CheckpointStore:
    """Mantém o estado da coleta em SQLite para que a execução possa ser retomada."""

    def __init__(self, path: Path) -> None:
        """Abre o banco local e prepara as tabelas usadas durante a coleta."""
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    def _create_schema(self) -> None:
        """Cria as tabelas e índices necessários sem alterar registros já coletados."""
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS resources (
                source TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                status TEXT NOT NULL,
                http_status INTEGER,
                error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source, resource_id)
            );

            CREATE TABLE IF NOT EXISTS records (
                source TEXT NOT NULL,
                record_id TEXT NOT NULL,
                properties_json TEXT NOT NULL,
                geometry_json TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (source, record_id)
            );

            CREATE INDEX IF NOT EXISTS idx_records_source ON records(source);

            CREATE TABLE IF NOT EXISTS venal_validations (
                cidade TEXT NOT NULL,
                inscricao_normalizada TEXT NOT NULL,
                inscricao_imobiliaria TEXT NOT NULL,
                exercicio INTEGER NOT NULL,
                valor_venal REAL NOT NULL,
                moeda TEXT NOT NULL DEFAULT 'BRL',
                numero_certidao TEXT,
                data_emissao TEXT,
                arquivo_origem TEXT NOT NULL,
                linha_origem INTEGER,
                imported_at TEXT NOT NULL,
                PRIMARY KEY (cidade, inscricao_normalizada, exercicio)
            );

            CREATE INDEX IF NOT EXISTS idx_venal_validations_inscricao
            ON venal_validations(cidade, inscricao_normalizada);
            """
        )
        self.conn.commit()

    @staticmethod
    def now() -> str:
        """Gera o horário UTC usado nos campos de auditoria do banco local."""
        return datetime.now(timezone.utc).isoformat()

    def set_state(self, key: str, value: Any) -> None:
        """Salva um marcador de execução que pode ser consultado em uma retomada."""
        self.conn.execute(
            """
            INSERT INTO state(key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, json.dumps(value, ensure_ascii=False), self.now()),
        )
        self.conn.commit()

    def get_state(self, key: str, default: Any = None) -> Any:
        """Recupera um marcador salvo; devolve o valor padrão quando ele ainda não existe."""
        row = self.conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default

    def resource_done(self, source: str, resource_id: str) -> bool:
        """Indica se um recurso de uma fonte já foi processado com sucesso."""
        row = self.conn.execute(
            "SELECT status FROM resources WHERE source=? AND resource_id=?",
            (source, resource_id),
        ).fetchone()
        return bool(row and row["status"] == "done")

    def mark_resource(
        self,
        source: str,
        resource_id: str,
        status: str,
        http_status: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """Registra o resultado de uma tentativa de leitura de recurso externo."""
        self.conn.execute(
            """
            INSERT INTO resources(source, resource_id, status, http_status, error, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, resource_id) DO UPDATE SET
                status=excluded.status, http_status=excluded.http_status,
                error=excluded.error, updated_at=excluded.updated_at
            """,
            (source, resource_id, status, http_status, error, self.now()),
        )
        self.conn.commit()

    def upsert_records(self, source: str, rows: Iterable[Dict[str, Any]]) -> int:
        """Grava ou atualiza um lote de feições normalizadas sem duplicar a chave da fonte."""
        prepared = []
        stamp = self.now()
        for row in rows:
            props = row["properties"]
            record_id = str(row["record_id"])
            prepared.append(
                (
                    source,
                    record_id,
                    json.dumps(props, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(row.get("geometry"), ensure_ascii=False, separators=(",", ":"))
                    if row.get("geometry") is not None
                    else None,
                    stamp,
                )
            )
        if not prepared:
            return 0
        self.conn.executemany(
            """
            INSERT INTO records(source, record_id, properties_json, geometry_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source, record_id) DO UPDATE SET
                properties_json=excluded.properties_json,
                geometry_json=excluded.geometry_json,
                updated_at=excluded.updated_at
            """,
            prepared,
        )
        self.conn.commit()
        return len(prepared)

    def iter_records(self, source: Optional[str] = None) -> Iterator[sqlite3.Row]:
        """Percorre os registros guardados, opcionalmente filtrando por uma fonte."""
        sql = "SELECT source, record_id, properties_json, geometry_json, updated_at FROM records"
        args: Tuple[Any, ...] = ()
        if source:
            sql += " WHERE source=?"
            args = (source,)
        sql += " ORDER BY source, record_id"
        yield from self.conn.execute(sql, args)

    def summary(self) -> Dict[str, int]:
        """Resume a quantidade de registros disponíveis em cada fonte coletada."""
        rows = self.conn.execute(
            "SELECT source, COUNT(*) AS n FROM records GROUP BY source ORDER BY source"
        ).fetchall()
        return {row["source"]: row["n"] for row in rows}

    def close(self) -> None:
        """Fecha a conexão com SQLite ao final da execução."""
        self.conn.close()

    def upsert_venal_validations(self, rows: Iterable[Dict[str, Any]]) -> int:
        """Armazena valores venais importados sem aceitar dados pessoais no modelo."""
        prepared = []
        stamp = self.now()
        for row in rows:
            prepared.append(
                (
                    row["cidade"],
                    row["inscricao_normalizada"],
                    row["inscricao_imobiliaria"],
                    row["exercicio"],
                    row["valor_venal"],
                    row.get("moeda", "BRL"),
                    row.get("numero_certidao"),
                    row.get("data_emissao"),
                    row["arquivo_origem"],
                    row.get("linha_origem"),
                    stamp,
                )
            )
        if not prepared:
            return 0
        self.conn.executemany(
            """
            INSERT INTO venal_validations(
                cidade, inscricao_normalizada, inscricao_imobiliaria, exercicio,
                valor_venal, moeda, numero_certidao, data_emissao, arquivo_origem,
                linha_origem, imported_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cidade, inscricao_normalizada, exercicio) DO UPDATE SET
                inscricao_imobiliaria=excluded.inscricao_imobiliaria,
                valor_venal=excluded.valor_venal,
                moeda=excluded.moeda,
                numero_certidao=excluded.numero_certidao,
                data_emissao=excluded.data_emissao,
                arquivo_origem=excluded.arquivo_origem,
                linha_origem=excluded.linha_origem,
                imported_at=excluded.imported_at
            """,
            prepared,
        )
        self.conn.commit()
        return len(prepared)

    def iter_venal_validations(self) -> Iterator[sqlite3.Row]:
        """Percorre as validações venais importadas para vínculo e exportação."""
        yield from self.conn.execute(
            """
            SELECT cidade, inscricao_normalizada, inscricao_imobiliaria, exercicio,
                   valor_venal, moeda, numero_certidao, data_emissao, arquivo_origem,
                   linha_origem, imported_at
            FROM venal_validations
            ORDER BY cidade, inscricao_imobiliaria, exercicio
            """
        )


# -------------------------- HTTP persistente --------------------------

class PersistentHttpClient:
    """Centraliza requisições públicas com limite de ritmo, tentativas e cookies locais."""

    def __init__(
        self,
        cookie_path: Path,
        timeout: int = 45,
        min_interval: float = 0.2,
    ) -> None:
        """Configura uma sessão HTTP previsível para as fontes públicas do projeto."""
        self.timeout = timeout
        self.min_interval = min_interval
        self._last_request = 0.0

        self.session = requests.Session()
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "User-Agent": APP_NAME,
                "Accept": "application/json,text/plain,*/*",
            }
        )

        self.cookie_path = cookie_path
        jar = MozillaCookieJar(str(cookie_path))
        if cookie_path.exists():
            try:
                jar.load(ignore_discard=True, ignore_expires=True)
            except Exception as exc:
                logging.warning("Não foi possível carregar cookies persistidos: %s", exc)
        self.session.cookies = jar

    def _wait(self) -> None:
        """Respeita o intervalo mínimo definido entre duas chamadas consecutivas."""
        remaining = self.min_interval - (time.monotonic() - self._last_request)
        if remaining > 0:
            time.sleep(remaining)

    def _save_cookies(self) -> None:
        """Persiste cookies técnicos para permitir a retomada da mesma sessão pública."""
        try:
            self.session.cookies.save(ignore_discard=True, ignore_expires=True)
        except Exception as exc:
            logging.warning("Não foi possível persistir cookies: %s", exc)

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """Executa uma requisição aplicando timeout, limite de ritmo e persistência de cookies."""
        self._wait()
        kwargs.setdefault("timeout", self.timeout)
        response = self.session.request(method.upper(), url, **kwargs)
        self._last_request = time.monotonic()
        self._save_cookies()
        return response

    def get_json(self, url: str, **kwargs: Any) -> Dict[str, Any]:
        """Busca um endpoint público e valida que a resposta HTTP foi bem-sucedida."""
        response = self.request("GET", url, **kwargs)
        response.raise_for_status()
        return response.json()

# -------------------------- Funções geoespaciais --------------------------

def tile_range_for_bounds(bounds: Sequence[float], zoom: int) -> Tuple[int, int, int, int]:
    """Calcula a grade XYZ que cobre os limites geográficos informados."""
    west, south, east, north = map(float, bounds)
    n = 2 ** zoom

    def x_from_lon(lon: float) -> int:
        """Converte longitude em coluna XYZ, limitada à grade válida do zoom."""
        return max(0, min(n - 1, int((lon + 180.0) / 360.0 * n)))

    def y_from_lat(lat: float) -> int:
        """Converte latitude em linha XYZ, respeitando o limite do Web Mercator."""
        lat = max(min(lat, 85.05112878), -85.05112878)
        y = (1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n
        return max(0, min(n - 1, int(y)))

    xmin, xmax = sorted((x_from_lon(west), x_from_lon(east)))
    ymin, ymax = sorted((y_from_lat(north), y_from_lat(south)))
    return xmin, xmax, ymin, ymax


def tile_coord_to_lonlat(px: float, py: float, x: int, y: int, z: int, extent: int) -> List[float]:
    """Converte um ponto local de tile vetorial para longitude e latitude WGS84."""
    n = 2 ** z
    gx = (x + (px / extent)) / n
    gy = (y + (py / extent)) / n
    lon = gx * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * gy))))
    return [round(lon, 7), round(lat, 7)]


def transform_tile_coordinates(coords: Any, x: int, y: int, z: int, extent: int) -> Any:
    """Transforma coordenadas locais de PBF para coordenadas WGS84 GeoJSON."""
    if isinstance(coords, (list, tuple)):
        if len(coords) >= 2 and isinstance(coords[0], (int, float)) and isinstance(coords[1], (int, float)):
            return tile_coord_to_lonlat(float(coords[0]), float(coords[1]), x, y, z, extent)
        return [transform_tile_coordinates(c, x, y, z, extent) for c in coords]
    return coords


def safe_text(value: Any) -> Optional[str]:
    """Converte um valor textual em texto limpo ou retorna nulo quando ele está vazio."""
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def safe_float(value: Any) -> Optional[float]:
    """Converte números vindos de CSV, aceitando separadores brasileiros e decimais simples."""
    return parse_brl_number(value)


def geometry_bbox(geometry: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    """Calcula o envelope WGS84 de uma geometria GeoJSON sem dependências GIS extras."""
    coords = geometry.get("coordinates")
    points: List[Tuple[float, float]] = []

    def visit(value: Any) -> None:
        """Visita recursivamente os níveis de coordenadas de uma geometria GeoJSON."""
        if isinstance(value, (list, tuple)):
            if len(value) >= 2 and all(isinstance(v, (int, float)) for v in value[:2]):
                points.append((float(value[0]), float(value[1])))
            else:
                for child in value:
                    visit(child)

    visit(coords)
    if not points:
        return None
    xs, ys = zip(*points)
    return min(xs), min(ys), max(xs), max(ys)


def quadra_key_from_inscricao(inscricao: Any) -> Optional[str]:
    """Converte 01-000451-029 para a chave pública de quadra 01004 do GeoMogi."""
    if not inscricao:
        return None
    parts = re.findall(r"\d+", str(inscricao))
    if len(parts) < 2 or len(parts[0]) != 2 or len(parts[1]) < 4:
        return None
    setor = parts[0]
    quadra = str(int(parts[1][:4])).zfill(3)
    return f"{setor}{quadra}"


def normalize_quadra_key(value: Any) -> Optional[str]:
    """Padroniza a chave de quadra pública em cinco dígitos para os cruzamentos do GeoMogi."""
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return None
    return digits.zfill(5)


def logradouro_key(value: Any) -> Optional[str]:
    """Normaliza nomes públicos de vias para associação sem sensibilidade a acento."""
    text = str(value or "").strip().upper()
    if not text:
        return None
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("ASCII")
    text = re.split(r"\s+-\s+\d{5}-?\d{3}\s*$", text)[0]
    text = re.sub(r"^(RUA|R)\s+", "R ", text)
    text = re.sub(r"^(AVENIDA|AV)\s+", "AV ", text)
    text = re.sub(r"^(ESTRADA|EST)\s+", "EST ", text)
    text = re.sub(r"^(RODOVIA|ROD)\s+", "ROD ", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip() or None


# -------------------------- Extrator de Itanhaém --------------------------

class ItanhaemPublicExtractor:
    """Coleta somente a camada pública de arruamento disponibilizada por Itanhaém."""

    source = "itanhaem_arruamento_publico"

    def __init__(self, client: PersistentHttpClient, store: CheckpointStore) -> None:
        """Recebe os serviços compartilhados de HTTP e checkpoint usados pela fonte."""
        self.client = client
        self.store = store

    def fetch_metadata(self) -> Dict[str, Any]:
        """Lê o TileJSON oficial e guarda seus metadados para auditoria da coleta."""
        metadata = self.client.get_json(ITANHAEM_TILEJSON)
        required = ("tiles", "bounds", "minzoom", "maxzoom")
        missing = [key for key in required if key not in metadata]
        if missing:
            raise ValueError(f"TileJSON incompleto: campos ausentes {missing}")
        self.store.set_state("itanhaem_tilejson", metadata)
        return metadata

    @staticmethod
    def feature_id(props: Dict[str, Any], geometry: Dict[str, Any]) -> str:
        """Usa o identificador da feição ou um hash estável quando a fonte não o fornece."""
        gid = props.get("gid")
        if gid is not None:
            return f"gid:{gid}"
        raw = json.dumps({"p": props, "g": geometry}, ensure_ascii=False, sort_keys=True)
        return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def process_tile(self, template: str, z: int, x: int, y: int) -> int:
        """Baixa um tile, converte sua geometria para WGS84 e registra as feições válidas."""
        resource_id = f"{z}/{x}/{y}"
        if self.store.resource_done(self.source, resource_id):
            return 0

        url = template.format(z=z, x=x, y=y)
        try:
            response = self.client.request("GET", url, headers={"Accept": "application/x-protobuf,*/*"})
            if response.status_code == 404:
                self.store.mark_resource(self.source, resource_id, "done", 404)
                return 0
            response.raise_for_status()
            if not response.content:
                self.store.mark_resource(self.source, resource_id, "done", response.status_code)
                return 0
            if mapbox_vector_tile is None:
                raise RuntimeError("Dependência ausente: execute 'pip install mapbox-vector-tile'.")

            decoded = mapbox_vector_tile.decode(response.content)
            records: List[Dict[str, Any]] = []
            for layer_name, layer in decoded.items():
                extent = int(layer.get("extent", 4096))
                for feature in layer.get("features", []):
                    props = feature.get("properties") or {}
                    geometry = feature.get("geometry")
                    if not geometry:
                        continue
                    geo = {
                        "type": geometry.get("type"),
                        "coordinates": transform_tile_coordinates(
                            geometry.get("coordinates"), x, y, z, extent
                        ),
                    }
                    normalized = {
                        "cidade": "Itanhaém",
                        "fonte": "TileServer público - camada arruamento",
                        "camada": layer_name,
                        "gid": props.get("gid"),
                        "tipo": safe_text(props.get("Tipo")),
                        "descricao": safe_text(props.get("Descrição") or props.get("Descriu00e7u00e3o")),
                        "bairro": safe_text(props.get("Bairro")),
                        "loteamento": safe_text(props.get("Loteamento")),
                        "lei_corredor": safe_text(props.get("Lei Corredor ")),
                        "restricao": safe_text(props.get("Restrição") or props.get("Restriu00e7u00e3o")),
                        "tile": {"z": z, "x": x, "y": y},
                    }
                    records.append(
                        {
                            "record_id": self.feature_id(props, geo),
                            "properties": normalized,
                            "geometry": geo,
                        }
                    )
            count = self.store.upsert_records(self.source, records)
            self.store.mark_resource(self.source, resource_id, "done", response.status_code)
            return count
        except Exception as exc:
            logging.exception("Falha no tile %s", resource_id)
            self.store.mark_resource(self.source, resource_id, "error", error=str(exc))
            return 0

    def run(
        self,
        limit_tiles: Optional[int] = None,
        force: bool = False,
        only_tile: Optional[Tuple[int, int, int]] = None,
    ) -> Dict[str, int]:
        """Percorre a grade oficial de tiles, permitindo retomada, recorte e reprocessamento."""
        metadata = self.fetch_metadata()
        template = metadata["tiles"][0]
        z = int(metadata["maxzoom"])
        xmin, xmax, ymin, ymax = tile_range_for_bounds(metadata["bounds"], z)
        total = (xmax - xmin + 1) * (ymax - ymin + 1)
        logging.info("Itanhaém: zoom=%s, grade=%s tiles, X=%s..%s, Y=%s..%s", z, total, xmin, xmax, ymin, ymax)

        if only_tile is not None:
            tz, tx, ty = only_tile
            if tz != z:
                raise ValueError(f"O TileJSON utiliza zoom máximo {z}; o tile informado possui zoom {tz}.")
            tiles: Iterable[Tuple[int, int, int]] = [(tz, tx, ty)]
            total = 1
        else:
            tiles = ((z, x, y) for y in range(ymin, ymax + 1) for x in range(xmin, xmax + 1))

        processed = 0
        inserted = 0
        for tz, x, y in tiles:
            if limit_tiles is not None and processed >= limit_tiles:
                self.store.set_state("itanhaem_last_run", {"limited": True, "processed": processed})
                return {"tiles_processados": processed, "registros_gravados": inserted}
            resource_id = f"{tz}/{x}/{y}"
            if force and self.store.resource_done(self.source, resource_id):
                self.store.mark_resource(self.source, resource_id, "pending")
            inserted += self.process_tile(template, tz, x, y)
            processed += 1
            if processed % 25 == 0:
                logging.info("Itanhaém: %s/%s tiles verificados; %s registros lidos", processed, total, inserted)

        self.store.set_state("itanhaem_last_run", {"limited": False, "processed": processed})
        return {"tiles_processados": processed, "registros_gravados": inserted}


# -------------------------- Extratores de Mogi --------------------------


class MogiQuadrasExtractor:
    """Coleta as quadras públicas do GeoMogi.

    A geometria individual de lote é restrita pela Prefeitura. As quadras são
    públicas e permitem localizar o cadastro aberto no nível territorial correto.
    """

    source = "mogi_quadras_publicas"

    def __init__(self, client: PersistentHttpClient, store: CheckpointStore) -> None:
        """Recebe os serviços compartilhados usados para coletar as quadras públicas."""
        self.client = client
        self.store = store

    def run(self, force: bool = False) -> Dict[str, int]:
        """Baixa e valida os polígonos públicos de quadra antes de gravá-los no checkpoint."""
        resource_id = "geomogi/quadra"
        if self.store.resource_done(self.source, resource_id) and not force:
            return {"quadras_processadas": 0, "registros_gravados": 0}

        response = self.client.request("GET", MOGI_QUADRAS_API, headers={"Accept": "application/json"})
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("A API pública do GeoMogi retornou uma resposta inesperada para quadras.")

        rows: List[Dict[str, Any]] = []
        ignored = 0
        for item in payload:
            try:
                geometry = json.loads(item["poligono"])
                if geometry.get("type") not in {"Polygon", "MultiPolygon"} or not geometry_bbox(geometry):
                    raise ValueError("geometria ausente ou inválida")
                quadra = normalize_quadra_key(item.get("texto"))
                if not quadra:
                    raise ValueError("código de quadra ausente")
                rows.append(
                    {
                        "record_id": f"quadra:{item['id']}",
                        "properties": {
                            "cidade": "Mogi das Cruzes",
                            "fonte": "GeoMogi - camada pública de quadras",
                            "camada": "quadra",
                            "quadra_chave": quadra,
                            "quadra_codigo_fonte": safe_text(item.get("texto")),
                            "quadra_id_fonte": item.get("id"),
                            "precisao_geometria": "quadra",
                        },
                        "geometry": geometry,
                    }
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                ignored += 1
                logging.debug("GeoMogi: quadra ignorada: %s", exc)

        inserted = self.store.upsert_records(self.source, rows)
        self.store.mark_resource(self.source, resource_id, "done", response.status_code)
        self.store.set_state(
            "mogi_quadras_last_run",
            {"processadas": len(payload), "gravadas": inserted, "ignoradas": ignored, "url": MOGI_QUADRAS_API},
        )
        return {"quadras_processadas": len(payload), "registros_gravados": inserted, "ignoradas": ignored}


class MogiLogradourosExtractor:
    """Coleta os eixos públicos de logradouro do GeoMogi para fallback territorial."""

    source = "mogi_logradouros_publicos"

    def __init__(self, client: PersistentHttpClient, store: CheckpointStore) -> None:
        """Recebe os serviços compartilhados usados para coletar os logradouros públicos."""
        self.client = client
        self.store = store

    def run(self, force: bool = False) -> Dict[str, int]:
        """Baixa os eixos de logradouro que servem como fallback de localização em Mogi."""
        resource_id = "geomogi/logradouro"
        if self.store.resource_done(self.source, resource_id) and not force:
            return {"logradouros_processados": 0, "registros_gravados": 0}

        response = self.client.request("GET", MOGI_LOGRADOUROS_API, headers={"Accept": "application/json"})
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("A API pública do GeoMogi retornou uma resposta inesperada para logradouros.")

        rows: List[Dict[str, Any]] = []
        ignored = 0
        for item in payload:
            try:
                geometry = json.loads(item["linha"])
                if geometry.get("type") not in {"LineString", "MultiLineString"} or not geometry_bbox(geometry):
                    raise ValueError("geometria ausente ou inválida")
                nome = safe_text(item.get("texto"))
                chave = logradouro_key(nome)
                if not chave:
                    raise ValueError("nome de logradouro ausente")
                rows.append(
                    {
                        "record_id": f"logradouro:{item['id']}",
                        "properties": {
                            "cidade": "Mogi das Cruzes",
                            "fonte": "GeoMogi - camada pública de logradouros",
                            "camada": "logradouro",
                            "logradouro": nome,
                            "logradouro_chave": chave,
                            "cep": safe_text(item.get("cep")),
                            "logradouro_id_fonte": item.get("id"),
                            "precisao_geometria": "eixo_do_logradouro",
                        },
                        "geometry": geometry,
                    }
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                ignored += 1
                logging.debug("GeoMogi: logradouro ignorado: %s", exc)

        inserted = self.store.upsert_records(self.source, rows)
        self.store.mark_resource(self.source, resource_id, "done", response.status_code)
        self.store.set_state(
            "mogi_logradouros_last_run",
            {"processados": len(payload), "gravados": inserted, "ignorados": ignored, "url": MOGI_LOGRADOUROS_API},
        )
        return {"logradouros_processados": len(payload), "registros_gravados": inserted, "ignoradas": ignored}


class MogiPublicExtractor:
    """Coleta e normaliza o cadastro imobiliário publicado no portal de dados abertos."""

    source = "mogi_cadastro_imobiliario_publico"

    def __init__(self, client: PersistentHttpClient, store: CheckpointStore) -> None:
        """Recebe os serviços compartilhados usados para baixar os CSVs públicos de Mogi."""
        self.client = client
        self.store = store

    def find_resources(self, year: int, full: bool = False) -> List[Dict[str, Any]]:
        """Descobre no CKAN os arquivos do exercício, sem depender de URLs fixas no código."""
        response = self.client.get_json(MOGI_PACKAGE_API)
        if not response.get("success"):
            raise RuntimeError("A API CKAN retornou success=false")
        resources = response["result"]["resources"]

        if full:
            pattern = re.compile(rf"^pmmc_imobiliario_{year}_part(\d+)\.csv$", re.IGNORECASE)
            parts = []
            for resource in resources:
                match = pattern.match(str(resource.get("name", "")))
                if match and str(resource.get("format", "")).upper() == "CSV":
                    parts.append((int(match.group(1)), resource))
            if not parts:
                raise LookupError(f"Nenhuma parte pmmc_imobiliario_{year}_partN.csv encontrada na API CKAN.")
            parts.sort(key=lambda item: item[0])
            selected = [resource for _, resource in parts]
            self.store.set_state(f"mogi_resources_{year}_full", selected)
            return selected

        candidates = [
            r for r in resources
            if str(r.get("format", "")).upper() == "CSV"
            and str(year) in (r.get("name", "") + " " + r.get("description", ""))
            and "IPTU" in (r.get("name", "") + " " + r.get("description", "")).upper()
        ]
        if not candidates:
            raise LookupError(f"Nenhum CSV de IPTU encontrado para o ano {year}.")
        candidates.sort(key=lambda r: ("part" in r.get("name", "").lower(), int(r.get("size") or 0)))
        resource = candidates[0]
        self.store.set_state(f"mogi_resource_{year}", resource)
        return [resource]

    @staticmethod
    def clean_row(row: Dict[str, str], year: int) -> Dict[str, Any]:
        """Seleciona somente campos públicos cadastrais, territoriais e de valor venal."""
        get = lambda *keys: next((row.get(k) for k in keys if row.get(k) not in (None, "")), None)
        inscricao = safe_text(get("local", "id_local"))
        return {
            "cidade": "Mogi das Cruzes",
            "fonte": "Portal de Dados Abertos - Cadastro Imobiliário",
            "exercicio": safe_text(get("exercicio")) or str(year),
            "tipo": safe_text(get("tipo", "classe_fiscal")),
            "data_transacao": safe_text(get("data_instr", "dt_lancamento")),
            "uso_imovel": safe_text(get("uso_imovel")),
            "inscricao_imobiliaria": inscricao,
            "numero_imovel_quadra": safe_text(get("nro_imov_qda", "nro_local")),
            "complemento": safe_text(get("complemento")),
            "logradouro": safe_text(get("logradouro")),
            "bairro": safe_text(get("bairro")),
            "area_terreno_m2": safe_float(get("area_terreno")),
            "area_construcao_m2": safe_float(get("area_construcao", "area1")),
            "valor_venal": safe_float(get("valor_venal")),
            "moeda": safe_text(get("moeda")),
            "quadra_chave": quadra_key_from_inscricao(inscricao),
            "precisao_geometria": "sem_geometria",
        }

    @staticmethod
    def record_id(props: Dict[str, Any], ordinal: int) -> str:
        """Cria uma chave determinística para permitir atualização idempotente do cadastro."""
        base = props.get("inscricao_imobiliaria") or props.get("numero_imovel_quadra") or str(ordinal)
        digest = hashlib.sha1(json.dumps(props, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:12]
        return f"{base}:{digest}"

    def run(
        self,
        year: int,
        force: bool = False,
        full: bool = False,
        limit_resources: Optional[int] = None,
    ) -> Dict[str, int]:
        """Processa os CSVs em streaming e grava lotes pequenos para conter o uso de memória."""
        resources = self.find_resources(year, full=full)
        processed = 0
        inserted = 0
        completed = 0

        for resource in resources:
            if limit_resources is not None and completed >= limit_resources:
                break
            resource_id = resource["id"]
            if self.store.resource_done(self.source, resource_id) and not force:
                logging.info("Mogi: recurso %s já está processado; use --force para reprocessar.", resource_id)
                completed += 1
                continue

            logging.info("Mogi: baixando %s", resource["url"])
            response = self.client.request("GET", resource["url"], stream=True, headers={"Accept": "text/csv,*/*"})
            response.raise_for_status()
            lines = (line.decode("latin-1") for line in response.iter_lines(decode_unicode=False))
            reader = csv.DictReader(lines, delimiter=";")
            batch: List[Dict[str, Any]] = []

            for ordinal, raw in enumerate(reader, start=1):
                props = self.clean_row(raw, year)
                if not props.get("inscricao_imobiliaria"):
                    continue
                batch.append({"record_id": self.record_id(props, ordinal), "properties": props, "geometry": None})
                processed += 1
                if len(batch) >= 1000:
                    inserted += self.store.upsert_records(self.source, batch)
                    batch.clear()
                    logging.info("Mogi: %s registros processados", processed)

            inserted += self.store.upsert_records(self.source, batch)
            self.store.mark_resource(self.source, resource_id, "done", response.status_code)
            completed += 1

        self.store.set_state(
            "mogi_last_run",
            {"year": year, "full": full, "processed": processed, "resources": completed, "total_resources": len(resources)},
        )
        return {"linhas_processadas": processed, "registros_gravados": inserted, "recursos_processados": completed}


def georeference_mogi_records(store: CheckpointStore) -> Dict[str, int]:
    """Associa cada cadastro de Mogi à quadra pública ou, em último caso, ao logradouro."""
    """Atribui quadra pública ou, em último caso, o eixo do logradouro público."""
    quadras: Dict[str, List[Dict[str, Any]]] = {}
    for row in store.iter_records(MogiQuadrasExtractor.source):
        props = json.loads(row["properties_json"])
        geometry = json.loads(row["geometry_json"])
        key = normalize_quadra_key(props.get("quadra_chave"))
        if key:
            quadras.setdefault(key, []).append(geometry)

    logradouros: Dict[str, List[Dict[str, Any]]] = {}
    for row in store.iter_records(MogiLogradourosExtractor.source):
        props = json.loads(row["properties_json"])
        geometry = json.loads(row["geometry_json"])
        key = logradouro_key(props.get("logradouro_chave") or props.get("logradouro"))
        if key:
            logradouros.setdefault(key, []).append(geometry)

    batch: List[Dict[str, Any]] = []
    matched = 0
    fallback_logradouro = 0
    unmatched = 0
    invalid = 0
    for row in store.iter_records(MogiPublicExtractor.source):
        props = json.loads(row["properties_json"])
        geometries = quadras.get(normalize_quadra_key(props.get("quadra_chave")) or "", [])
        geometry: Optional[Dict[str, Any]] = None
        boxes = [geometry_bbox(candidate) for candidate in geometries]
        boxes = [box for box in boxes if box is not None]
        if boxes:
            minx = min(box[0] for box in boxes)
            miny = min(box[1] for box in boxes)
            maxx = max(box[2] for box in boxes)
            maxy = max(box[3] for box in boxes)
            geometry = {"type": "Point", "coordinates": [round((minx + maxx) / 2, 7), round((miny + maxy) / 2, 7)]}
            props["precisao_geometria"] = "centro_da_quadra"
            props["metodo_georreferenciamento"] = "chave_cadastral_para_quadra_publica"
            props["quadras_geometricas_associadas"] = len(boxes)
            matched += 1
        else:
            geometries = logradouros.get(logradouro_key(props.get("logradouro")) or "", [])
            boxes = [geometry_bbox(candidate) for candidate in geometries]
            boxes = [box for box in boxes if box is not None]
        if geometry is None and boxes:
            minx = min(box[0] for box in boxes)
            miny = min(box[1] for box in boxes)
            maxx = max(box[2] for box in boxes)
            maxy = max(box[3] for box in boxes)
            geometry = {"type": "Point", "coordinates": [round((minx + maxx) / 2, 7), round((miny + maxy) / 2, 7)]}
            props["precisao_geometria"] = "centro_do_logradouro"
            props["metodo_georreferenciamento"] = "logradouro_publico_normalizado"
            props["logradouros_geometricos_associados"] = len(boxes)
            fallback_logradouro += 1
        elif geometry is None:
            props["precisao_geometria"] = "sem_geometria"
            props["metodo_georreferenciamento"] = "quadra_e_logradouro_publicos_nao_encontrados"
            if geometries:
                invalid += 1
            else:
                unmatched += 1

        batch.append({"record_id": row["record_id"], "properties": props, "geometry": geometry})
        if len(batch) >= 1000:
            store.upsert_records(MogiPublicExtractor.source, batch)
            batch.clear()
    if batch:
        store.upsert_records(MogiPublicExtractor.source, batch)

    result = {
        "georreferenciados_por_quadra": matched,
        "georreferenciados_por_logradouro": fallback_logradouro,
        "sem_geometria": unmatched,
        "geometrias_invalidas": invalid,
    }
    store.set_state("mogi_georreferenciamento", result)
    return result


# ---------------------- Importação local de valor venal ---------------------

VENAL_CITY_LABELS = {
    "mogi": "Mogi das Cruzes",
    "itanhaem": "Itanhaém",
}

VENAL_FIELD_ALIASES = {
    "inscricao_imobiliaria": ("inscricao_imobiliaria", "inscricao", "cadastro", "codigo_imovel", "codigo_contribuinte"),
    "exercicio": ("exercicio", "ano", "ano_exercicio"),
    "valor_venal": ("valor_venal", "valor_venal_total", "valor"),
    "numero_certidao": ("numero_certidao", "certidao", "id_certidao", "codigo_certidao"),
    "data_emissao": ("data_emissao", "emissao", "data_certidao"),
    "moeda": ("moeda", "currency"),
}

PII_OR_FINANCIAL_FIELD_MARKERS = (
    "cpf", "cnpj", "propriet", "titular", "contribuinte", "nome", "rg", "telefone",
    "celular", "email", "e_mail", "boleto", "codigo_barras", "pagamento", "vencimento",
    "parcela", "divida", "debito", "pix",
)


def normalized_field_name(value: Any) -> str:
    """Normaliza cabeçalhos de arquivos para aceitar variações de caixa, acento e separador."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", text.casefold()).strip("_")


def normalize_inscricao(value: Any) -> str:
    """Normaliza uma inscrição sem adivinhar dígitos nem alterar zeros à esquerda."""
    return re.sub(r"[^0-9a-zA-Z]", "", str(value or "")).upper()


def parse_brl_number(value: Any) -> Optional[float]:
    """Lê um número positivo em formatos brasileiros ou decimais sem perder a parte fracionária."""
    if value is None:
        return None
    text = str(value).strip().replace("R$", "").replace(" ", "")
    if not text:
        return None
    text = re.sub(r"[^0-9,.-]", "", text)
    if not text:
        return None
    last_comma = text.rfind(",")
    last_dot = text.rfind(".")
    if last_comma >= 0 and last_dot >= 0:
        decimal_mark = "," if last_comma > last_dot else "."
        thousand_mark = "." if decimal_mark == "," else ","
        text = text.replace(thousand_mark, "").replace(decimal_mark, ".")
    elif last_comma >= 0:
        text = text.replace(",", ".")
    elif text.count(".") > 1 or (last_dot >= 0 and len(text) - last_dot - 1 == 3):
        text = text.replace(".", "")
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _field_value(row: Dict[str, Any], field: str) -> Optional[Any]:
    """Encontra o primeiro campo preenchido entre os nomes aceitos para uma coluna venal."""
    normalized = {normalized_field_name(key): value for key, value in row.items()}
    for alias in VENAL_FIELD_ALIASES[field]:
        value = normalized.get(alias)
        if value is not None and str(value).strip():
            return value
    return None


def _validate_venal_headers(headers: Iterable[Any]) -> None:
    """Interrompe a importação quando o arquivo possui campos pessoais ou financeiros indevidos."""
    unsafe = []
    for header in headers:
        name = normalized_field_name(header)
        if any(marker in name for marker in PII_OR_FINANCIAL_FIELD_MARKERS):
            unsafe.append(str(header))
    if unsafe:
        fields = ", ".join(sorted(set(unsafe)))
        raise ValueError(
            "O arquivo de certidões contém campos pessoais, de boleto ou pagamento "
            f"não necessários e foi recusado: {fields}. Exporte somente inscrição, exercício, "
            "valor venal, número/data da certidão e moeda."
        )


def read_venal_rows(path: Path) -> List[Tuple[int, Dict[str, Any]]]:
    """Lê planilha local já autorizada; nenhuma chamada é feita ao portal municipal."""
    if not path.is_file():
        raise ValueError(f"Arquivo de certidões não encontrado: {path}")

    if path.suffix.casefold() == ".json":
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(raw, dict):
            for key in ("registros", "records", "certidoes", "certidoes_venais", "data"):
                if isinstance(raw.get(key), list):
                    raw = raw[key]
                    break
        if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
            raise ValueError("O JSON deve ser uma lista de objetos, ou conter uma lista em 'registros' ou 'certidoes'.")
        headers = [key for item in raw for key in item.keys()]
        _validate_venal_headers(headers)
        return list(enumerate(raw, start=1))

    if path.suffix.casefold() not in (".csv", ".txt"):
        raise ValueError("Use um arquivo CSV, TXT delimitado ou JSON para importar certidões venais.")

    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            with path.open("r", encoding=encoding, newline="") as fh:
                sample = fh.read(4096)
                fh.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
                except csv.Error:
                    dialect = csv.excel
                    dialect.delimiter = ";"
                reader = csv.DictReader(fh, dialect=dialect)
                if not reader.fieldnames:
                    raise ValueError("O CSV não possui cabeçalho.")
                _validate_venal_headers(reader.fieldnames)
                return [(line, dict(row)) for line, row in enumerate(reader, start=2)]
        except UnicodeDecodeError as exc:
            last_error = exc
    raise ValueError(f"Não foi possível ler o arquivo CSV: {last_error}")


def import_venal_validations(store: CheckpointStore, path: Path, city_key: str) -> Dict[str, int]:
    """Valida e importa uma lista local de valores venais para o município informado."""
    city = VENAL_CITY_LABELS[city_key]
    prepared: List[Dict[str, Any]] = []
    invalid = 0
    for line, row in read_venal_rows(path):
        inscricao = str(_field_value(row, "inscricao_imobiliaria") or "").strip()
        exercicio_raw = _field_value(row, "exercicio")
        valor = parse_brl_number(_field_value(row, "valor_venal"))
        try:
            exercicio = int(str(exercicio_raw).strip())
        except (TypeError, ValueError):
            exercicio = 0
        if not normalize_inscricao(inscricao) or not (1900 <= exercicio <= 2100) or valor is None:
            invalid += 1
            logging.warning("Certidão venal ignorada na linha %s: inscrição, exercício ou valor inválido.", line)
            continue
        moeda = str(_field_value(row, "moeda") or "BRL").strip().upper()
        if moeda not in ("BRL", "R$"):
            logging.warning("Certidão venal ignorada na linha %s: moeda não suportada.", line)
            invalid += 1
            continue
        prepared.append(
            {
                "cidade": city,
                "inscricao_normalizada": normalize_inscricao(inscricao),
                "inscricao_imobiliaria": inscricao,
                "exercicio": exercicio,
                "valor_venal": valor,
                "moeda": "BRL",
                "numero_certidao": str(_field_value(row, "numero_certidao") or "").strip() or None,
                "data_emissao": str(_field_value(row, "data_emissao") or "").strip() or None,
                "arquivo_origem": path.name,
                "linha_origem": line,
            }
        )

    imported = store.upsert_venal_validations(prepared)
    result = {"linhas_lidas": len(prepared) + invalid, "importadas": imported, "invalidas": invalid}
    store.set_state("venal_last_import", {"cidade": city, "arquivo": path.name, **result})
    return result


def public_cadastre_index(store: CheckpointStore) -> Dict[Tuple[str, str], List[str]]:
    """Indexa as inscrições públicas de Mogi para vincular certidões sem alterar a fonte original."""
    index: Dict[Tuple[str, str], List[str]] = {}
    for row in store.iter_records(MogiPublicExtractor.source):
        props = json.loads(row["properties_json"])
        city = str(props.get("cidade") or "").strip()
        inscricao = normalize_inscricao(props.get("inscricao_imobiliaria"))
        if city and inscricao:
            index.setdefault((city, inscricao), []).append(row["record_id"])
    return index


def apply_venal_validations(store: CheckpointStore) -> Dict[str, int]:
    """Anexa a certidão importada ao cadastro público correspondente, sem substituir a fonte original."""
    validations: Dict[Tuple[str, str], sqlite3.Row] = {}
    for row in store.iter_venal_validations():
        key = (row["cidade"], row["inscricao_normalizada"])
        previous = validations.get(key)
        if previous is None or row["exercicio"] > previous["exercicio"]:
            validations[key] = row
    if not validations:
        return {"validacoes": 0, "cadastros_vinculados": 0, "sem_cadastro_publico": 0}

    index = public_cadastre_index(store)
    batch: List[Dict[str, Any]] = []
    linked_ids = set()
    for row in store.iter_records(MogiPublicExtractor.source):
        props = json.loads(row["properties_json"])
        validation = validations.get((str(props.get("cidade") or "").strip(), normalize_inscricao(props.get("inscricao_imobiliaria"))))
        if validation is None:
            continue
        props["valor_venal_certidao"] = validation["valor_venal"]
        props["exercicio_certidao_venal"] = validation["exercicio"]
        props["moeda_certidao_venal"] = validation["moeda"]
        props["numero_certidao_venal"] = validation["numero_certidao"]
        props["data_emissao_certidao_venal"] = validation["data_emissao"]
        props["fonte_valor_venal_certidao"] = "certidao_importada_localmente"
        batch.append({"record_id": row["record_id"], "properties": props, "geometry": json.loads(row["geometry_json"]) if row["geometry_json"] else None})
        linked_ids.add((validation["cidade"], validation["inscricao_normalizada"]))
        if len(batch) >= 1000:
            store.upsert_records(MogiPublicExtractor.source, batch)
            batch.clear()
    if batch:
        store.upsert_records(MogiPublicExtractor.source, batch)

    unmatched = len([key for key in validations if key not in index])
    result = {"validacoes": len(validations), "cadastros_vinculados": len(linked_ids), "sem_cadastro_publico": unmatched}
    store.set_state("venal_vinculacao", result)
    return result


# -------------------------- Exportadores --------------------------

def export_json(store: CheckpointStore, output: Path) -> int:
    """Gera um JSON consolidado a partir do checkpoint, sem depender da execução da coleta."""
    count = 0
    with output.open("w", encoding="utf-8") as fh:
        fh.write("[\n")
        first = True
        for row in store.iter_records():
            record = json.loads(row["properties_json"])
            record["id"] = row["record_id"]
            record["fonte_id"] = row["source"]
            if row["geometry_json"]:
                record["geometry"] = json.loads(row["geometry_json"])
            if not first:
                fh.write(",\n")
            json.dump(record, fh, ensure_ascii=False, separators=(",", ":"))
            first = False
            count += 1
        fh.write("\n]\n")
    return count


def export_geojson(store: CheckpointStore, output: Path) -> int:
    """Gera GeoJSON apenas com registros que possuem referência espacial disponível."""
    count = 0
    with output.open("w", encoding="utf-8") as fh:
        fh.write('{"type":"FeatureCollection","features":[\n')
        first = True
        for row in store.iter_records():
            if not row["geometry_json"]:
                continue
            props = json.loads(row["properties_json"])
            props["id"] = row["record_id"]
            props["fonte_id"] = row["source"]
            feature = {"type": "Feature", "geometry": json.loads(row["geometry_json"]), "properties": props}
            if not first:
                fh.write(",\n")
            json.dump(feature, fh, ensure_ascii=False, separators=(",", ":"))
            first = False
            count += 1
        fh.write("\n]}\n")
    return count


def export_csv(store: CheckpointStore, output: Path) -> int:
    """Gera uma visão tabular dos atributos; a geometria permanece no arquivo GeoJSON."""
    fields = [
        "id", "fonte_id", "cidade", "fonte", "camada", "gid", "tipo", "descricao", "bairro",
        "loteamento", "lei_corredor", "restricao", "exercicio", "data_transacao", "uso_imovel", "inscricao_imobiliaria",
        "numero_imovel_quadra", "complemento", "logradouro", "area_terreno_m2", "area_construcao_m2",
        "valor_venal", "moeda", "quadra_chave", "precisao_geometria", "metodo_georreferenciamento",
        "valor_venal_certidao", "exercicio_certidao_venal", "moeda_certidao_venal",
        "numero_certidao_venal", "data_emissao_certidao_venal", "fonte_valor_venal_certidao",
    ]
    count = 0
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in store.iter_records():
            props = json.loads(row["properties_json"])
            props["id"] = row["record_id"]
            props["fonte_id"] = row["source"]
            writer.writerow(props)
            count += 1
    return count


def export_venal_validations_csv(store: CheckpointStore, output: Path) -> int:
    """Exporta somente metadados venais aceitos, incluindo o resultado do vínculo territorial."""
    fields = [
        "cidade", "inscricao_imobiliaria", "exercicio", "valor_venal", "moeda",
        "numero_certidao", "data_emissao", "arquivo_origem", "linha_origem",
        "cadastro_publico_encontrado", "registros_publicos_vinculados", "importado_em",
    ]
    public_index = public_cadastre_index(store)
    count = 0
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in store.iter_venal_validations():
            records = public_index.get((row["cidade"], row["inscricao_normalizada"]), [])
            writer.writerow(
                {
                    "cidade": row["cidade"],
                    "inscricao_imobiliaria": row["inscricao_imobiliaria"],
                    "exercicio": row["exercicio"],
                    "valor_venal": row["valor_venal"],
                    "moeda": row["moeda"],
                    "numero_certidao": row["numero_certidao"],
                    "data_emissao": row["data_emissao"],
                    "arquivo_origem": row["arquivo_origem"],
                    "linha_origem": row["linha_origem"],
                    "cadastro_publico_encontrado": bool(records),
                    "registros_publicos_vinculados": ";".join(records),
                    "importado_em": row["imported_at"],
                }
            )
            count += 1
    return count


# -------------------------- CLI --------------------------

def parse_args() -> argparse.Namespace:
    """Define a interface de linha de comando usada para as coletas e exportações."""
    parser = argparse.ArgumentParser(
        description="Extrai cadastro territorial público de Itanhaém e Mogi das Cruzes."
    )
    parser.add_argument(
        "cidade", choices=("itanhaem", "mogi", "ambos", "export", "importar-venal"),
        help="Fonte a processar ou apenas 'export' para regenerar arquivos do checkpoint.",
    )
    parser.add_argument("--year", type=int, default=2024, help="Exercício do CSV de Mogi (padrão: 2024).")
    parser.add_argument("--output-dir", type=Path, default=Path("data/output"), help="Diretório local das saídas.")
    parser.add_argument("--limit-tiles", type=int, default=None, help="Limita a quantidade de tiles de Itanhaém.")
    parser.add_argument("--tile", default=None, help="Processa somente um tile no formato z/x/y.")
    parser.add_argument(
        "--mogi-full",
        action="store_true",
        help="Coleta todas as partes pmmc_imobiliario_YYYY.csv do CKAN, em vez do CSV resumido.",
    )
    parser.add_argument(
        "--limit-mogi-resources",
        type=int,
        default=None,
        help="Limita o número de partes de Mogi processadas na execução.",
    )
    parser.add_argument(
        "--venal-file",
        type=Path,
        help="CSV/TXT ou JSON local com inscrições e valores venais de certidões já emitidas legitimamente.",
    )
    parser.add_argument(
        "--venal-cidade",
        choices=tuple(VENAL_CITY_LABELS),
        help="Município das certidões importadas; obrigatório em importar-venal.",
    )
    parser.add_argument("--force", action="store_true", help="Reprocessa recursos já concluídos.")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def main() -> int:
    """Orquestra a fonte escolhida, aplica vínculos venais e produz todas as saídas locais."""
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s | %(levelname)s | %(message)s")

    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    store = CheckpointStore(out / "checkpoint.sqlite")
    client = PersistentHttpClient(out / "session-cookies.txt")

    try:
        if args.cidade == "importar-venal":
            if not args.venal_file or not args.venal_cidade:
                raise ValueError("importar-venal exige --venal-file e --venal-cidade.")
            result = import_venal_validations(store, args.venal_file, args.venal_cidade)
            logging.info("Certidões venais importadas: %s", result)

        if args.cidade in ("itanhaem", "ambos"):
            only_tile = None
            if args.tile:
                try:
                    only_tile = tuple(int(part) for part in args.tile.split("/"))
                    if len(only_tile) != 3:
                        raise ValueError
                except ValueError:
                    raise ValueError("--tile deve estar no formato z/x/y.")
            result = ItanhaemPublicExtractor(client, store).run(args.limit_tiles, args.force, only_tile)
            logging.info("Itanhaém concluído: %s", result)

        if args.cidade in ("mogi", "ambos"):
            quadras = MogiQuadrasExtractor(client, store).run(args.force)
            logging.info("Mogi: quadras concluídas: %s", quadras)
            logradouros = MogiLogradourosExtractor(client, store).run(args.force)
            logging.info("Mogi: logradouros concluídos: %s", logradouros)
            result = MogiPublicExtractor(client, store).run(
                args.year,
                args.force,
                full=args.mogi_full,
                limit_resources=args.limit_mogi_resources,
            )
            logging.info("Mogi concluído: %s", result)
            georeference = georeference_mogi_records(store)
            logging.info("Mogi: georreferenciamento concluído: %s", georeference)

        venal_vinculacao = apply_venal_validations(store)
        if venal_vinculacao["validacoes"]:
            logging.info("Certidões venais vinculadas: %s", venal_vinculacao)

        # Sempre gera versões reconstruíveis a partir do SQLite.
        n_json = export_json(store, out / "cadastro_publico.json")
        n_geo = export_geojson(store, out / "cadastro_publico.geojson")
        n_csv = export_csv(store, out / "cadastro_publico.csv")
        n_venal = export_venal_validations_csv(store, out / "certidoes_venais_importadas.csv")
        manifest = {
            "gerado_em": datetime.now(timezone.utc).isoformat(),
            "resumo_por_fonte": store.summary(),
            "arquivos": {
                "json": "cadastro_publico.json",
                "geojson": "cadastro_publico.geojson",
                "csv": "cadastro_publico.csv",
                "certidoes_venais": "certidoes_venais_importadas.csv",
                "checkpoint": "checkpoint.sqlite",
            },
            "contagens": {"json": n_json, "geojson": n_geo, "csv": n_csv, "certidoes_venais": n_venal},
            "escopo": "Atributos públicos cadastrais, territoriais e geográficos; sem dados pessoais, boletos ou pagamentos. Em Itanhaém a fonte pública atual é arruamento, não lotes/inscrições. Em Mogi, os cadastros recebem ponto no centro da quadra pública e, sem quadra, no centro do eixo público de logradouro; a fonte pública não autoriza geometria individual de lote.",
        }
        (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        logging.info("Exportação concluída. %s", json.dumps(manifest["contagens"], ensure_ascii=False))
        return 0
    except KeyboardInterrupt:
        logging.warning("Execução interrompida. O checkpoint foi preservado; execute o mesmo comando para retomar.")
        return 130
    except Exception as exc:
        logging.exception("Execução interrompida por erro: %s", exc)
        return 1
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
