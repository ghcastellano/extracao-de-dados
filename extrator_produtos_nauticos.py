#!/usr/bin/env python3
"""
Extrator de Produtos Náuticos — catálogo público para tripulantes
=================================================================

Coleta catálogos públicos de lojas náuticas brasileiras com foco em produtos
úteis para tripulantes em escala curta no Porto de Santos (EPI, salvatagem,
vestuário de bordo, eletrônicos portáteis, itens de vida a bordo).

Fontes pré-configuradas (robots.txt verificado, coleta permitida ao agente `*`):
  - produtos-maritimos : produtosmaritimos.com.br  (plataforma Tray)
  - porto-nautico      : portonautico.com          (plataforma Tray)
  - keomar             : keomarnautica.com.br      (plataforma Tray)
  - r4nautic           : r4nautic.com              (WooCommerce)

O que este programa gera:
  - SQLite de checkpoint: permite parar e retomar sem baixar novamente.
  - JSON e CSV com nome, SKU, descrição, marca, GTIN, categoria, preço,
    disponibilidade e URLs das imagens de cada produto.
  - Download opcional das fotos para uso interno de catálogo.

Escopo intencional:
  - Só coleto páginas públicas de produto permitidas pelo robots.txt de cada
    loja, com intervalo mínimo entre requisições ao mesmo domínio.
  - A extração usa os dados estruturados (JSON-LD schema.org/Product) que as
    próprias lojas publicam para indexação; não acesso carrinho, checkout ou
    qualquer área autenticada.
  - Fotos e descrições permanecem de titularidade das lojas/fabricantes; o
    resultado serve para análise e curadoria interna, não para republicação.

Dependências:
  pip install requests

"""

from __future__ import annotations

import argparse
import csv
import html as html_module
import json
import logging
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.robotparser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


APP_NAME = "ExtratorProdutosNauticos/1.0"
USER_AGENT = (
    "Mozilla/5.0 (compatible; " + APP_NAME + "; coleta de catalogo publico)"
)

DEFAULT_DELAY_SECONDS = 1.5
DEFAULT_MAX_FOTOS = 5

# Palavras-chave de produtos que um tripulante em escala curta procura:
# proteção individual, vestuário de bordo, eletrônicos portáteis e vida a bordo.
TRIPULANTE_KEYWORDS = [
    "colete", "salva-vidas", "salva vidas", "salvatagem", "auto inflavel",
    "bota", "botina", "sapato", "sapatilha",
    "jaqueta", "japona", "capa de chuva", "conjunto de chuva", "impermeavel",
    "luva", "camiseta", "camisa", "bermuda", "calca", "bone", "chapeu",
    "oculos", "protetor solar", "protetor auricular",
    "lanterna", "farolete", "strobe", "apito", "espelho de sinalizacao",
    "faca", "canivete", "multiferramenta",
    "binoculo", "vhf portatil", "radio vhf", "radio portatil", "gps portatil",
    "epirb", "plb", "sart", "localizador",
    "mochila", "bolsa estanque", "saco estanque", "case estanque",
    "cinto de seguranca", "talabarte", "linha de vida",
    "beliche", "rede de descanso", "cadeado", "adaptador de tomada",
    "carregador", "power bank", "garrafa termica", "caneca",
]


def normalize_text(value: Any) -> str:
    """Remove acentos e baixa a caixa para comparação de palavras-chave."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip().lower()


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def strip_html(value: Any) -> Optional[str]:
    """Converte descrições HTML em texto simples normalizado."""
    if value is None:
        return None
    text = html_module.unescape(str(value))
    text = re.sub(r"<\s*(br|/p|/li|/div)\s*/?\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip() or None


def slugify(value: Any, fallback: str) -> str:
    text = normalize_text(value)
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:80] or fallback


# ----------------------------- Fontes suportadas -----------------------------

SITES: Dict[str, Dict[str, Any]] = {
    "produtos-maritimos": {
        "base": "https://www.produtosmaritimos.com.br",
        "sitemap": "https://www.produtosmaritimos.com.br/sitemap.xml",
        "plataforma": "tray",
    },
    "porto-nautico": {
        "base": "https://www.portonautico.com",
        "sitemap": "https://www.portonautico.com/sitemap.xml",
        "plataforma": "tray",
    },
    "keomar": {
        "base": "https://www.keomarnautica.com.br",
        "sitemap": "https://www.keomarnautica.com.br/sitemap.xml",
        "plataforma": "tray",
    },
    "r4nautic": {
        "base": "https://r4nautic.com",
        "sitemap": "https://r4nautic.com/sitemap_index.xml",
        "plataforma": "woocommerce",
        # No WooCommerce só os sitemaps de produto interessam.
        "sitemap_filter": re.compile(r"product-sitemap\d*\.xml"),
    },
}


# -------------------------- Camada de persistência --------------------------

class CheckpointStore:
    """Guarda URLs descobertas, produtos extraídos e fotos baixadas."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS product_urls (
                site TEXT NOT NULL,
                url TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pendente',
                erro TEXT,
                atualizado_em TEXT,
                PRIMARY KEY (site, url)
            );
            CREATE TABLE IF NOT EXISTS produtos (
                site TEXT NOT NULL,
                url TEXT NOT NULL,
                sku TEXT,
                nome TEXT,
                descricao TEXT,
                marca TEXT,
                gtin TEXT,
                categoria TEXT,
                preco REAL,
                moeda TEXT,
                disponibilidade TEXT,
                imagens_json TEXT,
                metodo TEXT,
                extraido_em TEXT,
                PRIMARY KEY (site, url)
            );
            CREATE TABLE IF NOT EXISTS fotos (
                site TEXT NOT NULL,
                produto_url TEXT NOT NULL,
                imagem_url TEXT NOT NULL,
                caminho_local TEXT,
                status TEXT NOT NULL DEFAULT 'pendente',
                erro TEXT,
                PRIMARY KEY (site, produto_url, imagem_url)
            );
            CREATE TABLE IF NOT EXISTS execucao (
                chave TEXT PRIMARY KEY,
                valor TEXT
            );
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # --- URLs de produto ---

    def add_urls(self, site: str, urls: Iterable[str]) -> int:
        rows = [(site, url, utcnow()) for url in urls]
        cur = self.conn.executemany(
            "INSERT OR IGNORE INTO product_urls (site, url, atualizado_em)"
            " VALUES (?, ?, ?)",
            rows,
        )
        self.conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0

    def pending_urls(self, site: str, limit: Optional[int] = None) -> List[str]:
        sql = (
            "SELECT url FROM product_urls WHERE site = ? AND status = 'pendente'"
            " ORDER BY url"
        )
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [row["url"] for row in self.conn.execute(sql, (site,))]

    def mark_url(self, site: str, url: str, status: str, erro: Optional[str] = None) -> None:
        self.conn.execute(
            "UPDATE product_urls SET status = ?, erro = ?, atualizado_em = ?"
            " WHERE site = ? AND url = ?",
            (status, erro, utcnow(), site, url),
        )
        self.conn.commit()

    def count_urls(self, site: str) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in self.conn.execute(
            "SELECT status, COUNT(*) AS total FROM product_urls WHERE site = ?"
            " GROUP BY status",
            (site,),
        ):
            counts[row["status"]] = row["total"]
        return counts

    # --- Produtos ---

    def upsert_product(self, produto: Dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO produtos (site, url, sku, nome, descricao, marca, gtin,
                categoria, preco, moeda, disponibilidade, imagens_json, metodo,
                extraido_em)
            VALUES (:site, :url, :sku, :nome, :descricao, :marca, :gtin,
                :categoria, :preco, :moeda, :disponibilidade, :imagens_json,
                :metodo, :extraido_em)
            ON CONFLICT (site, url) DO UPDATE SET
                sku = excluded.sku,
                nome = excluded.nome,
                descricao = excluded.descricao,
                marca = excluded.marca,
                gtin = excluded.gtin,
                categoria = excluded.categoria,
                preco = excluded.preco,
                moeda = excluded.moeda,
                disponibilidade = excluded.disponibilidade,
                imagens_json = excluded.imagens_json,
                metodo = excluded.metodo,
                extraido_em = excluded.extraido_em
            """,
            produto,
        )
        self.conn.commit()

    def iter_products(self, site: Optional[str] = None) -> Iterator[sqlite3.Row]:
        if site:
            cursor = self.conn.execute(
                "SELECT * FROM produtos WHERE site = ? ORDER BY site, nome", (site,)
            )
        else:
            cursor = self.conn.execute("SELECT * FROM produtos ORDER BY site, nome")
        yield from cursor

    def count_products(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS total FROM produtos").fetchone()
        return int(row["total"])

    # --- Fotos ---

    def add_fotos(self, site: str, produto_url: str, imagem_urls: Iterable[str]) -> None:
        rows = [(site, produto_url, u) for u in imagem_urls]
        self.conn.executemany(
            "INSERT OR IGNORE INTO fotos (site, produto_url, imagem_url)"
            " VALUES (?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def pending_fotos(self, site: str) -> List[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM fotos WHERE site = ? AND status = 'pendente'"
                " ORDER BY produto_url",
                (site,),
            )
        )

    def mark_foto(
        self,
        site: str,
        produto_url: str,
        imagem_url: str,
        status: str,
        caminho_local: Optional[str] = None,
        erro: Optional[str] = None,
    ) -> None:
        self.conn.execute(
            "UPDATE fotos SET status = ?, caminho_local = ?, erro = ?"
            " WHERE site = ? AND produto_url = ? AND imagem_url = ?",
            (status, caminho_local, erro, site, produto_url, imagem_url),
        )
        self.conn.commit()

    def fotos_do_produto(self, site: str, produto_url: str) -> List[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM fotos WHERE site = ? AND produto_url = ?"
                " AND status = 'baixada' ORDER BY imagem_url",
                (site, produto_url),
            )
        )

    def set_meta(self, chave: str, valor: str) -> None:
        self.conn.execute(
            "INSERT INTO execucao (chave, valor) VALUES (?, ?)"
            " ON CONFLICT (chave) DO UPDATE SET valor = excluded.valor",
            (chave, valor),
        )
        self.conn.commit()


# ------------------------------- Cliente HTTP -------------------------------

class PoliteHttpClient:
    """Sessão HTTP com retry, intervalo mínimo por domínio e robots.txt."""

    def __init__(self, delay_seconds: float = DEFAULT_DELAY_SECONDS) -> None:
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self._last_request: Dict[str, float] = {}
        self._robots: Dict[str, urllib.robotparser.RobotFileParser] = {}

    def _wait_turn(self, url: str) -> None:
        host = urlparse(url).netloc
        last = self._last_request.get(host)
        if last is not None:
            elapsed = time.monotonic() - last
            if elapsed < self.delay_seconds:
                time.sleep(self.delay_seconds - elapsed)
        self._last_request[host] = time.monotonic()

    def allowed(self, url: str) -> bool:
        host = urlparse(url).netloc
        parser = self._robots.get(host)
        if parser is None:
            parser = urllib.robotparser.RobotFileParser()
            robots_url = f"https://{host}/robots.txt"
            try:
                self._wait_turn(robots_url)
                response = self.session.get(robots_url, timeout=30)
                if response.status_code >= 400:
                    parser.parse([])
                else:
                    parser.parse(response.text.splitlines())
            except requests.RequestException:
                parser.parse([])
            self._robots[host] = parser
        return parser.can_fetch("*", url)

    def get(self, url: str, timeout: int = 45) -> requests.Response:
        self._wait_turn(url)
        response = self.session.get(url, timeout=timeout)
        response.raise_for_status()
        return response

    def get_text(self, url: str, timeout: int = 45) -> str:
        response = self.get(url, timeout=timeout)
        if not response.encoding or response.encoding.lower() == "iso-8859-1":
            # Tray declara ISO-8859-1; confirmo pelo conteúdo antes de decodificar.
            apparent = response.apparent_encoding
            if apparent:
                response.encoding = apparent
        return response.text


# --------------------------- Descoberta via sitemap ---------------------------

LOC_PATTERN = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")


def discover_product_urls(
    client: PoliteHttpClient, site_key: str, config: Dict[str, Any], logger: logging.Logger
) -> List[str]:
    """Percorre o sitemap (e sub-sitemaps) e devolve URLs de página de produto."""
    queue = [config["sitemap"]]
    seen_sitemaps = set()
    urls: List[str] = []
    sitemap_filter = config.get("sitemap_filter")
    while queue:
        sitemap_url = queue.pop(0)
        if sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)
        try:
            body = client.get_text(sitemap_url)
        except requests.RequestException as error:
            logger.warning("%s: falha ao ler sitemap %s (%s)", site_key, sitemap_url, error)
            continue
        locs = LOC_PATTERN.findall(body)
        if "<sitemapindex" in body:
            for loc in locs:
                if sitemap_filter and not sitemap_filter.search(loc):
                    continue
                queue.append(loc.strip())
            continue
        for loc in locs:
            loc = html_module.unescape(loc.strip())
            if loc.lower().endswith((".xml", ".jpg", ".jpeg", ".png", ".webp", ".pdf")):
                continue
            urls.append(loc)
    logger.info("%s: %d URLs descobertas no sitemap", site_key, len(urls))
    return urls


_KEYWORD_PATTERNS: Dict[str, re.Pattern] = {}


def _keyword_pattern(keyword: str) -> re.Pattern:
    # Fronteira de palavra evita falsos positivos como "bota" dentro de "botao".
    pattern = _KEYWORD_PATTERNS.get(keyword)
    if pattern is None:
        pattern = re.compile(r"\b" + re.escape(keyword) + r"\b")
        _KEYWORD_PATTERNS[keyword] = pattern
    return pattern


def matches_any_keyword(text: str, keywords: List[str]) -> bool:
    return any(_keyword_pattern(keyword).search(text) for keyword in keywords)


def url_matches_keywords(url: str, keywords: List[str]) -> bool:
    slug = normalize_text(urlparse(url).path.replace("-", " ").replace("/", " "))
    return matches_any_keyword(slug, keywords)


# --------------------------- Extração de produto ---------------------------

JSON_LD_PATTERN = re.compile(
    r"<script[^>]*type\s*=\s*[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.S | re.I,
)
OG_IMAGE_PATTERN = re.compile(
    r"<meta[^>]+property=[\"']og:image[\"'][^>]+content=[\"']([^\"']+)[\"']", re.I
)
META_DESCRIPTION_PATTERN = re.compile(
    r"<meta[^>]+name=[\"']description[\"'][^>]+content=[\"']([^\"']+)[\"']", re.I
)
TITLE_PATTERN = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)


def iter_json_ld_nodes(html: str) -> Iterator[Dict[str, Any]]:
    for block in JSON_LD_PATTERN.findall(html):
        block = block.strip()
        if not block:
            continue
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        stack: List[Any] = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                if "@graph" in node:
                    stack.extend(node["@graph"] if isinstance(node["@graph"], list) else [node["@graph"]])
                yield node


def is_product_node(node: Dict[str, Any]) -> bool:
    node_type = node.get("@type")
    if isinstance(node_type, list):
        return "Product" in node_type
    return node_type == "Product"


def normalize_images(value: Any, base_url: str) -> List[str]:
    """Aceita string, lista, ImageObject ou lista de ImageObject."""
    found: List[str] = []
    stack: List[Any] = [value]
    while stack:
        item = stack.pop(0)
        if item is None:
            continue
        if isinstance(item, str):
            url = urljoin(base_url, item.strip())
            if url.startswith("http") and url not in found:
                found.append(url)
        elif isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, dict):
            stack.append(item.get("url") or item.get("contentUrl"))
    return found


def parse_price(offers: Any) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    if isinstance(offers, list):
        offers = offers[0] if offers else None
    if not isinstance(offers, dict):
        return None, None, None
    price_raw = offers.get("price") or offers.get("lowPrice")
    price: Optional[float] = None
    if price_raw is not None:
        try:
            price = float(str(price_raw).replace(",", "."))
        except ValueError:
            price = None
    availability = offers.get("availability")
    if isinstance(availability, str):
        availability = availability.strip().rsplit("/", 1)[-1] or None
    return price, offers.get("priceCurrency"), availability


def extract_product(html: str, url: str) -> Optional[Dict[str, Any]]:
    """Extrai o produto do JSON-LD; complementa com metadados da página."""
    product_node: Optional[Dict[str, Any]] = None
    for node in iter_json_ld_nodes(html):
        if is_product_node(node):
            product_node = node
            break
    if product_node is None:
        return None

    brand = product_node.get("brand")
    if isinstance(brand, dict):
        brand = brand.get("name")
    category = product_node.get("category")
    if isinstance(category, dict):
        category = category.get("name")

    images = normalize_images(product_node.get("image"), url)
    if not images:
        og_match = OG_IMAGE_PATTERN.search(html)
        if og_match:
            images = [urljoin(url, html_module.unescape(og_match.group(1)))]

    description = strip_html(product_node.get("description"))
    if not description:
        meta_match = META_DESCRIPTION_PATTERN.search(html)
        if meta_match:
            description = strip_html(meta_match.group(1))

    name = strip_html(product_node.get("name"))
    if not name:
        title_match = TITLE_PATTERN.search(html)
        if title_match:
            name = strip_html(title_match.group(1))

    price, currency, availability = parse_price(product_node.get("offers"))
    sku = product_node.get("sku")

    return {
        "url": url,
        "sku": str(sku).strip() if sku not in (None, "") else None,
        "nome": name,
        "descricao": description,
        "marca": strip_html(brand),
        "gtin": product_node.get("gtin") or product_node.get("gtin13"),
        "categoria": strip_html(category),
        "preco": price,
        "moeda": currency,
        "disponibilidade": availability,
        "imagens": images,
        "metodo": "json-ld",
    }


def product_matches_keywords(produto: Dict[str, Any], keywords: List[str]) -> bool:
    haystack = normalize_text(
        " ".join(
            str(produto.get(field) or "")
            for field in ("nome", "descricao", "categoria", "url")
        ).replace("-", " ").replace("/", " ")
    )
    return matches_any_keyword(haystack, keywords)


# ------------------------------ Download de fotos ------------------------------

CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/avif": ".avif",
}


def download_fotos(
    client: PoliteHttpClient,
    store: CheckpointStore,
    site_key: str,
    fotos_dir: Path,
    logger: logging.Logger,
) -> Dict[str, int]:
    result = {"baixadas": 0, "erros": 0}
    produtos = {row["url"]: row for row in store.iter_products(site_key)}
    for foto in store.pending_fotos(site_key):
        produto = produtos.get(foto["produto_url"])
        sku = (produto["sku"] if produto else None) or "sem-sku"
        nome = produto["nome"] if produto else foto["produto_url"]
        pasta = fotos_dir / site_key / f"{slugify(sku, 'sem-sku')}_{slugify(nome, 'produto')}"
        try:
            response = client.get(foto["imagem_url"], timeout=60)
            content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip()
            extension = CONTENT_TYPE_EXTENSIONS.get(content_type)
            if extension is None:
                suffix = Path(urlparse(foto["imagem_url"]).path).suffix.lower()
                extension = suffix if suffix in (".jpg", ".jpeg", ".png", ".webp", ".gif") else ".jpg"
            pasta.mkdir(parents=True, exist_ok=True)
            index = len(list(pasta.glob("img_*"))) + 1
            destino = pasta / f"img_{index:02d}{extension}"
            destino.write_bytes(response.content)
            store.mark_foto(
                site_key, foto["produto_url"], foto["imagem_url"],
                "baixada", caminho_local=str(destino),
            )
            result["baixadas"] += 1
        except requests.RequestException as error:
            store.mark_foto(
                site_key, foto["produto_url"], foto["imagem_url"],
                "erro", erro=str(error),
            )
            result["erros"] += 1
            logger.warning("%s: falha na foto %s (%s)", site_key, foto["imagem_url"], error)
    return result


# --------------------------------- Coleta ---------------------------------

def collect_site(
    client: PoliteHttpClient,
    store: CheckpointStore,
    site_key: str,
    config: Dict[str, Any],
    keywords: List[str],
    prefilter_urls: bool,
    limit: Optional[int],
    logger: logging.Logger,
) -> Dict[str, int]:
    counts = store.count_urls(site_key)
    if not counts:
        urls = discover_product_urls(client, site_key, config, logger)
        if prefilter_urls and keywords:
            urls = [u for u in urls if url_matches_keywords(u, keywords)]
            logger.info(
                "%s: %d URLs após o pré-filtro por palavra-chave", site_key, len(urls)
            )
        store.add_urls(site_key, urls)

    result = {"produtos": 0, "fora_do_filtro": 0, "sem_produto": 0, "erros": 0}
    for url in store.pending_urls(site_key, limit=limit):
        if not client.allowed(url):
            store.mark_url(site_key, url, "bloqueada_robots")
            continue
        try:
            page = client.get_text(url)
        except requests.RequestException as error:
            store.mark_url(site_key, url, "erro", erro=str(error))
            result["erros"] += 1
            continue
        produto = extract_product(page, url)
        if produto is None:
            store.mark_url(site_key, url, "sem_produto")
            result["sem_produto"] += 1
            continue
        if keywords and not product_matches_keywords(produto, keywords):
            store.mark_url(site_key, url, "fora_do_filtro")
            result["fora_do_filtro"] += 1
            continue
        registro = dict(produto)
        registro["site"] = site_key
        registro["imagens_json"] = json.dumps(produto["imagens"], ensure_ascii=False)
        registro["extraido_em"] = utcnow()
        registro.pop("imagens")
        store.upsert_product(registro)
        store.add_fotos(site_key, url, produto["imagens"][: DEFAULT_MAX_FOTOS])
        store.mark_url(site_key, url, "concluida")
        result["produtos"] += 1
        logger.info(
            "%s: produto extraído — sku=%s nome=%s",
            site_key, produto.get("sku"), (produto.get("nome") or "")[:60],
        )
    return result


# ------------------------------- Exportações -------------------------------

CSV_FIELDS = [
    "site", "sku", "nome", "marca", "gtin", "categoria", "preco", "moeda",
    "disponibilidade", "url", "imagens_urls", "fotos_locais", "descricao",
]


def export_outputs(store: CheckpointStore, output_dir: Path) -> Dict[str, int]:
    registros: List[Dict[str, Any]] = []
    for row in store.iter_products():
        fotos = [f["caminho_local"] for f in store.fotos_do_produto(row["site"], row["url"])]
        registros.append(
            {
                "site": row["site"],
                "sku": row["sku"],
                "nome": row["nome"],
                "marca": row["marca"],
                "gtin": row["gtin"],
                "categoria": row["categoria"],
                "preco": row["preco"],
                "moeda": row["moeda"],
                "disponibilidade": row["disponibilidade"],
                "url": row["url"],
                "imagens_urls": json.loads(row["imagens_json"] or "[]"),
                "fotos_locais": fotos,
                "descricao": row["descricao"],
                "extraido_em": row["extraido_em"],
            }
        )

    json_path = output_dir / "produtos_nauticos.json"
    json_path.write_text(
        json.dumps(registros, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    csv_path = output_dir / "produtos_nauticos.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, delimiter=";")
        writer.writeheader()
        for registro in registros:
            row = {key: registro.get(key) for key in CSV_FIELDS}
            row["imagens_urls"] = " | ".join(registro["imagens_urls"])
            row["fotos_locais"] = " | ".join(registro["fotos_locais"])
            writer.writerow(row)

    return {"registros": len(registros)}


def write_manifest(
    output_dir: Path,
    sites: List[str],
    keywords: List[str],
    stats: Dict[str, Dict[str, int]],
    exportados: int,
) -> None:
    manifest = {
        "gerado_em": utcnow(),
        "aplicativo": APP_NAME,
        "sites": sites,
        "filtro_palavras_chave": keywords,
        "estatisticas_por_site": stats,
        "produtos_exportados": exportados,
        "observacao": (
            "Coleta de páginas públicas de produto permitidas pelo robots.txt,"
            " via JSON-LD schema.org/Product publicado pelas próprias lojas."
        ),
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ----------------------------------- CLI -----------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extrai SKU, descrição e fotos de catálogos náuticos públicos com"
            " foco em produtos para tripulantes em escala curta."
        )
    )
    parser.add_argument(
        "modo", choices=["coletar", "export"],
        help="'coletar' baixa e extrai; 'export' regenera as saídas do checkpoint",
    )
    parser.add_argument(
        "--saida", type=Path, required=True,
        help="Diretório de saída (recomendado dentro de data/)",
    )
    parser.add_argument(
        "--sites", default=",".join(SITES),
        help=f"Sites separados por vírgula. Disponíveis: {', '.join(SITES)}",
    )
    parser.add_argument(
        "--filtro", choices=["tripulante", "todos"], default="tripulante",
        help="'tripulante' mantém itens úteis em escala curta; 'todos' desativa o filtro",
    )
    parser.add_argument(
        "--palavras-chave", default=None,
        help="Lista própria de palavras-chave separadas por vírgula (substitui o filtro padrão)",
    )
    parser.add_argument(
        "--sem-prefiltro-url", action="store_true",
        help="Baixa todas as páginas antes de filtrar (mais lento, cobertura máxima)",
    )
    parser.add_argument(
        "--limite", type=int, default=None,
        help="Máximo de páginas de produto novas por site nesta execução",
    )
    parser.add_argument(
        "--sem-fotos", action="store_true", help="Não baixa as imagens dos produtos"
    )
    parser.add_argument(
        "--intervalo", type=float, default=DEFAULT_DELAY_SECONDS,
        help="Segundos mínimos entre requisições ao mesmo domínio",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logger = logging.getLogger("produtos-nauticos")

    site_keys = [key.strip() for key in args.sites.split(",") if key.strip()]
    unknown = [key for key in site_keys if key not in SITES]
    if unknown:
        logger.error("Sites desconhecidos: %s", ", ".join(unknown))
        return 2

    if args.palavras_chave:
        keywords = [normalize_text(kw) for kw in args.palavras_chave.split(",") if kw.strip()]
    elif args.filtro == "tripulante":
        keywords = [normalize_text(kw) for kw in TRIPULANTE_KEYWORDS]
    else:
        keywords = []

    output_dir: Path = args.saida
    output_dir.mkdir(parents=True, exist_ok=True)
    store = CheckpointStore(output_dir / "checkpoint.sqlite")
    stats: Dict[str, Dict[str, int]] = {}
    try:
        if args.modo == "coletar":
            client = PoliteHttpClient(delay_seconds=args.intervalo)
            for site_key in site_keys:
                logger.info("== Coletando %s ==", site_key)
                stats[site_key] = collect_site(
                    client, store, site_key, SITES[site_key], keywords,
                    prefilter_urls=not args.sem_prefiltro_url,
                    limit=args.limite, logger=logger,
                )
                if not args.sem_fotos:
                    stats[site_key].update(
                        download_fotos(client, store, site_key, output_dir / "fotos", logger)
                    )
            store.set_meta("ultima_coleta", utcnow())
        exported = export_outputs(store, output_dir)
        write_manifest(output_dir, site_keys, keywords, stats, exported["registros"])
        logger.info(
            "Concluído: %d produtos no catálogo (%s)",
            exported["registros"], output_dir,
        )
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
