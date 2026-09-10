import tempfile
import unittest
from pathlib import Path

from extrator_produtos_nauticos import (
    CheckpointStore,
    TRIPULANTE_KEYWORDS,
    extract_product,
    normalize_images,
    normalize_text,
    product_matches_keywords,
    url_matches_keywords,
)

TRAY_HTML = """
<html><head><title>Rádio VHF</title>
<script type="application/ld+json">
{"@context": "http://schema.org", "@type": "Product",
 "name": "Rádio VHF UM-385BK Uniden", "sku": "401",
 "description": "Rádio VHF com <b>DSC</b> e alerta de emergência.",
 "image": "https://images.tcdn.com.br/img/img_prod/1/radio_1.jpg",
 "gtin": "7891234577542", "brand": {"@type": "Brand", "name": "Uniden"},
 "offers": {"@type": "Offer", "priceCurrency": "BRL", "price": "2055.07",
            "availability": " http://schema.org/OutOfStock"}}
</script></head><body></body></html>
"""

WOO_HTML = """
<html><head>
<script type="application/ld+json">
{"@context": "https://schema.org", "@graph": [
  {"@type": "WebPage", "name": "pagina"},
  {"@type": "Product", "name": "Bolsa estanque 20L", "sku": "0312",
   "description": "Bolsa impermeável para tripulação.",
   "category": "Vida a bordo",
   "image": [{"@type": "ImageObject", "url": "https://x.com/a.jpg"},
             {"@type": "ImageObject", "url": "https://x.com/b.jpg"}],
   "offers": {"@type": "Offer", "price": "285.11", "priceCurrency": "BRL",
              "availability": "http://schema.org/InStock"}}
]}
</script></head><body></body></html>
"""


class ExtractProductTest(unittest.TestCase):
    def test_extracts_tray_product_with_sku_photo_and_description(self):
        """Garante os três campos pedidos: foto, SKU e descrição."""
        produto = extract_product(TRAY_HTML, "https://loja.exemplo/radio")
        self.assertIsNotNone(produto)
        self.assertEqual(produto["sku"], "401")
        self.assertEqual(produto["marca"], "Uniden")
        self.assertIn("DSC", produto["descricao"])
        self.assertNotIn("<b>", produto["descricao"])
        self.assertEqual(
            produto["imagens"],
            ["https://images.tcdn.com.br/img/img_prod/1/radio_1.jpg"],
        )
        self.assertEqual(produto["preco"], 2055.07)
        self.assertEqual(produto["disponibilidade"], "OutOfStock")

    def test_extracts_woocommerce_product_inside_graph(self):
        """Garante o suporte ao envelope @graph usado pelo WooCommerce."""
        produto = extract_product(WOO_HTML, "https://loja.exemplo/bolsa")
        self.assertIsNotNone(produto)
        self.assertEqual(produto["sku"], "0312")
        self.assertEqual(produto["categoria"], "Vida a bordo")
        self.assertEqual(len(produto["imagens"]), 2)
        self.assertEqual(produto["disponibilidade"], "InStock")

    def test_returns_none_without_product_json_ld(self):
        """Página sem schema.org/Product não vira registro."""
        self.assertIsNone(extract_product("<html><body>Home</body></html>", "https://x"))


class KeywordFilterTest(unittest.TestCase):
    def test_crew_keywords_match_without_accents(self):
        keywords = [normalize_text(kw) for kw in TRIPULANTE_KEYWORDS]
        produto = {
            "nome": "Rádio VHF portátil HX210",
            "descricao": "Flutuante, ideal para tripulação.",
            "categoria": None,
            "url": "https://loja.exemplo/radio-vhf-portatil",
        }
        self.assertTrue(product_matches_keywords(produto, keywords))
        self.assertTrue(
            url_matches_keywords("https://loja.exemplo/bolsa-estanque-20l", keywords)
        )
        self.assertFalse(
            url_matches_keywords("https://loja.exemplo/helice-de-bronze-3-pas", keywords)
        )

    def test_keyword_needs_word_boundary(self):
        """'bota' não pode casar com 'botão de painel'."""
        keywords = [normalize_text(kw) for kw in TRIPULANTE_KEYWORDS]
        self.assertFalse(
            url_matches_keywords("https://loja.exemplo/botao-de-painel-sw-200", keywords)
        )
        self.assertTrue(
            url_matches_keywords("https://loja.exemplo/bota-de-borracha-pvc", keywords)
        )


class NormalizeImagesTest(unittest.TestCase):
    def test_accepts_string_list_and_image_object(self):
        base = "https://loja.exemplo/produto"
        self.assertEqual(
            normalize_images("https://cdn.exemplo/a.jpg", base),
            ["https://cdn.exemplo/a.jpg"],
        )
        self.assertEqual(
            normalize_images(
                [{"@type": "ImageObject", "contentUrl": "https://cdn.exemplo/b.png"}],
                base,
            ),
            ["https://cdn.exemplo/b.png"],
        )
        self.assertEqual(normalize_images(None, base), [])


class CheckpointStoreTest(unittest.TestCase):
    def test_urls_are_deduplicated_and_resumable(self):
        """A segunda execução não reprocessa URLs já concluídas."""
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointStore(Path(directory) / "checkpoint.sqlite")
            try:
                store.add_urls("loja", ["https://a", "https://b"])
                store.add_urls("loja", ["https://a"])
                self.assertEqual(len(store.pending_urls("loja")), 2)
                store.mark_url("loja", "https://a", "concluida")
                self.assertEqual(store.pending_urls("loja"), ["https://b"])
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
