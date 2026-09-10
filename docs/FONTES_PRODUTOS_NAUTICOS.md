# Produtos náuticos — fontes, limites e decisões técnicas

Este documento registra as fontes do `extrator_produtos_nauticos.py` e as decisões que tomei para manter a coleta dentro do que as lojas publicam abertamente para indexação.

## Público-alvo da curadoria

O filtro padrão (`--filtro tripulante`) seleciona produtos úteis para tripulantes em escala curta no Porto de Santos: EPI e salvatagem (coletes, boias, apitos, strobes), vestuário de bordo (botas, capas de chuva, jaquetas), eletrônicos portáteis (VHF, binóculos, lanternas, GPS, EPIRB/PLB) e itens de vida a bordo (bolsas estanques, garrafas térmicas, adaptadores). A lista de palavras-chave está no próprio código e pode ser substituída com `--palavras-chave`.

Os ship chandlers tradicionais de Santos (Trapel, WJG, Ship Chandler Santos) trabalham por cotação B2B e não publicam catálogo com SKU; por isso as fontes são lojas náuticas de e-commerce com catálogo público.

## Fontes pré-configuradas

| Chave | Loja | Plataforma | Verificação |
|---|---|---|---|
| `produtos-maritimos` | [produtosmaritimos.com.br](https://www.produtosmaritimos.com.br) | Tray | robots.txt permite `*` fora de carrinho/checkout; sitemap público; JSON-LD Product com SKU |
| `porto-nautico` | [portonautico.com](https://www.portonautico.com) | Tray | idem |
| `keomar` | [keomarnautica.com.br](https://www.keomarnautica.com.br) | Tray | idem |
| `r4nautic` | [r4nautic.com](https://r4nautic.com) | WooCommerce | robots.txt permite `*`; sitemaps `product-sitemap*.xml`; JSON-LD Product com SKU |

Verificação feita em 2026-08-31. O robots.txt é reconsultado a cada execução e URLs bloqueadas são marcadas como `bloqueada_robots` no checkpoint.

## Método de extração

Cada página de produto é lida uma única vez e os campos vêm do bloco JSON-LD `schema.org/Product` que a própria loja publica para o Google: nome, SKU, descrição, marca, GTIN, categoria, preço, moeda, disponibilidade e imagens. Quando o JSON-LD não traz imagem ou descrição, uso `og:image` e a meta description como reserva. Páginas sem `Product` são marcadas `sem_produto` e não viram registro.

As lojas Tray servem HTML em ISO-8859-1; o cliente detecta o charset real antes de decodificar para não corromper acentuação.

## Uso responsável

- Intervalo mínimo entre requisições ao mesmo domínio (padrão 1,5 s, ajustável com `--intervalo`) e retry com recuo exponencial apenas para erros transitórios.
- Nada de carrinho, checkout, login ou área autenticada — somente páginas públicas de produto e sitemaps.
- Fotos e descrições permanecem de titularidade das lojas e fabricantes. O resultado serve para análise, comparação e curadoria interna de catálogo; republicar as fotos ou copiar descrições em outra loja exige autorização dos titulares.
- Preço e disponibilidade refletem o momento da coleta e expiram rápido; o campo `extraido_em` registra o instante de cada extração.

## Retomada e rastreabilidade

O SQLite local guarda cada URL descoberta com status (`pendente`, `concluida`, `sem_produto`, `fora_do_filtro`, `bloqueada_robots`, `erro`) e cada foto com o caminho baixado. Uma execução interrompida continua de onde parou, e as exportações são sempre reconstruídas a partir do checkpoint.
