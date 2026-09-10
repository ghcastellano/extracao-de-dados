# Extrator Imobiliário Mestre

Desenvolvi este projeto para consolidar dados territoriais públicos de Itanhaém e Mogi das Cruzes em arquivos prontos para análise e mapas. A prioridade foi criar uma coleta local, retomável e auditável, sem depender de serviços proprietários para armazenar os resultados.

## O que eu resolvi

- Coleto a camada pública de arruamento de Itanhaém e converto os tiles vetoriais para GeoJSON em WGS84.
- Baixo o Cadastro Imobiliário aberto de Mogi das Cruzes e mantenho as inscrições, áreas, endereço público e valor venal disponibilizados pela Prefeitura.
- Localizo os cadastros de Mogi pela quadra pública e, quando ela não está disponível, pelo eixo público do logradouro. A precisão fica registrada em cada item.
- Uso SQLite como checkpoint para parar e retomar a execução sem baixar novamente o que já foi concluído.
- Exporto JSON, GeoJSON, CSV e um manifesto de execução a partir do mesmo checkpoint.
- Aceito a importação local de valores venais de certidões obtidas legitimamente, mantendo esse valor separado da fonte pública original.

## Pontos fortes do código

| Aspecto | Decisão que tomei |
|---|---|
| Retomada segura | Cada tile, CSV e camada possui status no SQLite. A próxima execução continua do ponto em que parou. |
| Fontes rastreáveis | Cada registro preserva cidade, fonte, camada e método de georreferenciamento. |
| Uso responsável | O coletor trabalha somente com fontes públicas. Dados de proprietário, boletos e pagamentos ficam fora do projeto. |
| Qualidade espacial | Não invento polígonos de lote: Mogi usa quadra ou logradouro público; Itanhaém permanece identificado como arruamento. |
| Consumo controlado | Os CSVs são lidos em streaming e as gravações são feitas em lotes. |
| Saídas portáveis | GeoJSON para GIS/mapas, CSV para análise tabular e JSON para integrações. |

## Estrutura

```text
.
├── extrator_imobiliario_mestre.py  # comando principal
├── extrator_cadastro_publico.py    # compatibilidade com o nome anterior
├── requirements.txt
├── docs/
│   └── FONTES_E_LIMITES.md         # fontes oficiais e limites conhecidos
├── tests/
│   └── test_extrator_imobiliario_mestre.py
└── data/                           # resultados locais, ignorados pelo Git
```

## Requisitos

- Python 3.10 ou superior
- Acesso às fontes públicas municipais durante a coleta

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
```

## Execução

O comando principal é `extrator_imobiliario_mestre.py`. Ele oferece os modos `itanhaem`, `mogi`, `ambos`, `export` e `importar-venal`. A lista completa de argumentos está disponível com `--help`.

Para uma coleta completa, utilizo o modo `ambos` com `--mogi-full`. As saídas devem ficar em um diretório dentro de `data/`, que já está ignorado pelo Git.

## Saídas

| Arquivo | Finalidade |
|---|---|
| `checkpoint.sqlite` | Retomada, deduplicação e auditoria local da coleta. |
| `cadastro_publico.json` | Base consolidada para integrações. |
| `cadastro_publico.geojson` | Feições georreferenciadas para QGIS, ArcGIS, Leaflet ou Mapbox. |
| `cadastro_publico.csv` | Atributos tabulares para planilhas e ferramentas analíticas. |
| `certidoes_venais_importadas.csv` | Relação das certidões venais importadas e seu vínculo territorial. |
| `manifest.json` | Contagens, fontes processadas e escopo da execução. |

## Extrator de Produtos Náuticos

O repositório também inclui o `extrator_produtos_nauticos.py`, que coleta catálogos públicos de lojas náuticas brasileiras (SKU, descrição, fotos, marca, preço e disponibilidade) com curadoria voltada a tripulantes em escala curta no Porto de Santos. A extração usa o JSON-LD `schema.org/Product` publicado pelas próprias lojas, respeita robots.txt e usa o mesmo modelo de checkpoint retomável em SQLite.

```bash
# Coleta com o filtro padrão de itens para tripulante e download de fotos
python extrator_produtos_nauticos.py coletar --saida data/produtos-nauticos

# Regenera JSON/CSV a partir do checkpoint
python extrator_produtos_nauticos.py export --saida data/produtos-nauticos
```

Saídas: `produtos_nauticos.json`, `produtos_nauticos.csv`, `fotos/<loja>/<sku>_<produto>/img_NN.jpg` e `manifest.json`. As fontes verificadas e os limites de uso estão em [FONTES_PRODUTOS_NAUTICOS.md](docs/FONTES_PRODUTOS_NAUTICOS.md).

## Limites e segurança

Eu não trato CPF, CNPJ de pessoa física, nome de proprietário, boleto, código de barras, pagamentos ou posição financeira. A importação de valor venal rejeita arquivos que tragam esses campos. A documentação das fontes e dos limites de cada cidade está em [FONTES_E_LIMITES.md](docs/FONTES_E_LIMITES.md).
