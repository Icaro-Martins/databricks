# 🚀 Technical Implementations

This branch conteins **code, spplied studies and pratical projects in data engineer**, with focus in **good pratices, lakehouse architecture and scalable cloud soluctions.

This goals is to demonstrate **how I think about, organize and implement pipelines in real world scenarios**

## 🧠 What you will finde here

- Bach data pipelines
- Data ingestion, transformation and enrichment process
- Bronze, Silver and Gold medallion architecture
- Integreations with Azure, Databricks and Snowflake
- COrganized, versioned and doumentation code

## 🗂️ Brach Struture

```text
🌿 branch - databricks
├── 📚 studies/
│   ├── 🐍 python/
│   ├── 🧮 sql/
│   ├── 🔥 spark/
│
├── 🚀 projetos/
│   ├── 🥉 source_bronze/   # Ingestão de dados brutos
│   ├── 🥈 bronze_silver/   # Transformações e Limpeza dos dados
│   ├── 🥇 silver_gold/     # Camada de enriquecimento
│   └── 🧰 utils/           # Funções e componentes reutilizáveis 
│
├── ❄️ snowflake/       # Projetos focados em Snowflake (ELT, consumo, otimização)
├── 🧱 data_products/   # Outros projetos que não foram necessário usar a arquitetura Medallion
|
└── 🗂️ docs/
    └── 📘 notebooks_html/  # Usados para exibir nos wiki para projetos de referencia
```
