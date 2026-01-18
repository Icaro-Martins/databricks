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
├── 🚀 projects/
│   ├── 🥉 source_bronze/   # Raw data ingestion.
│   ├── 🥈 bronze_silver/   # Data transformation and cleansing.
│   ├── 🥇 silver_gold/     # Enrichmant layer.
│   └── 🧰 utils/           # Reusable funtions and components.
│
├── ❄️ snowflake/           # Projects focused on snowflake (ETL, consumption and optimization).
├── 🧱 data_products/       # Ohter projects that not did not require the user of Medallion Architecture.
|
└── 🗂️ docs/
    └── 📘 notebooks_html/  # Used for display in wiki to reference projects.
```
