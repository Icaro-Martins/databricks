# 🚀 Implementações Técnicas

Este branch contém **códigos, estudos aplicados e projetos práticos em Engenharia de Dados**, com foco em **boas práticas, arquitetura Lakehouse e soluções escaláveis em cloud**.

O objetivo é demonstrar **como penso, organizo e implemento pipelines de dados em cenários reais**.

## 🧠 O que você encontrará aqui

- Pipelines de dados **batch e streaming**
- Processos de ingestão, transformação e enriquecimento de dados
- Arquitetura medallion **Bronze, Silver e Gold**
- Integrações com **Azure, Databricks e Snowflake**
- Código organizado, versionado e documentado

## 🗂️ Estrutura do Branch

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
|   ├── ❄️ snowflake/       # Projetos focados em Snowflake (ELT, consumo, otimização)
|   ├── 🧱 data_products/   # Outros projetos que não foram necessário usar a arquitetura Medallion
│   └── 🧰 utils/           # Funções e componentes reutilizáveis 
│
└── 🗂️ docs/
    └── 📘 notebooks_html/  # Usados para exibir nos wiki para projetos de referencia
```
