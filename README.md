<img width="1075" height="321" alt="image" src="https://github.com/user-attachments/assets/a23bffd8-3468-43ff-a7d5-dacb1fa08309" />


# 🧱 Pipeline de Processamento - Databricks (Bronze → Silver → Gold)

## 📋 Visão Geral

Este projeto representa um **pipeline simples de ingestão, validação e transformação de dados** desenvolvido no **Databricks**, utilizando a arquitetura de camadas **Medallion** (Bronze, Silver e Gold).

O objetivo do fluxo é demonstrar um processo completo de **extração, tratamento e carga de dados**, com etapas de **validação intermediária** e **criação de camadas otimizadas** para consumo analítico.

---

## ⚙️ Estrutura de Jobs

Abaixo está o fluxo completo dos jobs que compõem o pipeline:

### **1️⃣ Job_Ingestao_csv**

* **Local:** `/Projeto Bikes/bronze/01_ingestao_csv`
* **Descrição:**
  Responsável pela **ingestão dos arquivos CSV brutos** para a camada **Bronze**.
  Nesta etapa, os dados são apenas carregados e armazenados sem transformação.

---

### **2️⃣ Validador_Bronze**

* **Local:** `/Projeto Bikes/bronze/02_validador_bronze`
* **Descrição:**
  Realiza **validações básicas de integridade** nos dados ingeridos, garantindo que os arquivos contenham o formato e schema esperados antes do avanço para Silver.

---

### **3️⃣ Silver_Customer, Silver_Order e Silver_Produto**

* **Locais:**

  * `/silver/PROD/03_silver_customers`
  * `/silver/PROD/02_silver_orders`
  * `/silver/PROD/01_silver_products`
* **Descrição:**
  Cada job da camada **Silver** realiza **limpeza, padronização e enriquecimento** dos dados, gerando tabelas intermediárias mais estruturadas e prontas para análise.

  * **Silver_Customer:** normaliza dados de clientes.
  * **Silver_Order:** trata informações de pedidos.
  * **Silver_Produto:** estrutura o catálogo de produtos.

---

### **4️⃣ Validador_Silver**

* **Local:** `/silver/PROD/01_silver_products_prod`
* **Descrição:**
  Etapa de **validação cruzada entre tabelas Silver**, garantindo consistência entre pedidos, clientes e produtos antes da promoção para Gold.

---

### **5️⃣ gold_order_pending e gold_sales_ny**

* **Locais:**

  * `/GOLD/02_gold_orders_pending`
  * `/GOLD/01_gold_sales_ny`
* **Descrição:**
  Responsáveis por gerar as **tabelas finais otimizadas para consumo**.

  * **gold_order_pending:** consolida pedidos pendentes.
  * **gold_sales_ny:** consolida vendas realizadas (exemplo: vendas em NY).

---

## 🧩 Fluxo Resumido

```
Job_Ingestao_csv
    ↓
Validador_Bronze
    ↓
┌────────────┬────────────┬────────────┐
│Silver_Customer│Silver_Order│Silver_Produto│
└────────────┴────────────┴────────────┘
           ↓
     Validador_Silver
           ↓
┌───────────────────┬────────────────┐
│gold_order_pending │gold_sales_ny   │
└───────────────────┴────────────────┘
```

---

## 🧠 Conceitos Envolvidos

* **Camada Bronze:** ingestão bruta dos dados.
* **Camada Silver:** limpeza e estruturação.
* **Camada Gold:** consumo analítico e dashboards.
* **Validação:** checagens automatizadas entre camadas.
* **Jobs Databricks:** organização modular do pipeline.

---

## 🚀 Objetivo do Case

Este case foi desenvolvido para demonstrar:

* A construção de **pipelines modulares** no Databricks.
* A aplicação prática do **modelo Medallion Architecture**.
* Boas práticas de **validação e versionamento** de jobs.
* Um exemplo de **transformação incremental** e **segura** entre camadas.
