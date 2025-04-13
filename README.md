🔷 Azure Data Engineering Project: Netflix ELT Pipeline with Databricks, ADF & Unity Catalog
**🔹 Modular, Parameterized Pipeline:**  
Developed a modular, parameterized ELT pipeline using Azure Data Factory (ADF) and Azure Databricks, orchestrating dynamic ingestion, transformation, and streaming workflows for Netflix data.

**🔹 Dynamic Pipelines:**  
Implemented **dynamic pipelines** using ADF’s **Set Variable** and **ForEach** loop activities to iterate over GitHub-sourced file metadata, enabling scalable and file-agnostic ingestion.

**🔹 Databricks Notebook Reusability:**  
Enabled **parameterized notebook execution** in Databricks for reusability across datasets by passing **folder names, file paths**, and **conditions** as parameters.

**🔹 Weekday vs Weekend Conditional Logic:**  
Used a **weekday-driven logic** in Databricks Workflows:  
- A **lookup task (weekday_lookup)** checks if the current day is Sunday  
- If **True**, it triggers the `silver_Master_Data` transformation  
- If **False**, it routes to a `FalseNotebook`, skipping unnecessary processing — enabling a **smart weekend-only logic**

**🔹 Multi-layered Delta Lake Architecture:**  
- **Bronze Layer:** Raw ingestion using **Auto Loader**  
- **Silver Layer:** Null cleansing, type casting, enrichment (`Shorttitle`, `type_flag`), and formatting  
- **Gold Layer:** Aggregations using **groupBy()**, **rank()**, and **KPI calculations**

**🔹 Unity Catalog + DLT Integration:**  
- Managed **access control and lineage** using Unity Catalog  
- Built streaming tables using **Delta Live Tables (DLT)** with **auto-managed streaming** and lineage visualization  
- Tracked transformations using **DLT Graph** for debugging and performance optimization

**🔹 Dynamic Execution Control using Parameters:**  
- Created tasks like `silver_iteration` and `Lookup_loc` to **dynamically drive pipeline execution** based on parameters and folder context

**📂 Repository Access:**  
Hosted all notebooks in a public-facing GitHub repo to showcase transparency and enable direct reuse:  
[https://github.com/AlishaRuqshan/azure-databricks-netflix-pipeline-DLT-databricks](https://github.com/AlishaRuqshan/azure-databricks-netflix-pipeline-DLT-databricks)
