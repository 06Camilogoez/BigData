# Proyecto Final: Procesamiento Distribuido y Cierre del Caso (EA3)

**Asignatura:** Big Data y Procesamiento Distribuido  
**Carrera:** Ingeniería de Software  
**Estudiante:** Wilfran Camilo Valencia Góez  
**Repositorio:** [https://github.com/06Camilogoez/BigData](https://github.com/06Camilogoez/BigData)

---

## 📁 Contenido de la Carpeta `/ea3`

| Archivo | Descripción |
| :--- | :--- |
| **`notebook.ipynb`** | Notebook completo con las 7 secciones oficiales, materialización de la Capa Oro, experimento de optimización con protocolo de benchmark riguroso, planes de ejecución `.explain()`, respuesta a la pregunta orientadora y guion de sustentación. |
| **`README.md`** | Esta guía con el paso a paso de ejecución en Databricks, explicación teórica de la optimización y guion para la grabación del video. |

---

## 🚀 Guía Paso a Paso para la Ejecución en Databricks

### Paso 1: Importar el Notebook a Databricks
1. Ingresa a tu espacio de trabajo en **Databricks**.
2. En el menú lateral izquierdo, ve a **Workspace** → **Users** → Tu usuario.
3. Haz clic en la flecha de opciones (o clic derecho) y selecciona **Import**.
4. Selecciona o arrastra el archivo local: `c:\Users\USER\Desktop\tareas\Big Data\ea3\notebook.ipynb`.

### Paso 2: Ejecutar el Notebook Completo
1. Enciende tu clúster o asocia el cómputo Serverless.
2. Haz clic en **Run All** (Ejecutar todo):
   - **Sección 2:** Creará la tabla Delta agregada en Capa Oro: `oro.rendimiento_comercial_destinos`.
   - **Sección 3:** Desactivará temporalmente el auto-broadcast (`spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)`) para forzar un **SortMergeJoin (SMJ)** y mostrará el plan inicial con sus operadores de costo (`Exchange hashpartitioning` y `Sort`).
   - **Sección 4:** Correrá el protocolo de medición: 1 corrida de calentamiento (descartada) + 3 corridas de medición consecutivas, calculando la mediana de referencia.
   - **Sección 5:** Aplicará la optimización con `F.broadcast(df_props_base)`, capturará el nuevo plan de ejecución donde desaparece el Shuffle Exchange y se sustituye por `BroadcastHashJoin`.
   - **Sección 6:** Ejecutará las 3 mediciones optimizadas, calculará la nueva mediana y mostrará el resumen con el porcentaje de mejora y factor de aceleración (*Speedup*).
   - **Sección 7:** Desplegará la respuesta argumentada a la pregunta orientadora, las conclusiones y la matriz de autoría.

### Paso 3: Exportar a HTML para Canvas
1. Una vez ejecutado de arriba a abajo con todas las celdas y salidas visibles (planes `.explain()` y tablas de tiempos):
2. Menú superior: **File** → **Export** → **HTML**.
3. Guarda el archivo como `notebook_final.html` para la entrega oficial en Canvas.

---

## 🎥 Guion para el Video de Sustentación (8 a 10 minutos)

> [!IMPORTANT]
> **Requisitos obligatorios del video:**
> - Duración total entre 8 y 10 minutos.
> - Cámara web encendida al presentarte y visible durante la sustentación.
> - Mostrar el notebook ejecutándose y los planes de ejecución en vivo en la pantalla (no diapositivas).
> - Publicar en YouTube (modo no listado / unlisted) o Google Drive (acceso público de lectura).

### Estructura de Tiempo Recomendada:

```
[0:00 - 1:15] Introducción Personal y Contexto del Proyecto Final
[1:15 - 2:45] Recorrido de la Capa Oro y Balance Volumétrico (Bronce -> Plata -> Oro)
[2:45 - 5:00] Pregunta 1: Planes de Ejecución (Antes y Después) y Operadores que Cambiaron
[5:00 - 7:00] Pregunta 2: ¿Cuándo sería CONTRA PRODUCENTE el Broadcast Join?
[7:00 - 8:30] Pregunta 3: Respuesta a la Pregunta Orientadora del Curso (< 1 min)
[8:30 - 9:30] Conclusiones del Proyecto Completo y Cierre
```

---

### Respuestas a las Tres Preguntas Obligatorias (Ficha Técnica para el Video):

#### 1. Muestre el plan de ejecución antes y después de la optimización, y explique qué cambió.
* **En pantalla:** Enfoque en la celda del `.explain(True)` inicial y luego en la del optimizado.
* **Explicación:**  
  *"En el plan inicial forzamos un **SortMergeJoin**. En él se observan claramente los dos operadores más costosos de Spark:*  
  *1. **`Exchange hashpartitioning(property_id, 200)`:** Un Shuffle masivo que obliga a serializar y transmitir cientos de miles de registros de clickstream por la red física del clúster.*  
  *2. **`Sort [property_id ASC]`:** Ordenamiento en memoria de ambas tablas con complejidad algorítmica O(N log N).*  
  *Al aplicar **`F.broadcast(properties)`**, el plan físico cambia drásticamente: desaparece por completo el operador `Exchange` sobre la tabla de clickstream y se sustituye `SortMergeJoin` por **`BroadcastHashJoin`**. La dimensión pequeña se copia en RAM en cada worker y la tabla masiva se recorre en streaming local con búsqueda hash en tiempo O(1), logrando una reducción sustancial en el tiempo de ejecución y eliminando el tráfico de red."*

#### 2. ¿En qué caso esa misma optimización sería contraproducente?
* **Explicación:**  
  *"Toda técnica tiene su límite. Aplicar `broadcast()` sería contraproducente y destructivo en tres casos:*  
  *1. **Riesgo crítico de OutOfMemoryError (OOM) en el Driver:** El nodo Driver debe recolectar la tabla entera antes de transmitirla. Si la tabla dimensional pesa varios gigabytes y supera la memoria heap del Driver, este colapsa con `java.lang.OutOfMemoryError` y tumba todo el clúster.*  
  *2. **Saturación del ancho de banda de red (Network Saturation):** Distribuir una tabla pesada hacia decenas o cientos de ejecutores satura los enlaces de red, tardando más tiempo que un shuffle particionado estándar.*  
  *3. **Agotamiento de RAM en Workers:** Cada ejecutor debe almacenar la tabla hash en su memoria heap. Si hay múltiples tareas concurrentes, esto reduce la memoria disponible para agregaciones y provoca derrames a disco (spill to disk). Por eso Spark limita el auto-broadcast por defecto a tablas de 10 MB."*

#### 3. Responda la pregunta orientadora del curso en menos de un minuto.
* **Pregunta:** *Una empresa recibe datos de su portal web, Facebook, Instagram, TikTok y WhatsApp Business. ¿Bajo qué paradigma, herramientas y arquitectura debería analizarlos?*  
* **Explicación:**  
  *"Debe abordarse bajo el **paradigma Lakehouse**, porque combina la ingesta de datos semiestructurados (JSON de redes sociales) y no estructurados (mensajes de WhatsApp) con transaccionalidad ACID sobre **Delta Lake**.*  
  *Como **herramientas**, utilizaría **Apache Kafka o Databricks Auto Loader** para la ingesta en streaming continuo de webhooks de Meta y TikTok, **Apache Spark en Databricks** como motor unificado de procesamiento distribuido y **Unity Catalog** para anonimizar datos personales (PII) y aplicar gobernanza.*  
  *La **arquitectura** debe ser **Medallion**: capa **Bronce** para guardar los payloads JSON crudos de las APIs; capa **Plata** para limpieza, desanidado y resolución de identidad omnicanal (cruzar el número de WhatsApp con el usuario web); y capa **Oro** para calcular modelos de atribución de marketing, costo de adquisición (CAC) y tasa de conversión omnicanal."*

---

## ⭐ Opción Adicional (Hasta 5 Puntos Opcionales)

Para aprovechar los 5 puntos adicionales, puedes crear un **Dashboard en Databricks SQL** sobre la tabla `oro.rendimiento_comercial_destinos` con 3 gráficos:
1. **Gráfico de Barras Horizontales:** Top 10 ciudades con mayores ingresos consolidados (`ingresos_totales_usd`).
2. **Gráfico de Dispersión / Burbujas:** Relación entre `usuarios_interesados` (tráfico) vs. `tasa_conversion_pct` (conversión).
3. **Tarjeta de KPI / Counter:** Ingresos totales de la plataforma y ticket promedio global.
