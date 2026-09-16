# Evidencia de Aprendizaje 2 (EA2): Despliegue y Gobierno de una Infraestructura de Datos en la Nube

**Asignatura:** Big Data y Procesamiento Distribuido  
**Carrera:** Ingeniería de Software  
**Estudiante:** Wilfran Camilo Valencia Góez  
**Repositorio:** [https://github.com/06Camilogoez/BigData](https://github.com/06Camilogoez/BigData)

---

## 📁 Contenido de la Carpeta `/ea2`

| Archivo | Descripción |
| :--- | :--- |
| **`notebook.ipynb`** | Notebook interactivo completo con las 7 secciones oficiales de la plantilla, código SQL/PySpark ejecutable, justificaciones teóricas y guion de sustentación. |
| **`arquitectura.svg`** | Diagrama vectorial en alta resolución del flujo end-to-end (*Fuentes → Ingesta → Almacenamiento → Procesamiento → Consumo*) y la delimitación del Modelo de Responsabilidad Compartida. |
| **`linaje.png`** | *(Captura requerida)* Imagen del grafo de linaje generado en Databricks Catalog Explorer para la tabla `oro.kpi_conversion_dispositivos`. |
| **`job_run.png`** | *(Captura requerida)* Imagen del grafo de tareas encadenadas y la corrida exitosa (*Succeeded*) en Databricks Workflows. |
| **`README.md`** | Esta guía de instrucciones, paso a paso de ejecución y guion para el video de sustentación. |

---

## 🚀 Guía Paso a Paso para la Ejecución en Databricks

### Paso 1: Importar el Notebook a Databricks
1. Ingresa a tu espacio de trabajo en **Databricks**.
2. En el menú lateral izquierdo, ve a **Workspace** → **Users** → Tu usuario.
3. Haz clic en la flecha hacia abajo (o clic derecho) y selecciona **Import**.
4. Arrastra o selecciona el archivo `ea2/notebook.ipynb`.

### Paso 2: Ejecutar las Celdas en Orden
1. Enciende o asocia tu cluster (Databricks Runtime 13.x o superior, con soporte para Unity Catalog).
2. Ejecuta secuencialmente las celdas:
   - **Sección 2:** Creación de catálogo `wanderbricks_lakehouse`, esquemas (`bronce`, `plata`, `oro`) y volúmenes (`raw_landing`, `checkpoints`).
   - **Sección 3:** Ejecución de los `GRANT` diferenciados y consulta con `SHOW GRANTS`.
   - **Sección 4:** Ejecución del script PySpark que lee de `plata` y materializa la tabla Delta `oro.kpi_conversion_dispositivos`.

### Paso 3: Tomar la Captura del Linaje (Data Lineage)
1. En la barra lateral izquierda, abre **Catalog** (Catalog Explorer).
2. Navega a: `wanderbricks_lakehouse` → `oro` → `kpi_conversion_dispositivos`.
3. Haz clic en la pestaña superior **Lineage** (Linaje).
4. Verás el grafo visual conectando `plata.clickstream` con `oro.kpi_conversion_dispositivos`.
5. Haz clic en **Lineage graph** (puedes activar *Column lineage*).
6. Toma una captura de pantalla clara y guárdala como **`linaje.png`** dentro de esta carpeta `ea2/`.

### Paso 4: Configurar el Job en Databricks Workflows
1. En la barra lateral izquierda, haz clic en **Workflows** → **Create Job**.
2. Asigna como nombre: `Wanderbricks_Medallion_Pipeline`.
3. Configura la **Tarea 1**:
   - *Task name:* `01_ingesta_bronce_a_plata`
   - *Type:* Notebook (o Python)
   - *Source:* Workspace (selecciona tu notebook de ingesta o celda correspondiente)
4. Agrega la **Tarea 2**:
   - *Task name:* `02_agregacion_plata_a_oro`
   - *Depends on:* Selecciona `01_ingesta_bronce_a_plata` (encadenamiento secuencial)
   - *Type:* Notebook (o SQL)
5. En el panel derecho (**Job details**):
   - Configura un **Trigger**: Programado (Schedule) diario (ej. `0 0 2 * * ?`).
6. Haz clic en **Run now** (Ejecutar ahora).
7. Espera a que ambas tareas terminen en color verde (**Succeeded**).
8. Toma una captura de pantalla del grafo con el estado de éxito y el historial de corridas, y guárdala como **`job_run.png`** dentro de esta carpeta `ea2/`.
9. ⚠️ **IMPORTANTE:** Inmediatamente después de tomar la captura, pon el schedule en **Pause** (Pausar) para evitar consumir la cuota de cómputo diario del workspace.

### Paso 5: Exportar a HTML para Canvas
1. Abre tu notebook en Databricks con todas las salidas de código visibles.
2. Menú superior: **File** → **Export** → **HTML**.
3. Guarda el archivo como `notebook_ea2.html` para adjuntarlo en la entrega oficial de Canvas.

---

## 🎥 Guion para la Grabación del Video de Sustentación (6 a 9 minutos)

> [!IMPORTANT]
> **Requisitos obligatorios del video:**
> - Duración entre 6 y 9 minutos.
> - Cámara encendida al presentarse y visible durante el recorrido.
> - Recorrido en el entorno en vivo (no diapositivas).
> - Publicar en YouTube (no listado) o Google Drive (acceso público de lectura).

### Estructura de Tiempo y Contenido:

```
[0:00 - 1:15] Introducción Personal y Contexto del Proyecto
[1:15 - 2:45] Recorrido de Catalog Explorer, Esquemas, Volúmenes y Permisos GRANT
[2:45 - 4:15] Demostración en vivo de Linaje de Datos (Table & Column Lineage)
[4:15 - 5:45] Recorrido de Databricks Workflows (Grafo DAG encadenado y corrida exitosa)
[5:45 - 8:30] Respuestas Técnicas a las 3 Preguntas Clave
[8:30 - 9:00] Conclusiones Finales y Cierre
```

### Respuestas a las Tres Preguntas Obligatorias (para memorizar o tener de ficha técnica):

#### 1. ¿Qué parte de esta arquitectura administra el proveedor y cuál administran ustedes?
* **Proveedor (Databricks / Cloud):** Administra la infraestructura física (servidores, redes de centros de datos), la durabilidad del almacenamiento objeto (S3/ADLS con 11 nueves de disponibilidad), el plano de control del metastore de Unity Catalog, el auto-escalado elástico, el reemplazo automático de nodos con fallos de hardware y los parches de seguridad del sistema operativo Linux y del motor Spark/Photon.
* **Nosotros (Equipo de Datos):** Administramos la capa lógica y de gobierno: el modelado de datos Medallion (Bronce, Plata, Oro), los contratos formales de datos con `StructType`, la limpieza y desanidado de eventos de navegación, las políticas de seguridad RBAC (`GRANT`/`REVOKE`), la orquestación de tareas en Workflows y las vistas analíticas para consumo de negocio.

#### 2. Muestre un GRANT que usted ejecutó y explique a quién le está dando qué, y por qué.
* **Sentencia en pantalla:** `GRANT USE SCHEMA, SELECT ON SCHEMA wanderbricks_lakehouse.oro TO \`account users\`;`
* **Explicación:** Le estoy otorgando al grupo de analistas y usuarios de negocio (`account users`) permiso de uso (`USE SCHEMA`) y lectura (`SELECT`) estrictamente sobre el esquema `oro`.
* **Justificación técnica:** Aplicamos el **Principio de Mínimo Privilegio (PoLP)**. Los analistas solo necesitan consultar métricas consolidadas de conversión y KPIs de negocio en dashboards para la toma de decisiones. No tienen ninguna necesidad de acceder a los datos crudos de las capas `bronce` o `plata`, donde residen datos personales (PII) de los usuarios como correos, teléfonos o registros de pago. Así protegemos la privacidad y evitamos la manipulación accidental de datos base.

#### 3. Si tuvieran que montar esto sobre máquinas virtuales, ¿qué sería lo primero que se les complicaría?
* **Explicación:** Lo primero y más crítico sería **el aprovisionamiento, integración y alta disponibilidad del metastore distribuido (Hive Metastore sobre PostgreSQL) junto con la gestión del estado y fallos del clúster de Spark**.
* En Databricks, Unity Catalog es serverless y funciona de inmediato. En IaaS puro tendríamos que instalar PostgreSQL, inicializar los esquemas de tablas de Hive 3.x, configurar el servicio Thrift (puerto 9083), resolver incompatibilidades entre los JARs de Hadoop y AWS SDK, y si un nodo worker colapsa por memoria o fallo de VM, tendríamos que reconfigurar y levantar el nodo a mano, mientras que en Databricks la plataforma gestionada realiza auto-healing transparente sin interrupción del servicio.
