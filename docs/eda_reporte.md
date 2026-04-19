# Reporte de Calidad de Datos - RetailCo (EDA)

## 1. Resumen de Dimensiones

- **Total de registros**: 128.975
- **Total de columnas**: 24
- **Total de duplicados**: 0 - No hay redundancia exacta en las filas

## 2. Identificación de Problemas de Calidad

### Problema 1: Tipos de Datos Inconsistentes

- **Columna**: `Date`
- **Qué es**: el tipo de dato de la columna sale como `str`
- **impacto en el análisis**: al ser texto no podremos realizar filtrados por periodos de tiempo (meses, trimestres), 
calcular la estacionalidad de las ventas, ni ordenar cronológicamente el reporte.

### Problema 2: Valores Nulos (Missing Data)

- **Columna**: `Amount`, `currency` y `fulfilled-by`
- **Qué es**: `Amount` y `currency` tienen un **6.04%** de nulos (7,795 registros). Mientras que la columna 
`fulfilled-by` tiene un **69.5%** de nulos.
- **impacto en el análisis**: 
  1. Uno de los mayores problemas que generan los datos nulos en la columna `Amount`, es el sesgo en el cálculo de 
  los ingresos totales. No podemos saber si estas ventas fueron de costo 0 o se perdió información financiera.
  2. La columna `fulfilled-by` queda prácticamente inútil para análisis logísticos debido a que la gran mayoría de datos
  no están disponibles.

### Problema 3: Valores Inconsistentes o Imposibles

- **Columna**: `Qty` y `Amount`
- **Qué es**: el valor mínimo (`min`) en ambas columnas es 0.00.
- **Impacto Potencial**: Una transacción con cantidad `0` no debería existir en un registro de ventas exitosas. Esto nos
indica que el dataset incluye órdenes canceladas, devueltas o errores de carga. Si no se filtran, el "ticket promedio"
y el "promedio de unidades por orden" (que actualmente es de **0.90**) estarán subestimados y darán una visión falsa del
rendimiento del negocio.

### Problema 4: Ruido en la Estructura (Basura Técnica)

- **Columna**: `Unnamed: 22`
- **Qué es**: Una columna sin nombre con un **38%** de nulos.
- **Impacto Potencial**: Indica una mala exportación de los datos desde la base de datos original. Consume memoria 
innecesaria y ensucia el dataset para el equipo de análisis de datos.

## 3. Análisis de Tipos de Datos Incorrectos

La columna `Date` es de tipo str, lo que impide el análisis de periodos de tiempo, por lo que se sugiere transformarlo 
en el tipo de dato `Datetime`. Por otra parte la columna `ship-postal-code` es de tipo `float64` esto es un error debido
a que el código postal no es una magnitud física. Al ser float, Pandas permite operaciones absurdas como promediar 
códigos postales, además de que los ceros a la izquierda (comunes en muchas regiones y paises) se borran.
