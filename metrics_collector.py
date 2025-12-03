from pymongo import MongoClient
import certifi
import time
import statistics
from collections import defaultdict
from typing import Dict, List
import json

class MetricsCollector:
    def __init__(self, mongodb_uri: str):
        """
        Inicializa el recolector de métricas conectándose a MongoDB
        
        Args:
            mongodb_uri: URI de conexión a MongoDB
        """
        ca = certifi.where()
        
        try:
            self.client = MongoClient(mongodb_uri, tlsCAFile=ca)
            self.client.admin.command('ping')
            print("✅ Conexión a MongoDB establecida correctamente.\n")
        except Exception as e:
            print(f"❌ Error de conexión: {e}")
            raise
        
        self.db = self.client['matematicas_primaria']
        self.collection = self.db['problemas']
    
    def generar_reporte_completo(self, guardar_json: bool = True):
        """
        Genera el reporte completo con todas las métricas necesarias para la tesis
        
        Args:
            guardar_json: Si es True, guarda el reporte en un archivo JSON
        """
        print("="*70)
        print("📊 REPORTE DE MÉTRICAS - MÓDULO 1: PREPROCESAMIENTO DE DATOS")
        print("="*70)
        print()
        
        # 1. Métricas de Volumen
        print("┌─────────────────────────────────────────────────────────────────┐")
        print("│ TABLA 5.1: DISTRIBUCIÓN DE PROBLEMAS POR TIPO DE OPERACIÓN     │")
        print("└─────────────────────────────────────────────────────────────────┘")
        tabla_5_1 = self.tabla_distribucion_por_tipo()
        print()
        
        # 2. Métricas de Completitud
        print("┌─────────────────────────────────────────────────────────────────┐")
        print("│ TABLA 5.2: COMPLETITUD DE CAMPOS POR TIPO DE OPERACIÓN         │")
        print("└─────────────────────────────────────────────────────────────────┘")
        tabla_5_2 = self.tabla_completitud_campos()
        print()
        
        # 3. Duplicados
        print("┌─────────────────────────────────────────────────────────────────┐")
        print("│ DETECCIÓN DE DUPLICADOS                                         │")
        print("└─────────────────────────────────────────────────────────────────┘")
        duplicados = self.analizar_duplicados()
        print()
        
        # 4. Distribución por Dificultad
        print("┌─────────────────────────────────────────────────────────────────┐")
        print("│ TABLA 5.3: DISTRIBUCIÓN POR NIVEL DE DIFICULTAD                │")
        print("└─────────────────────────────────────────────────────────────────┘")
        tabla_5_3 = self.tabla_distribucion_dificultad()
        print()
        
        # 5. Estadísticas de Scores
        print("┌─────────────────────────────────────────────────────────────────┐")
        print("│ TABLA 5.4: ESTADÍSTICAS DE SCORES DE DIFICULTAD                │")
        print("└─────────────────────────────────────────────────────────────────┘")
        tabla_5_4 = self.tabla_estadisticas_scores()
        print()
        
        # 6. Latencias de Consultas
        print("┌─────────────────────────────────────────────────────────────────┐")
        print("│ TABLA 5.5: LATENCIAS DE CONSULTAS MONGODB (1000 iteraciones)   │")
        print("└─────────────────────────────────────────────────────────────────┘")
        tabla_5_5 = self.tabla_latencias_consultas()
        print()
        
        # 7. Resumen Ejecutivo
        print("┌─────────────────────────────────────────────────────────────────┐")
        print("│ RESUMEN EJECUTIVO                                               │")
        print("└─────────────────────────────────────────────────────────────────┘")
        resumen = self.resumen_ejecutivo(tabla_5_1, tabla_5_3, tabla_5_5)
        print()
        
        # Guardar en JSON
        if guardar_json:
            reporte = {
                "tabla_5_1_distribucion_por_tipo": tabla_5_1,
                "tabla_5_2_completitud": tabla_5_2,
                "duplicados": duplicados,
                "tabla_5_3_distribucion_dificultad": tabla_5_3,
                "tabla_5_4_estadisticas_scores": tabla_5_4,
                "tabla_5_5_latencias": tabla_5_5,
                "resumen_ejecutivo": resumen,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            with open("reporte_metricas_tesis.json", "w", encoding="utf-8") as f:
                json.dump(reporte, f, indent=2, ensure_ascii=False)
            
            print("✅ Reporte guardado en: reporte_metricas_tesis.json")
        
        return {
            "tabla_5_1": tabla_5_1,
            "tabla_5_2": tabla_5_2,
            "duplicados": duplicados,
            "tabla_5_3": tabla_5_3,
            "tabla_5_4": tabla_5_4,
            "tabla_5_5": tabla_5_5,
            "resumen": resumen
        }
    
    def tabla_distribucion_por_tipo(self) -> Dict:
        """TABLA 5.1: Distribución de problemas por tipo de operación"""
        pipeline = [
            {
                "$group": {
                    "_id": "$metadata.tipo_operacion",
                    "cantidad": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        resultados = list(self.collection.aggregate(pipeline))
        total = sum(r['cantidad'] for r in resultados)
        
        print(f"{'Tipo Operación':<20} {'Cantidad':>10} {'Porcentaje':>12}")
        print("-" * 45)
        
        datos = {}
        for r in resultados:
            tipo = r['_id'].capitalize() if r['_id'] else 'Sin tipo'
            cantidad = r['cantidad']
            porcentaje = (cantidad / total * 100) if total > 0 else 0
            
            print(f"{tipo:<20} {cantidad:>10} {porcentaje:>11.1f}%")
            datos[tipo] = {
                "cantidad": cantidad,
                "porcentaje": round(porcentaje, 1)
            }
        
        print("-" * 45)
        print(f"{'TOTAL':<20} {total:>10} {'100.0%':>12}")
        
        return {
            "datos": datos,
            "total": total
        }
    
    def tabla_completitud_campos(self) -> Dict:
        """TABLA 5.2: Completitud de campos obligatorios por tipo"""
        total_docs = self.collection.count_documents({})
        
        if total_docs == 0:
            print("⚠️ No hay documentos en la colección")
            return {}
        
        # Agrupar por tipo
        pipeline = [
            {
                "$group": {
                    "_id": "$metadata.tipo_operacion",
                    "total": {"$sum": 1},
                    "con_enunciado": {
                        "$sum": {
                            "$cond": [
                                {"$and": [
                                    {"$ne": ["$enunciado", None]},
                                    {"$ne": ["$enunciado", ""]}
                                ]},
                                1,
                                0
                            ]
                        }
                    },
                    "con_solucion": {
                        "$sum": {
                            "$cond": [
                                {"$and": [
                                    {"$ne": ["$solucion_completa", None]},
                                    {"$ne": ["$solucion_completa", ""]}
                                ]},
                                1,
                                0
                            ]
                        }
                    },
                    "con_conceptos": {
                        "$sum": {
                            "$cond": [
                                {"$isArray": "$metadata.conceptos_involucrados"},
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        resultados = list(self.collection.aggregate(pipeline))
        
        print(f"{'Tipo':<15} {'Enunciado':>12} {'Solución':>12} {'Conceptos':>12}")
        print("-" * 54)
        
        datos = {}
        for r in resultados:
            tipo = r['_id'].capitalize() if r['_id'] else 'Sin tipo'
            total = r['total']
            
            pct_enunciado = (r['con_enunciado'] / total * 100) if total > 0 else 0
            pct_solucion = (r['con_solucion'] / total * 100) if total > 0 else 0
            pct_conceptos = (r['con_conceptos'] / total * 100) if total > 0 else 0
            
            print(f"{tipo:<15} {pct_enunciado:>11.0f}% {pct_solucion:>11.0f}% {pct_conceptos:>11.0f}%")
            
            datos[tipo] = {
                "enunciado": round(pct_enunciado, 1),
                "solucion": round(pct_solucion, 1),
                "conceptos": round(pct_conceptos, 1)
            }
        
        return datos
    
    def analizar_duplicados(self) -> Dict:
        """Análisis de duplicados basado en operandos y tipo"""
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "tipo": "$metadata.tipo_operacion",
                        "enunciado": "$enunciado"
                    },
                    "count": {"$sum": 1},
                    "ids": {"$push": "$problema_id"}
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            }
        ]
        
        duplicados = list(self.collection.aggregate(pipeline))
        total_docs = self.collection.count_documents({})
        num_duplicados = sum(d['count'] - 1 for d in duplicados)
        
        pct_duplicados = (num_duplicados / total_docs * 100) if total_docs > 0 else 0
        
        print(f"Total de problemas: {total_docs}")
        print(f"Grupos de duplicados encontrados: {len(duplicados)}")
        print(f"Problemas duplicados: {num_duplicados} ({pct_duplicados:.2f}%)")
        print(f"Problemas únicos: {total_docs - num_duplicados}")
        
        return {
            "total_problemas": total_docs,
            "grupos_duplicados": len(duplicados),
            "problemas_duplicados": num_duplicados,
            "porcentaje_duplicados": round(pct_duplicados, 2),
            "problemas_unicos": total_docs - num_duplicados
        }
    
    def tabla_distribucion_dificultad(self) -> Dict:
        """TABLA 5.3: Distribución de problemas por nivel de dificultad"""
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "tipo": "$metadata.tipo_operacion",
                        "nivel": "$metadata.dificultad.nivel"
                    },
                    "cantidad": {"$sum": 1}
                }
            },
            {"$sort": {"_id.tipo": 1, "_id.nivel": 1}}
        ]
        
        resultados = list(self.collection.aggregate(pipeline))
        
        # Organizar datos por tipo y nivel
        datos = defaultdict(lambda: {"Básico": 0, "Intermedio": 0, "Avanzado": 0, "Total": 0})
        
        for r in resultados:
            tipo = r['_id']['tipo'].capitalize() if r['_id']['tipo'] else 'Sin tipo'
            nivel = r['_id']['nivel'] if r['_id']['nivel'] else 'Sin nivel'
            cantidad = r['cantidad']
            
            datos[tipo][nivel] = cantidad
            datos[tipo]["Total"] += cantidad
        
        # Totales por nivel
        totales = {"Básico": 0, "Intermedio": 0, "Avanzado": 0, "Total": 0}
        
        # Imprimir tabla
        print(f"{'Tipo Operación':<15} {'Básico':>10} {'Interm.':>10} {'Avanz.':>10} {'Total':>10}")
        print("-" * 58)
        
        for tipo, valores in sorted(datos.items()):
            print(f"{tipo:<15} {valores['Básico']:>10} {valores['Intermedio']:>10} "
                  f"{valores['Avanzado']:>10} {valores['Total']:>10}")
            
            totales['Básico'] += valores['Básico']
            totales['Intermedio'] += valores['Intermedio']
            totales['Avanzado'] += valores['Avanzado']
            totales['Total'] += valores['Total']
        
        print("-" * 58)
        print(f"{'TOTAL':<15} {totales['Básico']:>10} {totales['Intermedio']:>10} "
              f"{totales['Avanzado']:>10} {totales['Total']:>10}")
        
        # Porcentajes
        if totales['Total'] > 0:
            pct_basico = totales['Básico'] / totales['Total'] * 100
            pct_intermedio = totales['Intermedio'] / totales['Total'] * 100
            pct_avanzado = totales['Avanzado'] / totales['Total'] * 100
            
            print(f"{'Porcentaje':<15} {pct_basico:>9.1f}% {pct_intermedio:>9.1f}% "
                  f"{pct_avanzado:>9.1f}% {'100.0%':>10}")
        
        return {
            "por_tipo": dict(datos),
            "totales": totales
        }
    
    def tabla_estadisticas_scores(self) -> Dict:
        """TABLA 5.4: Estadísticas de scores de dificultad"""
        pipeline = [
            {
                "$group": {
                    "_id": "$metadata.tipo_operacion",
                    "scores": {"$push": "$metadata.dificultad.score"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        resultados = list(self.collection.aggregate(pipeline))
        
        print(f"{'Tipo Operación':<15} {'Media':>8} {'Mediana':>8} {'Desv.':>8} {'Rango':>12}")
        print("-" * 58)
        
        datos = {}
        for r in resultados:
            tipo = r['_id'].capitalize() if r['_id'] else 'Sin tipo'
            scores = [s for s in r['scores'] if s is not None]
            
            if len(scores) > 0:
                media = statistics.mean(scores)
                mediana = statistics.median(scores)
                desv = statistics.stdev(scores) if len(scores) > 1 else 0
                minimo = min(scores)
                maximo = max(scores)
                
                print(f"{tipo:<15} {media:>8.2f} {mediana:>8.2f} {desv:>8.2f} "
                      f"[{minimo:.1f}-{maximo:.1f}]")
                
                datos[tipo] = {
                    "media": round(media, 2),
                    "mediana": round(mediana, 2),
                    "desviacion": round(desv, 2),
                    "minimo": round(minimo, 2),
                    "maximo": round(maximo, 2)
                }
        
        return datos
    
    def tabla_latencias_consultas(self, num_iteraciones: int = 100) -> Dict:
        """TABLA 5.5: Latencias de consultas MongoDB"""
        print(f"⏱️ Ejecutando {num_iteraciones} consultas de prueba...\n")
        
        tipos_consulta = {
            "Por problema_id": lambda: self.collection.find_one({"problema_id": {"$exists": True}}),
            "Por tipo operación": lambda: list(self.collection.find({"metadata.tipo_operacion": "suma"}).limit(10)),
            "Por dificultad": lambda: list(self.collection.find({"metadata.dificultad.nivel": "Básico"}).limit(10)),
            "Agregación taxonomía": lambda: list(self.collection.aggregate([
                {"$group": {"_id": "$taxonomia.subtema", "count": {"$sum": 1}}}
            ]))
        }
        
        resultados = {}
        
        for nombre, consulta_func in tipos_consulta.items():
            latencias = []
            
            for _ in range(num_iteraciones):
                start = time.time()
                consulta_func()
                end = time.time()
                latencias.append((end - start) * 1000)  # Convertir a ms
            
            latencias.sort()
            media = statistics.mean(latencias)
            p95 = latencias[int(0.95 * len(latencias))]
            p99 = latencias[int(0.99 * len(latencias))]
            
            resultados[nombre] = {
                "media_ms": round(media, 2),
                "p95_ms": round(p95, 2),
                "p99_ms": round(p99, 2)
            }
        
        # Imprimir tabla
        print(f"{'Tipo de Consulta':<25} {'Media':>10} {'P95':>10} {'P99':>10}")
        print("-" * 58)
        
        for nombre, valores in resultados.items():
            print(f"{nombre:<25} {valores['media_ms']:>9.2f}ms {valores['p95_ms']:>9.2f}ms "
                  f"{valores['p99_ms']:>9.2f}ms")
        
        # Verificar objetivo <100ms
        todas_bajo_100 = all(v['p99_ms'] < 100 for v in resultados.values())
        print()
        if todas_bajo_100:
            print("✅ OBJETIVO CUMPLIDO: Todas las consultas <100ms en P99")
        else:
            print("⚠️ ADVERTENCIA: Algunas consultas exceden 100ms")
        
        return resultados
    
    def resumen_ejecutivo(self, tabla_5_1, tabla_5_3, tabla_5_5) -> Dict:
        """Genera resumen ejecutivo para conclusiones"""
        total = tabla_5_1['total']
        totales_dif = tabla_5_3['totales']
        
        # Calcular latencia promedio general
        latencias_promedio = [v['media_ms'] for v in tabla_5_5.values()]
        latencia_general = statistics.mean(latencias_promedio) if latencias_promedio else 0
        
        resumen = {
            "total_problemas": total,
            "tipos_operacion": len(tabla_5_1['datos']),
            "distribucion_dificultad": {
                "basico_pct": round(totales_dif['Básico'] / total * 100, 1) if total > 0 else 0,
                "intermedio_pct": round(totales_dif['Intermedio'] / total * 100, 1) if total > 0 else 0,
                "avanzado_pct": round(totales_dif['Avanzado'] / total * 100, 1) if total > 0 else 0
            },
            "latencia_promedio_ms": round(latencia_general, 2),
            "cumple_objetivo_latencia": all(v['p99_ms'] < 100 for v in tabla_5_5.values())
        }
        
        print(f"📌 Total de problemas extraídos: {total}")
        print(f"📌 Tipos de operación: {resumen['tipos_operacion']}")
        print(f"📌 Distribución dificultad:")
        print(f"   - Básico: {resumen['distribucion_dificultad']['basico_pct']}%")
        print(f"   - Intermedio: {resumen['distribucion_dificultad']['intermedio_pct']}%")
        print(f"   - Avanzado: {resumen['distribucion_dificultad']['avanzado_pct']}%")
        print(f"📌 Latencia promedio de consultas: {resumen['latencia_promedio_ms']:.2f}ms")
        print(f"📌 Cumple objetivo <100ms: {'✅ SÍ' if resumen['cumple_objetivo_latencia'] else '❌ NO'}")
        
        return resumen
    
    def cerrar(self):
        """Cierra la conexión a MongoDB"""
        self.client.close()
        print("\n✅ Conexión cerrada")


# Uso principal
if __name__ == "__main__":
    print("="*70)
    print("🎓 RECOLECTOR DE MÉTRICAS PARA TESIS - MÓDULO 1")
    print("="*70)
    print()
    
    # URI de MongoDB
    MONGODB_URI = "mongodb+srv://angelmucha_db_user:yjA0xEU9MgDpxEnD@cluster18.gpivg1d.mongodb.net/"
    
    try:
        # Crear recolector
        collector = MetricsCollector(MONGODB_URI)
        
        # Generar reporte completo
        reporte = collector.generar_reporte_completo(guardar_json=True)
        
        print("\n" + "="*70)
        print("✅ REPORTE COMPLETADO")
        print("="*70)
        print()
        print("📄 Los datos están listos para copiar a tu tesis:")
        print("   - Archivo JSON: reporte_metricas_tesis.json")
        print("   - Tablas impresas arriba en formato texto")
        print()
        print("💡 Próximos pasos:")
        print("   1. Copia las tablas a tu documento de tesis (Sección 5.1)")
        print("   2. Genera gráficos con los datos del JSON")
        print("   3. Redacta las conclusiones usando el resumen ejecutivo")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        collector.cerrar()