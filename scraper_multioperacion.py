from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
import time
from typing import List, Dict
import re
import certifi

class MathProblemScraperMulti:
    def __init__(self, mongodb_uri: str = "mongodb+srv://angelmucha_db_user:xxxxxxxxx@cluster18.gpivg1d.mongodb.net/", headless: bool = True, num_operaciones: int = 10):
        """
        Inicializa el scraper con Selenium y la conexión a MongoDB
        
        Args:
            mongodb_uri: URI de conexión a MongoDB
            headless: Si es True, Chrome se ejecuta sin interfaz gráfica
            num_operaciones: Número de operaciones por página (por defecto 10)
        """
        # Configurar Chrome
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Inicializar el driver
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        self.num_operaciones = num_operaciones
        
        # Conexión a MongoDB
        ca = certifi.where()
        
        try:
            self.client = MongoClient(mongodb_uri, tlsCAFile=ca)
            self.client.admin.command('ping')
            print("✅ Conexión a MongoDB establecida correctamente con SSL.")
        except Exception as e:
            print(f"❌ Error crítico de conexión: {e}")
        self.db = self.client['matematicas_primaria']
        self.collection = self.db['problemas']
    
    def cambiar_num_operaciones(self):
        """
        Cambia el número de operaciones en la página usando JavaScript
        """
        try:
            # Esperar a que cargue el select
            self.wait.until(EC.presence_of_element_located((By.NAME, "define")))
            
            # Cambiar el valor usando JavaScript
            script = f"""
            var select = document.querySelector('select[name="define"]');
            if (select) {{
                select.value = '{self.num_operaciones}';
                select.dispatchEvent(new Event('change'));
                return true;
            }}
            return false;
            """
            resultado = self.driver.execute_script(script)
            
            if resultado:
                print(f"   🔢 Número de operaciones cambiado a: {self.num_operaciones}")
                time.sleep(2)  # Esperar a que se recargue la página
                return True
            else:
                print("   ⚠️ No se pudo cambiar el número de operaciones")
                return False
                
        except Exception as e:
            print(f"   ⚠️ Error al cambiar número de operaciones: {e}")
            return False
    
    def _calcular_dificultad_simulada(self, operando1, operando2, tipo_operacion):
        """Calcula dificultad según el tipo de operación"""
        # Lógica adaptada según operación
        if tipo_operacion == 'suma':
            num_pasos = 1 if operando1 <= 10 and operando2 <= 10 else 2
        elif tipo_operacion == 'resta':
            num_pasos = 1 if operando1 <= 20 else 2
        elif tipo_operacion == 'multiplicacion':
            num_pasos = 1 if operando1 <= 5 and operando2 <= 5 else 3
        elif tipo_operacion == 'division':
            num_pasos = 2 if operando1 <= 50 else 3
        else:
            num_pasos = 1
            
        score = (num_pasos * 0.3) + (1 * 0.5)
        
        if score < 1: nivel = "Básico"
        elif score < 2: nivel = "Intermedio"
        else: nivel = "Avanzado"
            
        return {"nivel": nivel, "score": round(score, 2)}

    def limpiar_y_estructurar_datos(self, problemas: List[Dict]) -> List[Dict]:
        """Convierte datos planos a la estructura compleja de la Tesis"""
        import time
        problemas_estructurados = []
        
        for p in problemas:
            tipo = p['tipo']
            operando1 = p['operando1']
            operando2 = p['operando2']
            
            # Generar ID único
            unique_id = f"P_{tipo.upper()}_{operando1}_{operando2}_{int(time.time() * 1000)}"
            dificultad = self._calcular_dificultad_simulada(operando1, operando2, tipo)
            
            # Mapeo de operaciones
            simbolos = {
                'suma': '+',
                'resta': '-',
                'multiplicacion': '×',
                'division': '÷'
            }
            
            subtemas = {
                'suma': 'Adición',
                'resta': 'Sustracción',
                'multiplicacion': 'Multiplicación',
                'division': 'División'
            }
            
            simbolo = simbolos.get(tipo, '+')
            subtema = subtemas.get(tipo, 'Operación')
            
            # ESTRUCTURA EXACTA DE LA TESIS
            nuevo_esquema = {
                "problema_id": unique_id,
                "enunciado": f"Calcula: {operando1} {simbolo} {operando2}",
                "solucion_completa": str(p['resultado']),
                "pasos_detallados": [
                    {"orden": 1, "descripcion": f"Identificar {subtema.lower()}", "operacion": f"{operando1}, {operando2}"},
                    {"orden": 2, "descripcion": "Realizar operación", "calculo": p['operacion']}
                ],
                "taxonomia": {
                    "area_curricular": "Números y operaciones",
                    "subtema": subtema,
                    "competencia": "Resuelve problemas de cantidad",
                    "grado_objetivo": [p['nivel']]
                },
                "metadata": {
                    "dificultad": dificultad,
                    "tiempo_estimado": 1 if tipo in ['suma', 'resta'] else 2,
                    "conceptos_involucrados": [subtema],
                    "tipo_operacion": tipo
                },
                "estadisticas_uso": {
                    "veces_recomendado": 0
                }
            }
            problemas_estructurados.append(nuevo_esquema)
            
        return problemas_estructurados   
    
    def extraer_problemas_sumas(self, url: str, debug: bool = False) -> List[Dict]:
        """Extrae problemas de SUMAS"""
        try:
            print(f"   🌐 Cargando página de SUMAS...")
            self.driver.get(url)
            time.sleep(2)
            
            # Cambiar número de operaciones
            self.cambiar_num_operaciones()
            
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "tr")))
            
            problemas = []
            all_inputs = self.driver.find_elements(By.CSS_SELECTOR, 'input[type="hidden"]')
            
            if debug:
                print(f"   📊 Total inputs encontrados: {len(all_inputs)}")
            
            sumandos_dict = {}
            
            for inp in all_inputs:
                name = inp.get_attribute('name')
                value = inp.get_attribute('value')
                
                if name and value and 'sumando' in name:
                    match = re.match(r'sumando(\d+)(\d)', name)
                    
                    if match:
                        num_problema = int(match.group(1))
                        num_sumando = int(match.group(2))
                        
                        if num_problema not in sumandos_dict:
                            sumandos_dict[num_problema] = {}
                        
                        sumandos_dict[num_problema][num_sumando] = int(value)
            
            for num_problema in sorted(sumandos_dict.keys()):
                sumandos = sumandos_dict[num_problema]
                
                if 1 in sumandos and 2 in sumandos:
                    sumando1 = sumandos[1]
                    sumando2 = sumandos[2]
                    resultado = sumando1 + sumando2
                    
                    problema = {
                        'tipo': 'suma',
                        'operando1': sumando1,
                        'operando2': sumando2,
                        'resultado': resultado,
                        'operacion': f"{sumando1} + {sumando2} = {resultado}",
                        'nivel': self._extraer_nivel_de_url(url),
                        'grupo': self._extraer_grupo_de_url(url),
                        'fuente': url
                    }
                    
                    problemas.append(problema)
            
            return problemas
            
        except Exception as e:
            print(f"   ❌ Error al extraer SUMAS: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def extraer_problemas_restas(self, url: str, debug: bool = False) -> List[Dict]:
        """Extrae problemas de RESTAS"""
        try:
            print(f"   🌐 Cargando página de RESTAS...")
            self.driver.get(url)
            time.sleep(2)
            
            # Cambiar número de operaciones
            self.cambiar_num_operaciones()
            
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "tr")))
            
            problemas = []
            all_inputs = self.driver.find_elements(By.CSS_SELECTOR, 'input[type="hidden"]')
            
            if debug:
                print(f"   📊 Total inputs encontrados: {len(all_inputs)}")
            
            # Para restas: minuendo y sustraendo
            restas_dict = {}
            
            for inp in all_inputs:
                name = inp.get_attribute('name')
                value = inp.get_attribute('value')
                
                if name and value:
                    # Buscar patrones: minuendo11, sustraendo11, etc.
                    match_minuendo = re.match(r'minuendo(\d+)', name)
                    match_sustraendo = re.match(r'sustraendo(\d+)', name)
                    
                    if match_minuendo:
                        num_problema = int(match_minuendo.group(1))
                        if num_problema not in restas_dict:
                            restas_dict[num_problema] = {}
                        restas_dict[num_problema]['minuendo'] = int(value)
                    
                    elif match_sustraendo:
                        num_problema = int(match_sustraendo.group(1))
                        if num_problema not in restas_dict:
                            restas_dict[num_problema] = {}
                        restas_dict[num_problema]['sustraendo'] = int(value)
            
            for num_problema in sorted(restas_dict.keys()):
                datos = restas_dict[num_problema]
                
                if 'minuendo' in datos and 'sustraendo' in datos:
                    minuendo = datos['minuendo']
                    sustraendo = datos['sustraendo']
                    resultado = minuendo - sustraendo
                    
                    problema = {
                        'tipo': 'resta',
                        'operando1': minuendo,
                        'operando2': sustraendo,
                        'resultado': resultado,
                        'operacion': f"{minuendo} - {sustraendo} = {resultado}",
                        'nivel': self._extraer_nivel_de_url(url),
                        'grupo': self._extraer_grupo_de_url(url),
                        'fuente': url
                    }
                    
                    problemas.append(problema)
            
            return problemas
            
        except Exception as e:
            print(f"   ❌ Error al extraer RESTAS: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def extraer_problemas_multiplicacion(self, url: str, debug: bool = False) -> List[Dict]:
        """Extrae problemas de MULTIPLICACIÓN"""
        try:
            print(f"   🌐 Cargando página de MULTIPLICACIÓN...")
            self.driver.get(url)
            time.sleep(2)
            
            # Cambiar número de operaciones
            self.cambiar_num_operaciones()
            
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "tr")))
            
            problemas = []
            all_inputs = self.driver.find_elements(By.CSS_SELECTOR, 'input[type="hidden"]')
            
            if debug:
                print(f"   📊 Total inputs encontrados: {len(all_inputs)}")
                print("   🔍 DEBUG - Primeros 20 inputs:")
                for i, inp in enumerate(all_inputs[:20]):
                    name = inp.get_attribute('name')
                    value = inp.get_attribute('value')
                    print(f"      Input {i+1}: name='{name}', value='{value}'")
            
            # Para multiplicación: factora y factorb (según el HTML que viste)
            mult_dict = {}
            
            for inp in all_inputs:
                name = inp.get_attribute('name')
                value = inp.get_attribute('value')
                
                if name and value:
                    # Buscar patrones: factora1, factorb1, resultado1, etc.
                    match_factora = re.match(r'factora(\d+)', name)
                    match_factorb = re.match(r'factorb(\d+)', name)
                    match_resultado = re.match(r'resultad[oa](\d+)', name)  # resultad[o/a]
                    
                    if match_factora:
                        num_problema = int(match_factora.group(1))
                        if num_problema not in mult_dict:
                            mult_dict[num_problema] = {}
                        mult_dict[num_problema]['factora'] = int(value)
                    
                    elif match_factorb:
                        num_problema = int(match_factorb.group(1))
                        if num_problema not in mult_dict:
                            mult_dict[num_problema] = {}
                        mult_dict[num_problema]['factorb'] = int(value)
                    
                    elif match_resultado:
                        num_problema = int(match_resultado.group(1))
                        if num_problema not in mult_dict:
                            mult_dict[num_problema] = {}
                        mult_dict[num_problema]['resultado'] = int(value)
            
            for num_problema in sorted(mult_dict.keys()):
                datos = mult_dict[num_problema]
                
                if 'factora' in datos and 'factorb' in datos:
                    factora = datos['factora']
                    factorb = datos['factorb']
                    resultado = datos.get('resultado', factora * factorb)  # Usar el resultado del HTML si existe
                    
                    problema = {
                        'tipo': 'multiplicacion',
                        'operando1': factora,
                        'operando2': factorb,
                        'resultado': resultado,
                        'operacion': f"{factora} × {factorb} = {resultado}",
                        'nivel': self._extraer_nivel_de_url(url),
                        'grupo': self._extraer_grupo_de_url(url),
                        'fuente': url
                    }
                    
                    problemas.append(problema)
                    
                    if debug:
                        print(f"   ✅ Problema {num_problema} encontrado: {problema['operacion']}")
            
            return problemas
            
        except Exception as e:
            print(f"   ❌ Error al extraer MULTIPLICACIÓN: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def extraer_problemas_division(self, url: str, debug: bool = False) -> List[Dict]:
        """Extrae problemas de DIVISIÓN"""
        try:
            print(f"   🌐 Cargando página de DIVISIÓN...")
            self.driver.get(url)
            time.sleep(2)
            
            # Cambiar número de operaciones
            self.cambiar_num_operaciones()
            
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "tr")))
            
            problemas = []
            all_inputs = self.driver.find_elements(By.CSS_SELECTOR, 'input[type="hidden"]')
            
            if debug:
                print(f"   📊 Total inputs encontrados: {len(all_inputs)}")
            
            # Para división: dividendo y divisor
            div_dict = {}
            
            for inp in all_inputs:
                name = inp.get_attribute('name')
                value = inp.get_attribute('value')
                
                if name and value:
                    # Buscar patrones: dividendo11, divisor11, etc.
                    match_dividendo = re.match(r'dividendo(\d+)', name)
                    match_divisor = re.match(r'divisor(\d+)', name)
                    
                    if match_dividendo:
                        num_problema = int(match_dividendo.group(1))
                        if num_problema not in div_dict:
                            div_dict[num_problema] = {}
                        div_dict[num_problema]['dividendo'] = int(value)
                    
                    elif match_divisor:
                        num_problema = int(match_divisor.group(1))
                        if num_problema not in div_dict:
                            div_dict[num_problema] = {}
                        div_dict[num_problema]['divisor'] = int(value)
            
            for num_problema in sorted(div_dict.keys()):
                datos = div_dict[num_problema]
                
                if 'dividendo' in datos and 'divisor' in datos and datos['divisor'] != 0:
                    dividendo = datos['dividendo']
                    divisor = datos['divisor']
                    resultado = dividendo // divisor  # División entera
                    
                    problema = {
                        'tipo': 'division',
                        'operando1': dividendo,
                        'operando2': divisor,
                        'resultado': resultado,
                        'operacion': f"{dividendo} ÷ {divisor} = {resultado}",
                        'nivel': self._extraer_nivel_de_url(url),
                        'grupo': self._extraer_grupo_de_url(url),
                        'fuente': url
                    }
                    
                    problemas.append(problema)
            
            return problemas
            
        except Exception as e:
            print(f"   ❌ Error al extraer DIVISIÓN: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def extraer_problemas_automatico(self, url: str, debug: bool = False) -> List[Dict]:
        """
        Detecta automáticamente el tipo de operación según la URL y extrae los problemas
        """
        if 'sumas.php' in url:
            return self.extraer_problemas_sumas(url, debug)
        elif 'restas.php' in url:
            return self.extraer_problemas_restas(url, debug)
        elif 'multiplicar' in url:
            return self.extraer_problemas_multiplicacion(url, debug)
        elif 'divisiones.php' in url:
            return self.extraer_problemas_division(url, debug)
        else:
            print(f"   ⚠️ Tipo de operación no reconocido en URL: {url}")
            return []
    
    def _extraer_nivel_de_url(self, url: str) -> int:
        """Extrae el nivel de la URL"""
        # Intentar diferentes formatos
        match = re.search(r'[?&]n(?:ivel)?=(\d+)', url)
        return int(match.group(1)) if match else 1
    
    def _extraer_grupo_de_url(self, url: str) -> str:
        """Extrae el grupo de la URL"""
        match = re.search(r'[?&]g(?:rupo)?=([^&]+)', url)
        return match.group(1) if match else 'G1'
    
    def mostrar_problemas(self, problemas: List[Dict]):
        """Muestra los problemas en consola adaptados al esquema de la Tesis"""
        if not problemas:
            print("❌ No hay problemas para mostrar")
            return
        
        print(f"\n{'='*60}")
        print(f"📊 TOTAL DE PROBLEMAS ESTRUCTURADOS: {len(problemas)}")
        print(f"{'='*60}\n")
        
        for i, p in enumerate(problemas, 1):
            tax = p.get('taxonomia', {})
            meta = p.get('metadata', {})
            dif = meta.get('dificultad', {})
            
            print(f"Problema #{i} [ID: {p.get('problema_id')}]")
            print(f"  📖 Enunciado: {p.get('enunciado')}")
            print(f"  📝 Solución: {p.get('solucion_completa')}")
            print(f"  📚 Taxonomía: {tax.get('area_curricular')} > {tax.get('subtema')}")
            print(f"  🎯 Competencia: {tax.get('competencia')}")
            print(f"  🧠 Dificultad: {dif.get('nivel')} (Score: {dif.get('score')})")
            print(f"  ⏱️ Tiempo est: {meta.get('tiempo_estimado')} min")
            print(f"  🔢 Tipo: {meta.get('tipo_operacion', 'N/A').upper()}")
            
            pasos = p.get('pasos_detallados', [])
            if pasos:
                print("  👣 Pasos de resolución:")
                for paso in pasos:
                    print(f"     {paso['orden']}. {paso['descripcion']} -> {paso.get('operacion', paso.get('calculo'))}")
            
            print(f"  {'-'*58}")
            
    def guardar_en_mongodb(self, problemas: List[Dict]) -> int:
        """Guarda los problemas estructurados en MongoDB"""
        if not problemas:
            print("No hay problemas para guardar")
            return 0
        
        try:
            insertados = 0
            for p in problemas:
                resultado = self.collection.update_one(
                    {'problema_id': p['problema_id']},
                    {'$set': p},
                    upsert=True
                )
                
                if resultado.upserted_id or resultado.modified_count > 0:
                    insertados += 1
            
            print(f"✓ {insertados} problemas procesados en MongoDB")
            print(f"  Total en colección: {self.collection.count_documents({})}")
            return insertados
            
        except Exception as e:
            print(f"Error al guardar en MongoDB: {e}")
            return 0
    
    def scrapear_multiples_urls(self, urls: List[str], guardar_db: bool = False, debug: bool = False):
        """
        Scrapea múltiples URLs de diferentes tipos de operaciones
        
        Args:
            urls: Lista de URLs a scrapear
            guardar_db: Si es True, guarda en MongoDB
            debug: Si es True, muestra información de debugging
        """
        total_problemas = 0
        todos_los_problemas = []
        
        for i, url in enumerate(urls, 1):
            print(f"\n{'='*60}")
            print(f"📥 [{i}/{len(urls)}] Scrapeando URL...")
            print(f"   {url}")
            print(f"{'='*60}")
            
            problemas = self.extraer_problemas_automatico(url, debug=debug)
            print(f"   ✅ Extraídos: {len(problemas)} problemas")
            
            problemas_limpios = self.limpiar_y_estructurar_datos(problemas)
            print(f"   🧹 Después de estructurar: {len(problemas_limpios)} problemas")
            
            if guardar_db:
                insertados = self.guardar_en_mongodb(problemas_limpios)
                total_problemas += insertados
            else:
                todos_los_problemas.extend(problemas_limpios)
                total_problemas += len(problemas_limpios)
            
            # Pausa para no sobrecargar el servidor
            time.sleep(1)
        
        if not guardar_db:
            print(f"\n{'='*60}")
            print("📋 RESUMEN DE TODOS LOS PROBLEMAS EXTRAÍDOS")
            print(f"{'='*60}")
            self.mostrar_problemas(todos_los_problemas)
        
        print(f"\n✅ Proceso completado. Total de problemas: {total_problemas}")
        
        # Resumen por tipo
        if todos_los_problemas:
            tipos = {}
            for p in todos_los_problemas:
                tipo = p['metadata']['tipo_operacion']
                tipos[tipo] = tipos.get(tipo, 0) + 1
            
            print("\n📊 Distribución por tipo:")
            for tipo, cantidad in tipos.items():
                print(f"   {tipo.upper()}: {cantidad} problemas")
    
    def cerrar(self):
        """Cierra el navegador y la conexión a MongoDB"""
        self.driver.quit()
        self.client.close()


# Ejemplo de uso
if __name__ == "__main__":
    print("=== SCRAPER MULTIOPERACIÓN - MATEMÁTICAS PRIMARIA ===\n")
    
    # Configuración
    scraper = MathProblemScraperMulti(
        mongodb_uri="mongodb+srv://angelmucha_db_user:yjA0xEU9MgDpxEnD@cluster18.gpivg1d.mongodb.net/",
        headless=True,
        num_operaciones=10  # ¡AHORA CON 10 OPERACIONES!
    )
    
    try:
        # URLs de ejemplo para todas las operaciones
        urls_ejemplo = [
            
            # DIVISIÓN
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=1",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=2",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=3",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=4",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=5",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=6",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=7",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=8",
            "https://matesdeprimaria.es/divisiones.php?hacer-division&g=G30&n=9",

        ]
        
        print(f"🎯 Configurado para extraer {scraper.num_operaciones} operaciones por página")
        print(f"📋 Total de URLs a procesar: {len(urls_ejemplo)}\n")
        
        # Modo de operación
        modo = input("Selecciona modo:\n1. Solo visualizar\n2. Guardar en MongoDB\nOpción (1/2): ")
        
        if modo == '2':
            confirmar = input("⚠️ ¿Confirmas guardar en MongoDB? (s/n): ")
            guardar = confirmar.lower() == 's'
        else:
            guardar = False
        
        # Ejecutar scraping
        scraper.scrapear_multiples_urls(
            urls=urls_ejemplo,
            guardar_db=guardar,
            debug=True  # Cambia a True para ver más detalles
        )

    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        scraper.cerrar()

        print("\n👋 Proceso finalizado")
