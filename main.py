import requests
import json
from deepdiff import DeepDiff
from datetime import datetime, timedelta
import time
import re
import os
from collections import defaultdict
import concurrent.futures
import threading
from collections import OrderedDict
import random

CATALOGAPIPERF = 'catalog-api-perf.aeocp4.labs.gvp.telefonica.com'
CATALOGAPIFUNC = 'catalog-api.labs.gvp.telefonica.com'
CONTENTAPIPERF = 'contentapi-performance.labs.gvp.telefonica.com'
CONTENTAPIFUNC = 'contentapi-preprod.labs.gvp.telefonica.com'

arrOrderBy = ['PID','AVAILABLE_FROM_UTC','AVAILABLE_UNTIL_UTC','TITLE','SHORT_DESCRIPTION','AVERAGE_RATING','TOTAL_VOTES','AGE_RATING_PID','CONTENT_TYPE','DESCRIPTION','DURATION','YEAR','VIEWS','RECENT_VIEWS','RELEASE_DATE','COUNTRY','ORIGINAL_TITLE','']

arrContentType = [["AGE"], ["MAT"] ,["APP"], ["AVA"], ["CHA"], ["CMP"], ["COR"], ["EPI"], ["GEN"], ["LCH"], ["LNK"], ["LPR"], ["LSC"], ["LSE"], ["LSR"], ["MED"], ["MOV"], ["PER"], ["SEA"], ["SER"], ["STL"], ["SUB"], ["THB"]]
arrType = [['AGE']]
valores = ["LCH100", "LCH101", "LCH102", "LCH103", "LCH104", "LCH105", "LCH106", "LCH107", "LCH108", "LCH109", "LCH11", "LCH110", "LCH111", "LCH112", "LCH113", "LCH114", "LCH115", "LCH116", "LCH117", "LCH118", "LCH12", "LCH120", "LCH1211", "LCH122", "LCH123", "LCH1237", "LCH1242", "LCH1252", "LCH1253", "LCH127", "LCH128", "LCH1287", "LCH129", "LCH130", "LCH131", "LCH1313", "LCH1314", "LCH132", "LCH133", "LCH135", "LCH136", "LCH138", "LCH14", "LCH142", "LCH143", "LCH144", "LCH145", "LCH146", "LCH147", "LCH148", "LCH149", "LCH15", "LCH150", "LCH15025", "LCH151", "LCH152", "LCH15267", "LCH15268", "LCH154", "LCH155", "LCH156", "LCH157", "LCH158", "LCH159", "LCH160", "LCH17", "LCH18", "LCH182", "LCH183", "LCH184", "LCH185", "LCH186", "LCH187", "LCH189", "LCH191", "LCH192", "LCH193", "LCH194", "LCH195", "LCH196", "LCH198", "LCH199", "LCH1995", "LCH1996", "LCH20", "LCH2000", "LCH202", "LCH2023", "LCH2025", "LCH2028", "LCH203", "LCH204", "LCH2044", "LCH2045", "LCH2050", "LCH2055", "LCH2062", "LCH2064", "LCH2066", "LCH2067", "LCH2069", "LCH207", "LCH2070", "LCH2071", "LCH2073", "LCH2077", "LCH2078", "LCH208", "LCH2080", "LCH2081", "LCH209", "LCH2090", "LCH2091", "LCH2093", "LCH2100", "LCH2102", "LCH2104", "LCH2105", "LCH2106", "LCH211", "LCH213", "LCH214", "LCH215", "LCH216", "LCH217", "LCH2172", "LCH218", "LCH219", "LCH2199", "LCH2200", "LCH23", "LCH2398", "LCH24", "LCH2439", "LCH2440", "LCH2441", "LCH2442", "LCH25", "LCH27", "LCH28", "LCH29", "LCH30", "LCH31", "LCH32", "LCH33", "LCH34", "LCH35", "LCH35988", "LCH35989", "LCH36", "LCH36001", "LCH3765", "LCH3766", "LCH3767", "LCH3768", "LCH38", "LCH39", "LCH40", "LCH4057", "LCH41", "LCH42", "LCH43", "LCH44", "LCH442", "LCH443", "LCH444", "LCH459", "LCH46", "LCH460", "LCH461", "LCH462", "LCH463", "LCH464", "LCH465", "LCH466", "LCH467", "LCH4677", "LCH4678", "LCH4685", "LCH4686", "LCH469", "LCH47", "LCH471", "LCH49", "LCH50", "LCH51", "LCH52", "LCH53", "LCH54", "LCH55", "LCH56", "LCH58", "LCH5838", "LCH5855", "LCH5858", "LCH5860", "LCH5861", "LCH5864", "LCH5867", "LCH59", "LCH60", "LCH61", "LCH62", "LCH64", "LCH65", "LCH653", "LCH654", "LCH66", "LCH67", "LCH677", "LCH6873", "LCH6874", "LCH69", "LCH70", "LCH71", "LCH72", "LCH7237", "LCH7250", "LCH7350", "LCH74", "LCH7446", "LCH75", "LCH76", "LCH7658", "LCH7659", "LCH7660", "LCH7661", "LCH7662", "LCH7663", "LCH7664", "LCH7665", "LCH7666", "LCH7667", "LCH7668", "LCH7669", "LCH7670", "LCH7671", "LCH7672", "LCH7673", "LCH7674", "LCH77", "LCH770", "LCH771", "LCH772", "LCH773", "LCH7796", "LCH7797", "LCH7841", "LCH7842", "LCH7843", "LCH79", "LCH7906", "LCH7981", "LCH7982", "LCH7983", "LCH7984", "LCH8", "LCH80", "LCH81", "LCH82", "LCH8330", "LCH8331", "LCH8409", "LCH85", "LCH854", "LCH855", "LCH856", "LCH8561", "LCH8562", "LCH8563", "LCH8564", "LCH8565", "LCH8566", "LCH8567", "LCH857", "LCH86", "LCH8628", "LCH8642", "LCH8645", "LCH87", "LCH883", "LCH887", "LCH888", "LCH89", "LCH891", "LCH9", "LCH90", "LCH9032", "LCH913", "LCH92", "LCH928", "LCH929", "LCH93", "LCH930", "LCH931", "LCH932", "LCH933", "LCH934", "LCH935", "LCH936", "LCH937", "LCH94", "LCH946", "LCH948", "LCH949", "LCH95", "LCH97", "LCH98"]
valoresPageSchedules = ["LCH102", "LCH104", "LCH108", "LCH111", "LCH112", "LCH119", "LCH1193", "LCH1194", "LCH120", "LCH1210", "LCH1211", "LCH123", "LCH1237", "LCH1242", "LCH1252", "LCH1253", "LCH126", "LCH128", "LCH1313", "LCH1314", "LCH1315", "LCH1316", "LCH132", "LCH1358", "LCH136", "LCH1360", "LCH142", "LCH143", "LCH144", "LCH147", "LCH149", "LCH150", "LCH151", "LCH152", "LCH154", "LCH155", "LCH157", "LCH158", "LCH17", "LCH182", "LCH183", "LCH191", "LCH196", "LCH197", "LCH1995", "LCH1996", "LCH2000", "LCH2023", "LCH2025", "LCH2028", "LCH204", "LCH2042", "LCH2044", "LCH2045", "LCH2050", "LCH2055", "LCH2064", "LCH2066", "LCH2067", "LCH2070", "LCH2071", "LCH2072", "LCH2073", "LCH2075", "LCH208", "LCH2080", "LCH2081", "LCH2088", "LCH2090", "LCH2091", "LCH2093", "LCH2100", "LCH2102", "LCH2103", "LCH2104", "LCH2105", "LCH2106", "LCH211", "LCH213", "LCH214", "LCH215", "LCH216", "LCH217", "LCH2172", "LCH2173", "LCH218", "LCH219", "LCH2199", "LCH2200", "LCH2393", "LCH2397", "LCH2398", "LCH2399", "LCH2418", "LCH2426", "LCH2427", "LCH2428", "LCH2429", "LCH2430", "LCH2432", "LCH2433", "LCH2434", "LCH2435", "LCH2436", "LCH2437", "LCH2439", "LCH2440", "LCH2441", "LCH2442", "LCH2443", "LCH2444", "LCH2449", "LCH2460", "LCH2716", "LCH2777", "LCH2791", "LCH2792", "LCH2793", "LCH2794", "LCH2795", "LCH28", "LCH2817", "LCH2818", "LCH2887", "LCH2889", "LCH29", "LCH3081", "LCH3082", "LCH3083", "LCH3084", "LCH3085", "LCH3086", "LCH3087", "LCH3088", "LCH3089", "LCH3090", "LCH3099", "LCH3100", "LCH3101", "LCH3130", "LCH3131", "LCH3132", "LCH3133", "LCH3134", "LCH3135", "LCH3136", "LCH3137", "LCH3138", "LCH3144", "LCH3145", "LCH3146", "LCH3147", "LCH3148", "LCH3149", "LCH3150", "LCH3151", "LCH3152", "LCH3153", "LCH3154", "LCH3155", "LCH3156", "LCH3157", "LCH3158", "LCH3159", "LCH3160", "LCH3161", "LCH3162", "LCH3163", "LCH3164", "LCH3165", "LCH3166", "LCH3167", "LCH3168", "LCH3169", "LCH3170", "LCH3171", "LCH3180", "LCH3181", "LCH3182", "LCH3183", "LCH3184", "LCH3185", "LCH3186", "LCH3187", "LCH3188", "LCH3189", "LCH3190", "LCH3191", "LCH3192", "LCH3193", "LCH3194", "LCH3196", "LCH32", "LCH3261", "LCH3286", "LCH3287", "LCH3288", "LCH3343", "LCH3372", "LCH3373", "LCH3374", "LCH3375", "LCH3376", "LCH3377", "LCH3378", "LCH3379", "LCH3380", "LCH3381", "LCH3382", "LCH3383", "LCH3384", "LCH3385", "LCH3386", "LCH3387", "LCH3388", "LCH3389", "LCH3390", "LCH3391", "LCH3392", "LCH3393", "LCH3394", "LCH3395", "LCH3396", "LCH3397", "LCH3398", "LCH3399", "LCH3400", "LCH3401", "LCH3469", "LCH3470", "LCH3471", "LCH3472", "LCH3473", "LCH3474", "LCH3475", "LCH3476", "LCH3477", "LCH3478", "LCH3479", "LCH3480", "LCH3481", "LCH3544", "LCH3545", "LCH3546", "LCH3547", "LCH3548", "LCH3549", "LCH3550", "LCH36", "LCH3618", "LCH3619", "LCH3620", "LCH3648", "LCH3649", "LCH3650", "LCH3651", "LCH3652", "LCH3653", "LCH37", "LCH3714", "LCH3716", "LCH3734", "LCH3736", "LCH3737", "LCH3739", "LCH3740", "LCH3741", "LCH3742", "LCH3743", "LCH3744", "LCH3746", "LCH3750", "LCH3751", "LCH3764", "LCH3765", "LCH3767", "LCH3808", "LCH3809", "LCH3810", "LCH3811", "LCH3812", "LCH3813", "LCH3814", "LCH3815", "LCH3816", "LCH3817", "LCH3818", "LCH3819", "LCH3820", "LCH3823", "LCH3824", "LCH3825", "LCH3826", "LCH3827", "LCH3828", "LCH3829", "LCH3830", "LCH3831", "LCH3832", "LCH3833", "LCH3834", "LCH3835", "LCH3836", "LCH3837", "LCH3845", "LCH3846", "LCH3847", "LCH3848", "LCH3849", "LCH3850", "LCH3851", "LCH3852", "LCH3853", "LCH3854", "LCH3855", "LCH3856", "LCH3857", "LCH3858", "LCH3859", "LCH3860", "LCH3861", "LCH3862", "LCH3870", "LCH3871", "LCH3872", "LCH3873", "LCH3874", "LCH39", "LCH3914", "LCH3915", "LCH3916", "LCH3917", "LCH3928", "LCH3929", "LCH3930", "LCH3931", "LCH3932", "LCH3933", "LCH3934", "LCH3958", "LCH3959", "LCH3960", "LCH3966", "LCH3967", "LCH4033", "LCH4034", "LCH4035", "LCH4039", "LCH4040", "LCH4041", "LCH4042", "LCH4044", "LCH4050", "LCH4066", "LCH4068", "LCH4069", "LCH4070", "LCH4071", "LCH4073", "LCH4074", "LCH4075", "LCH4076", "LCH4078", "LCH4079", "LCH4080", "LCH4084", "LCH4085", "LCH4086", "LCH4089", "LCH4090", "LCH4091", "LCH4092", "LCH4093", "LCH4094", "LCH4097", "LCH4103", "LCH4104", "LCH4106", "LCH4107", "LCH4108", "LCH4112", "LCH4117", "LCH4122", "LCH4123", "LCH4125", "LCH4131", "LCH4132", "LCH4133", "LCH4134", "LCH4137", "LCH42", "LCH459", "LCH460", "LCH461", "LCH462", "LCH464", "LCH466", "LCH467", "LCH469", "LCH48", "LCH49", "LCH51", "LCH52", "LCH5478", "LCH5479", "LCH5480", "LCH5481", "LCH5486", "LCH5487", "LCH5488", "LCH5489", "LCH5490", "LCH5492", "LCH5493", "LCH5494", "LCH5540", "LCH5552", "LCH5553", "LCH5554", "LCH5555", "LCH5556", "LCH5557", "LCH5573", "LCH5574", "LCH5575", "LCH5576", "LCH5577", "LCH5578", "LCH5580", "LCH5581", "LCH5582", "LCH5583", "LCH5584", "LCH5585", "LCH5586", "LCH5587", "LCH5588", "LCH5589", "LCH5590", "LCH5596", "LCH56", "LCH57", "LCH60", "LCH61", "LCH649", "LCH653", "LCH677", "LCH70", "LCH75", "LCH76", "LCH769", "LCH770", "LCH771", "LCH772", "LCH773", "LCH79", "LCH81", "LCH854", "LCH855", "LCH857", "LCH86", "LCH867", "LCH870", "LCH875", "LCH88", "LCH891", "LCH894", "LCH897", "LCH900", "LCH902", "LCH904", "LCH907", "LCH911", "LCH912", "LCH914", "LCH917", "LCH919", "LCH920", "LCH921", "LCH928", "LCH929", "LCH930", "LCH931", "LCH932", "LCH933", "LCH934", "LCH936", "LCH937", "LCH949", "LCH95", "LCH98"]


arrayImageDescription = ["None","Cover","VideoFrame","Banner","Icon","Background","Logo","RegistrationBanner","PaymentMethodImages","LandscapeCover","LandscapeArt","LandscapeArt"]

json1 = {}
json2 = {}

def crearJson(diccionario, archivo):
    with open(archivo, 'w', encoding='utf-8') as file:
        json.dump(diccionario, file, indent=4, ensure_ascii=False)

def crearJsonOrderBy(diccionario, carpeta):
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

    for clave, contenido in diccionario.items():
        archivo_path = os.path.join(carpeta, f"{clave}.json")
        with open(archivo_path, 'w') as file:
            json.dump({clave: contenido}, file, indent=4)

def guardarDiferencias(diferencias, urlCatalog, urlContent):
    # Crea el directorio si no existe
    os.makedirs("orderBy2", exist_ok=True)

    agrupadas_por_path = {}
    for path, diff in diferencias.items():
        if "['Content']" in path:
            primera_parte = path.split("['Content']")[0]
        else:
            primera_parte = "''"

        if not primera_parte or primera_parte == "root":
            primera_parte = "empty"
        if primera_parte not in agrupadas_por_path:
            agrupadas_por_path[primera_parte] = {}
        agrupadas_por_path[primera_parte][path] = diff         
    for primera_parte, diffs in agrupadas_por_path.items():
        print ("RAMON")
        print (primera_parte)
        filename = f"{primera_parte}.json"
        filepath = os.path.join("orderBy", filename)

        datos_existentes = {}
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    datos_existentes = json.load(f)
            except json.JSONDecodeError:
                datos_existentes = {}

        if "url" not in datos_existentes:
            datos_existentes["url"] = {
                'urlCatalog': urlCatalog,
                'urlContent': urlContent
            }

        if primera_parte not in datos_existentes:
            datos_existentes[primera_parte] = {}

        datos_existentes[primera_parte].update(diffs)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(datos_existentes, f, ensure_ascii=False, indent=2)


lock = threading.Lock() 

def crearJsonContent(diccionario, carpeta_base, Pid, urlCatalog, urlContent):
    if not isinstance(diccionario, dict):
        raise ValueError("Se esperaba un diccionario, pero se recibió otro tipo de dato.")
    
    if not os.path.exists(carpeta_base):
        os.makedirs(carpeta_base)

    prefijo = Pid[:3]
    archivo_path = os.path.join(carpeta_base, f"{prefijo}.json")
    
    with lock:  
        if os.path.exists(archivo_path):
            try:
                with open(archivo_path, 'r', encoding='utf-8') as file:
                    datos_existentes = json.load(file)
            except json.JSONDecodeError:
                datos_existentes = {}
        else:
            datos_existentes = {}

        if prefijo not in datos_existentes:
            datos_existentes[prefijo] = {}

        if "url" not in datos_existentes:
            datos_existentes["url"] = {
                'urlCatalog': urlCatalog,
                'urlContent': urlContent
            }

        if Pid not in datos_existentes[prefijo]:
            datos_existentes[prefijo][Pid] = {}

        for clave, contenido in diccionario.items():
            if clave not in datos_existentes[prefijo][Pid]:
                datos_existentes[prefijo][Pid][clave] = {}
            datos_existentes[prefijo][Pid][clave].update(contenido)

        with open(archivo_path, 'w', encoding='utf-8') as file:
            json.dump(datos_existentes, file, indent=4, ensure_ascii=False)

def crearJsonReleated(diccionario, carpeta_base, Pid):
    try:
        if not isinstance(diccionario, dict):
            raise ValueError("Se esperaba un diccionario, pero se recibió otro tipo de dato.")
        
        if not os.path.exists(carpeta_base):
            os.makedirs(carpeta_base)
            print(f"Carpeta creada: {carpeta_base}")

        all_data = {}

        for clave, contenido in diccionario.items():
            if not isinstance(contenido, dict):
                raise ValueError(f"Se esperaba que el contenido de {clave} fuera un diccionario.")
            for subclave, data in contenido.items():
                prefijo = Pid[:3]
                if prefijo not in all_data:
                    all_data[prefijo] = {}
                if Pid not in all_data[prefijo]:
                    all_data[prefijo][Pid] = {}
                if clave not in all_data[prefijo][Pid]:
                    all_data[prefijo][Pid][clave] = {}
                all_data[prefijo][Pid][clave][subclave] = data

        for prefijo, datos in all_data.items():
            archivo_path = os.path.join(carpeta_base, f"{prefijo}.json")
            
            if os.path.exists(archivo_path):
                with open(archivo_path, 'r', encoding='utf-8') as file:
                    datos_existentes = json.load(file)
                    print(f"Archivo existente encontrado: {archivo_path}")
                datos_existentes.update(datos)
            else:
                datos_existentes = datos
                print(f"Creando nuevo archivo: {archivo_path}")
            
            with open(archivo_path, 'w', encoding='utf-8') as file:
                json.dump(datos_existentes, file, ensure_ascii=False, indent=4)
                
    except Exception as e:
        print(f"Error: {e}")

def crearJsonChildren(diccionario, carpeta_base, Pid):
    all_data = {}

    if not isinstance(diccionario, dict):
        prefijo = Pid[:3]
        if prefijo not in all_data:
            all_data[prefijo] = {}
        all_data[prefijo][Pid] = "no hay diferencia"
    else:
        if not os.path.exists(carpeta_base):
            os.makedirs(carpeta_base)

        for clave, contenido in diccionario.items():
            for data in contenido.items():
                prefijo = Pid[:3]
                if prefijo not in all_data:
                    all_data[prefijo] = {}
                if Pid not in all_data[prefijo]:
                    all_data[prefijo][Pid] = {}
                all_data[prefijo][Pid][clave] = data

    for prefijo, datos in all_data.items():
        archivo_path = os.path.join(carpeta_base, f"{prefijo}.json")

        os.makedirs(os.path.dirname(archivo_path), exist_ok=True)

        if os.path.exists(archivo_path):
            with open(archivo_path, 'r') as file:
                datos_existentes = json.load(file)
            datos_existentes.update(datos)
        else:
            datos_existentes = datos
        
        with open(archivo_path, 'w') as file:
            json.dump(datos_existentes, file, indent=4)

def cargarJson(archivo):
    with open(archivo, 'r') as f:
        return json.load(f)

def comparaJsonAntiguo(data1, data2):
    if isinstance(data1, str):
        data1 = json.loads(data1)
    if isinstance(data2, str):
        data2 = json.loads(data2)
    
    differences = DeepDiff(data1, data2, ignore_order=True).to_dict()
    
    output_diff = {}

    if not differences:
        return "No hay diferencias"

    relevant_diff_types = [
        "values_changed", "dictionary_item_added", "dictionary_item_removed", 
        "type_changes", "iterable_item_added", "iterable_item_removed"
    ]

    for diff_type in relevant_diff_types:
        if diff_type in differences:
            changes = differences[diff_type]
            if isinstance(changes, dict):
                for key, change in changes.items():
                    parts = key.split("']")
                    if len(parts) > 1:
                        main_key = parts[0].split("['")[1]
                    else:
                        main_key = key.split("['")[1]

                    if main_key not in output_diff:
                        output_diff[main_key] = {}

                    if diff_type == "values_changed":
                        output_diff[main_key][key] = {
                            "catalog-api": change["old_value"],
                            "contentapi": change["new_value"]
                        }
                    elif diff_type == "dictionary_item_added":
                        output_diff[main_key][key] = {
                            "contentapi": change,
                            "catalog-api": "No está presente"
                        }
                    elif diff_type == "dictionary_item_removed":
                        output_diff[main_key][key] = {
                            "catalog-api": change,
                            "contentapi": "No está presente"
                        }
                    elif diff_type == "type_changes":
                        output_diff[main_key][key] = {
                            "catalog-api": change["old_value"],
                            "contentapi": change["new_value"]
                        }
            elif isinstance(changes, list):
                for change in changes:
                    key = change['key']
                    parts = key.split("']")
                    if len(parts) > 1:
                        main_key = parts[0].split("['")[1]
                    else:
                        main_key = key.split("['")[1]

                    if main_key not in output_diff:
                        output_diff[main_key] = {}

                    if diff_type == "iterable_item_added":
                        output_diff[main_key][key] = {
                            "contentapi": change["value"],
                            "catalog-api": "No está presente"
                        }
                    elif diff_type == "iterable_item_removed":
                        output_diff[main_key][key] = {
                            "catalog-api": change["value"],
                            "contentapi": "No está presente"
                        }

    return output_diff

def comparaJson(d1, d2, filtros="",path=""):
    diferencias = {}

    # Verificar si ambos son diccionarios antes de intentar acceder a sus claves
    if not isinstance(d1, dict) or not isinstance(d2, dict):
        diferencias[path] = {
            "catalog-api": d1,
            "contentapi": d2
        }
        return diferencias

    # Comparar claves comunes
    for key in d1.keys() & d2.keys():
        new_path = f"{path}['{key}']" if path else key
        
        if any(filtro in new_path for filtro in filtros):
            continue
        
        if isinstance(d1[key], dict) and isinstance(d2[key], dict):
            sub_diff = comparaJson(d1[key], d2[key], new_path)
            if sub_diff:
                diferencias.update(sub_diff)
        elif isinstance(d1[key], list) and isinstance(d2[key], list):
            if len(d1[key]) == len(d2[key]):
                for i in range(len(d1[key])):
                    new_list_path = f"{new_path}[{i}]"
                    if isinstance(d1[key][i], dict) and isinstance(d2[key][i], dict):
                        sub_diff = comparaJson(d1[key][i], d2[key][i], new_list_path)
                        if sub_diff:
                            diferencias.update(sub_diff)
                    elif d1[key][i] != d2[key][i]:
                        diferencias[new_list_path] = {
                            "catalog-api": d1[key][i],
                            "contentapi": d2[key][i]
                        }
            else:
                diferencias[new_path] = {
                    "catalog-api": d1[key],
                    "contentapi": d2[key]
                }
        else:
            if d1[key] != d2[key]:
                diferencias[new_path] = {
                    "catalog-api": d1[key],
                    "contentapi": d2[key]
                }

    # Claves en d1 pero no en d2
    for key in d1.keys() - d2.keys():
        new_path = f"{path}['{key}']" if path else key
        
        if any(filtro in new_path for filtro in filtros):
            continue

        diferencias[new_path] = {
            "catalog-api": d1[key],
            "contentapi": "No está presente"
        }
    
    # Claves en d2 pero no en d1
    for key in d2.keys() - d1.keys():
        new_path = f"{path}['{key}']" if path else key
        
        if any(filtro in new_path for filtro in filtros):
            continue

        diferencias[new_path] = {
            "catalog-api": "No está presente",
            "contentapi": d2[key]
        }
    
    return diferencias


def comparaJsonSchedule(data1, data2, filtros=""):
    if isinstance(data1, str):
        data1 = json.loads(data1)
    if isinstance(data2, str):
        data2 = json.loads(data2)

    def comparar_recursivo(d1, d2, path=""):
        diferencias = {}

        for key in d1.keys() & d2.keys():
            new_path = f"{path}['{key}']" if path else key
            
            if any(filtro in new_path for filtro in filtros):
                continue
            
            if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                sub_diff = comparar_recursivo(d1[key], d2[key], new_path)
                if sub_diff:
                    diferencias.update(sub_diff)
            elif isinstance(d1[key], list) and isinstance(d2[key], list):
                if len(d1[key]) == len(d2[key]):
                    for i in range(len(d1[key])):
                        new_list_path = f"{new_path}[{i}]"
                        if isinstance(d1[key][i], dict) and isinstance(d2[key][i], dict):
                            sub_diff = comparar_recursivo(d1[key][i], d2[key][i], new_list_path)
                            if sub_diff:
                                diferencias.update(sub_diff)
                        elif d1[key][i] != d2[key][i]:
                            diferencias[new_list_path] = {
                                "catalog-api": d1[key][i],
                                "contentapi": d2[key][i]
                            }
                else:
                    diferencias[new_path] = {
                        "catalog-api": d1[key],
                        "contentapi": d2[key]
                    }
            else:
                if d1[key] != d2[key]:
                    diferencias[new_path] = {
                        "catalog-api": d1[key],
                        "contentapi": d2[key]
                    }

        # Claves en contentapi pero no en catalog-api
        for key in d1.keys() - d2.keys():
            new_path = f"{path}['{key}']" if path else key
            
            if any(filtro in new_path for filtro in filtros):
                continue

            diferencias[new_path] = {
                "contentapi": d1[key],
                "catalog-api": "No está presente"
            }

        # Claves en catalog-api pero no en contentapi
        for key in d2.keys() - d1.keys():
            new_path = f"{path}['{key}']" if path else key
            
            if any(filtro in new_path for filtro in filtros):
                continue

            diferencias[new_path] = {
                "contentapi": "No está presente",
                "catalog-api": d2[key]
            }

        return diferencias

    # Lógica específica para comparar "Content" usando "Pid"
    if "Content" in data1 and "Content" in data2:
        diferencias = {}

        # Convertir listas de "Content" a diccionarios indexados por "Pid"
        content_dict1 = {item["Pid"]: item for item in data1["Content"]}
        content_dict2 = {item["Pid"]: item for item in data2["Content"]}

        # Comparar elementos que están en ambas listas
        for pid in content_dict1.keys() & content_dict2.keys():
            path = f"Content['{pid}']"
            print (path)
            diff = comparar_recursivo(content_dict1[pid], content_dict2[pid], path)
            if diff:
                diferencias.update(diff)

        # Identificar elementos que están solo en una de las listas
        for pid in content_dict1.keys() - content_dict2.keys():
            path = f"Content['{pid}']"
            diferencias[path] = {
                "catalog-api": content_dict1[pid],
                "contentapi": "No está presente"
            }

        for pid in content_dict2.keys() - content_dict1.keys():
            path = f"Content['{pid}']"
            diferencias[path] = {
                "catalog-api": "No está presente",
                "contentapi": content_dict2[pid]
            }

        return diferencias
    else:
        raise KeyError("La clave 'Content' no está presente en ambos JSON.")
        diferencias = comparar_recursivo(data1, data2)
        return diferencias

def llamadaApi(endpoint,instanceId,alias,idioma,contenido,entorno,contentType=None,cantidad=None,order=None):

    match contenido:

        case 'contentsAll':
            if (isinstance(contentType,list)):
                tipos = ",".join(contentType)
                resp = requests.get('https://'+str(endpoint)+'/'+instanceId+"/"+alias+"/"+idioma+'/contents/all'+'?limit=1000&contentTypes='+tipos)
                print ('https://'+str(endpoint)+'/'+instanceId+"/"+alias+"/"+idioma+'/contents/all'+'?contentTypes='+tipos)
            else:
                resp = requests.get('https://'+str(endpoint)+'/'+instanceId+"/"+alias+"/"+idioma+'/contents/all'+'?limit=1000&contentTypes='+contentType)
                #print ('https://'+str(endpoint)+'/'+instanceId+"/"+alias+"/"+idioma+'/contents/all'+'?limit=1000?contentTypes='+contentType)
            json = resp.json()
            dictPid = {}

            if (isinstance(contentType,list)):
                for i in contentType:
                    aux = []
                    if cantidad is None:
                        for x in json["Content"]["List"]:
                            if (x["Pid"][:3] == i):
                                aux.append(x["Pid"])
                                #comparaPids(x["Pid"],x["Pid"][:3],entorno,cantidad)
                        dictPid[i] = aux
                    else:
                        flag = False
                        contador = 0
                        aux = []

                        for index, item in enumerate(json["Content"]["List"]):
                            if flag or (item["Pid"][:3] == i):
                                flag = True 
                                if contador < cantidad:
                                    if item["Pid"][:3] == i:
                                        aux.append(item["Pid"])
                                        #comparaPids(item["Pid"], item["Pid"][:3],entorno,cantidad)
                                        contador = contador + 1
                                        
                        dictPid[i] = aux
            else:
                aux = []
                if cantidad is not None:
                    flag = False
                    contador = 0
                    aux = []

                    for index, item in enumerate(json["Content"]["List"]):
                            if contador < cantidad:
                                aux.append(item["Pid"])
                                #comparaPids(item["Pid"], item["Pid"][:3],entorno,cantidad)
                                #print(item["Pid"])
                                contador = contador + 1
                                        
                    dictPid[item["Pid"][:3]] = aux
                else:
                    for i in json["Content"]["List"]:

                        aux.append(i["Pid"])
                        #comparaPids(i["Pid"],i["Pid"][:3],entorno,cantidad)

            return dictPid
        
        case 'relatedcontents':
            print ("relatedcontents")
            #r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?sourcePids=CHA356&relationTypes=AnyLevelChildren&contentTypes=CHA&limit=700&offset=0&includeImages=Cover%2cVideoFrame%2cBanner%2cIcon%2cBackground%2cLogo%2cRegistrationBanner%2cPaymentMethodImages%2cLandscapeCover%2cLandscapeArt%2cPortraitArt&orderBy=PARENTCHANNELID%2cCONTENTORDER%2cPID&includeAttributes=CA_RequiresPin%2cCA_AnyLevelChildren%2cCA_DeviceTypes&CA_DeviceTypes=801')
            r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?sourcePids='+contentType+'&relationTypes=AnyLevelChildren&contentTypes=CHA&limit=100&offset=0&includeImages=Cover%2cVideoFrame%2cBanner%2cIcon%2cBackground%2cLogo%2cRegistrationBanner%2cPaymentMethodImages%2cLandscapeCover%2cLandscapeArt%2cPortraitArt&orderBy=Pid&includeAttributes=CA_RequiresPin%2cCA_AnyLevelChildren%2cCA_DeviceTypes&CA_DeviceTypes=801')
            print ('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?sourcePids='+contentType+'&relationTypes=AnyLevelChildren&contentTypes=CHA&limit=100&offset=0&includeImages=Cover%2cVideoFrame%2cBanner%2cIcon%2cBackground%2cLogo%2cRegistrationBanner%2cPaymentMethodImages%2cLandscapeCover%2cLandscapeArt%2cPortraitArt&orderBy=Pid&includeAttributes=CA_RequiresPin%2cCA_AnyLevelChildren%2cCA_DeviceTypes&CA_DeviceTypes=801')
            json = r.json()
            return json       

        case 'pagedschedules':
            print ("pagedschedules")
            now = datetime.now()
            startTime = datetime.now() -timedelta(hours=2)
            endTime = now + timedelta(hours=2)
            starttime_epoch = int(time.mktime(startTime.timetuple()))
            finishtime_epoch = int((time.mktime(endTime.timetuple())))
            #print (str(starttime_epoch) + " - "+str(finishtime_epoch))
            r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?ca_devicetypes=502|null&offset=0&fields=ageratingpid,channelname,description,end,epgnetworkdvr,epgserieid,images,livechannelpid,liveprogrampid,pid,shortdescription,start,title&includeAttributes=ca_programtypeid&includeImages=videoframe&includerelations=genre&limit=100&liveChannelPids='+contentType+'&orderBy=DateStart&NoCache=798293031&startTime='+str(starttime_epoch)+"&endTime="+str(finishtime_epoch))
            print ('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?ca_devicetypes=502|null&offset=0&fields=ageratingpid,channelname,description,end,epgnetworkdvr,epgserieid,images,livechannelpid,liveprogrampid,pid,shortdescription,start,title&includeAttributes=ca_programtypeid&includeImages=videoframe&includerelations=genre&limit=100&liveChannelPids='+contentType+'&orderBy=DateStart&NoCache=798293031&startTime='+str(starttime_epoch)+"&endTime="+str(finishtime_epoch))
            json = r.json()
            return json

        case 'schedules':
            now = datetime.now()
            startTime = datetime.now()-timedelta(hours=2)
            endTime = datetime.now()+timedelta(hours=2)
            starttime_epoch = int(time.mktime(startTime.timetuple()))
            finishtime_epoch = int((time.mktime(endTime.timetuple())))
            #print (str(startTime) +" - "+ str(endTime))
            #print (str(starttime_epoch))
            #print (str(finishtime_epoch))
            #r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?ca_devicetypes=502|null&offset=0&fields=ageratingpid,channelname,description,end,epgnetworkdvr,epgserieid,images,livechannelpid,liveprogrampid,pid,shortdescription,start,title&includeAttributes=ca_programtypeid,ca_cpvrDisable,ca_descriptors,ca_blackout_target,ca_blackout_areas&includeImages=videoframe&includerelations=genre&limit=100&orderBy=PID&liveChannelPids=LCH100,LCH101,LCH102,LCH103,LCH104,LCH105,LCH106,LCH107,LCH108,LCH109,LCH11,LCH110,LCH111,LCH112,LCH113,LCH114,LCH115,LCH116,LCH117,LCH118,LCH12,LCH120,LCH1211,LCH122,LCH123,LCH1237,LCH1242,LCH1252,LCH1253,LCH127,LCH128,LCH1287,LCH129,LCH130,LCH131,LCH1313,LCH1314,LCH132,LCH133,LCH135,LCH136,LCH138,LCH14,LCH142,LCH143,LCH144,LCH145,LCH146,LCH147,LCH148,LCH149,LCH15,LCH150,LCH15025,LCH151,LCH152,LCH15267,LCH15268,LCH154,LCH155,LCH156,LCH157,LCH158,LCH159,LCH160,LCH17,LCH18,LCH182,LCH183,LCH184,LCH185,LCH186,LCH187,LCH189,LCH191,LCH192,LCH193,LCH194,LCH195,LCH196,LCH198,LCH199,LCH1995,LCH1996,LCH20,LCH2000,LCH202,LCH2023,LCH2025,LCH2028,LCH203,LCH204,LCH2044,LCH2045,LCH2050,LCH2055,LCH2062,LCH2064,LCH2066,LCH2067,LCH2069,LCH207,LCH2070,LCH2071,LCH2073,LCH2077,LCH2078,LCH208,LCH2080,LCH2081,LCH209,LCH2090,LCH2091,LCH2093,LCH2100,LCH2102,LCH2104,LCH2105,LCH2106,LCH211,LCH213,LCH214,LCH215,LCH216,LCH217,LCH2172,LCH218,LCH219,LCH2199,LCH2200,LCH23,LCH2398,LCH24,LCH2439,LCH2440,LCH2441,LCH2442,LCH25,LCH27,LCH28,LCH29,LCH30,LCH31,LCH32,LCH33,LCH34,LCH35,LCH35988,LCH35989,LCH36,LCH36001,LCH3765,LCH3766,LCH3767,LCH3768,LCH38,LCH39,LCH40,LCH4057,LCH41,LCH42,LCH43,LCH44,LCH442,LCH443,LCH444,LCH459,LCH46,LCH460,LCH461,LCH462,LCH463,LCH464,LCH465,LCH466,LCH467,LCH4677,LCH4678,LCH4685,LCH4686,LCH469,LCH47,LCH471,LCH49,LCH50,LCH51,LCH52,LCH53,LCH54,LCH55,LCH56,LCH58,LCH5838,LCH5855,LCH5858,LCH5860,LCH5861,LCH5864,LCH5867,LCH59,LCH60,LCH61,LCH62,LCH64,LCH65,LCH653,LCH654,LCH66,LCH67,LCH677,LCH6873,LCH6874,LCH69,LCH70,LCH71,LCH72,LCH7237,LCH7250,LCH7350,LCH74,LCH7446,LCH75,LCH76,LCH7658,LCH7659,LCH7660,LCH7661,LCH7662,LCH7663,LCH7664,LCH7665,LCH7666,LCH7667,LCH7668,LCH7669,LCH7670,LCH7671,LCH7672,LCH7673,LCH7674,LCH77,LCH770,LCH771,LCH772,LCH773,LCH7796,LCH7797,LCH7841,LCH7842,LCH7843,LCH79,LCH7906,LCH7981,LCH7982,LCH7983,LCH7984,LCH8,LCH80,LCH81,LCH82,LCH8330,LCH8331,LCH8409,LCH85,LCH854,LCH855,LCH856,LCH8561,LCH8562,LCH8563,LCH8564,LCH8565,LCH8566,LCH8567,LCH857,LCH86,LCH8628,LCH8642,LCH8645,LCH87,LCH883,LCH887,LCH888,LCH89,LCH891,LCH9,LCH90,LCH9032,LCH913,LCH92,LCH928,LCH929,LCH93,LCH930,LCH931,LCH932,LCH933,LCH934,LCH935,LCH936,LCH937,LCH94,LCH946,LCH948,LCH949,LCH95,LCH97,LCH98&startTime='+str(starttime_epoch)+"&endTime="+str(finishtime_epoch))
            r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?ca_devicetypes=502|null&offset=0&fields=start,end,AvailableFrom,AvailableUntil,ageratingpid,channelname&includeAttributes=ca_programtypeid,ca_cpvrDisable,ca_descriptors,ca_blackout_target,ca_blackout_areas&includeImages=videoframe&orderBy=PID&includerelations=genre&limit=100&liveChannelPids='+contentType+'&startTime='+str(starttime_epoch)+"&endTime="+str(finishtime_epoch),headers={'Cache-Control': 'no-cache'})
            print ('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?ca_devicetypes=502|null&offset=0&fields=ageratingpid,channelname&includeAttributes=ca_programtypeid,ca_cpvrDisable,ca_descriptors,ca_blackout_target,ca_blackout_areas&includeImages=videoframe&includerelations=genre&limit=100&orderBy=PID&liveChannelPids='+contentType+'&startTime='+str(starttime_epoch)+"&endTime="+str(finishtime_epoch))
            json = r.json()
            return json

        case 'content':
            r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'/'+contentType)
            print ('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'/'+contentType)
            json = r.json()
            return json
    
        case 'children':
            now = datetime.now()
            startTime = datetime.now()-timedelta(hours=4)
            endTime = datetime.now()+timedelta(hours=4)
            starttime_epoch = int(time.mktime(startTime.timetuple()))
            finishtime_epoch = int((time.mktime(endTime.timetuple())))
            #r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/content/'+contentType+'/children?ca_deviceTypes=401&contenttypes=MOV,SER,SEA,EPI,CHA,LPR,LSE,LSR&ca_RequiresPin=false&includeattributes=ca_commercialoffer,ca_channelmaps,Ca_subscriptions,ca_commercializationtype,ca_devicetypes,ca_devicetypes_qualities,ca_distributor,ca_gvpid,ca_products,ca_requirespin,ca_type,ca_descriptors,ca_nextepisode&includerelations=genre,actor,director,writer,producer,productdependencies,media,subtitle,provider,pricingmodel,season,thumbnail&fields=availableuntilutc,title,shortdescription,averagerating,totalvotes,ageratingpid,description,duration,year,views,recentviews,releasedate,Ccountry,originaltitle,awards,assettype,commercializationtype,contentorder,distributor,distributorid,distributorproductid,extcatchupurn,finalyear,imdbrating,iscomingsoon,isdtp,personal,producer,promotionalrating,requirespin,seasonid,seriesid,seriesname,statusdate,statusdateorder,totalchildren,transparentprovider,twitterhashtag,externalcatchupurl,availablefrom,availableuntil,starttime,endtime,defaultlanguageorders,closingcreditsstarttime,seriespid,seasonpid,seasonnumber,episodenumber&startTime='+str(starttime_epoch)+'&endTime='+str(finishtime_epoch)+'&NoCache=402787560')
            r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/content/'+contentType+'/children?ca_deviceTypes=401&contenttypes=MOV,SER,SEA,EPI,CHA,LPR,LSE,LSR&ca_RequiresPin=false&includeattributes=ca_commercialoffer,ca_channelmaps,Ca_subscriptions,ca_commercializationtype,ca_devicetypes,ca_devicetypes_qualities,ca_distributor,ca_gvpid,ca_products,ca_requirespin,ca_type,ca_descriptors,ca_nextepisode&includerelations=genre,actor,director,writer,producer,productdependencies,media,subtitle,provider,pricingmodel,season,thumbnail&fields=availableuntilutc,title,shortdescription,averagerating,totalvotes,ageratingpid,description,duration,year,views,recentviews,releasedate,Ccountry,originaltitle,awards,assettype,commercializationtype,contentorder,distributor,distributorid,distributorproductid,extcatchupurn,finalyear,imdbrating,iscomingsoon,isdtp,personal,producer,promotionalrating,requirespin,seasonid,seriesid,seriesname,statusdate,statusdateorder,totalchildren,transparentprovider,twitterhashtag,externalcatchupurl,availablefrom,availableuntil,starttime,endtime,defaultlanguageorders,closingcreditsstarttime,seriespid,seasonpid,seasonnumber,episodenumber&startTime='+str(starttime_epoch)+'&endTime='+str(finishtime_epoch)+'&NoCache=402787560')
            print ('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/content/'+contentType+'/children?ca_deviceTypes=401&contenttypes=MOV,SER,SEA,EPI,CHA,LPR,LSE,LSR&ca_RequiresPin=false&includeattributes=ca_commercialoffer,ca_channelmaps,Ca_subscriptions,ca_commercializationtype,ca_devicetypes,ca_devicetypes_qualities,ca_distributor,ca_gvpid,ca_products,ca_requirespin,ca_type,ca_descriptors,ca_nextepisode&includerelations=genre,actor,director,writer,producer,productdependencies,media,subtitle,provider,pricingmodel,season,thumbnail&fields=availableuntilutc,title,shortdescription,averagerating,totalvotes,ageratingpid,description,duration,year,views,recentviews,releasedate,Ccountry,originaltitle,awards,assettype,commercializationtype,contentorder,distributor,distributorid,distributorproductid,extcatchupurn,finalyear,imdbrating,iscomingsoon,isdtp,personal,producer,promotionalrating,requirespin,seasonid,seriesid,seriesname,statusdate,statusdateorder,totalchildren,transparentprovider,twitterhashtag,externalcatchupurl,availablefrom,availableuntil,starttime,endtime,defaultlanguageorders,closingcreditsstarttime,seriespid,seasonpid,seasonnumber,episodenumber&startTime='+str(starttime_epoch)+'&endTime='+str(finishtime_epoch)+'&NoCache=402787560')
            json = r.json()
            return json
        
        case 'search':
            if (isinstance(contentType,list)):
                tipos = ",".join(contentType)
                r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?ca_deviceTypes=401&includeAttributes=ca_requiresPin,ca_blackout_target,ca_blackout_areas&includeRelations=ProductDependencies,Genre,Provider&fields=pid,title,callLetter,channelName,duration,start,end,epgChannelId,channelId,programId,commercializationType,distributor,releaseDate,forbiddenTechnology,images.videoFrame,images.cover,images.landscapeCover,images.banner,images.portraitArt&term=the&searchFields=keywords&ca_requiresPin=false&ca_neoCatalog=true&orderBy=PID:D&offset=0&limit=100&contentTypes='+tipos)
            else:
                r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/'+contenido+'?ca_deviceTypes=401&includeAttributes=ca_requiresPin,ca_blackout_target,ca_blackout_areas&includeRelations=ProductDependencies,Genre,Provider&fields=pid,title,callLetter,channelName,duration,start,end,epgChannelId,channelId,programId,commercializationType,distributor,releaseDate,forbiddenTechnology,images.videoFrame,images.cover,images.landscapeCover,images.banner,images.portraitArt&term=the&searchFields=keywords&ca_requiresPin=false&ca_neoCatalog=true&orderBy=PID:D&offset=0&limit=100&contentTypes='+contentType)
            json = r.json()

        case 'orderBy':
            r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/contents/all?ca_deviceTypes=401&contentTypes=CHA&fields=pid,title,titleInMenu,description,anchorExtensionPosition,IsSpecialChannel,parentChannelId,channelType,vodDefaultOrder,liveDefaultOrder,uxReferenceLayout,uxReferenceSearch,nextLevel,showSubscribedFilter,images.logo,images.banner,images.icon&includeAttributes=ca_requiresPin,ca_externalUrls&orderBy='+order)
            print ('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+'/contents/all?ca_deviceTypes=401&contentTypes=CHA&fields=pid,title,titleInMenu,description,anchorExtensionPosition,IsSpecialChannel,parentChannelId,channelType,vodDefaultOrder,liveDefaultOrder,uxReferenceLayout,uxReferenceSearch,nextLevel,showSubscribedFilter,images.logo,images.banner,images.icon&includeAttributes=ca_requiresPin,ca_externalUrls&orderBy='+order)
            json = r.json()
            return json
        
        case 'enlace':
            r = requests.get('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+contentType)
            print ('https://'+endpoint+'/'+instanceId+"/"+alias+"/"+idioma+contentType)
            json = r.json()
            return json
        
def prueba(entorno,instanceId,alias,idioma,contenido,contentType=None,cantidad=None):
    diccionarioContent = {}
    diccionarioCatalog = {}
    if entorno == 'performance':
        if contenido == 'orderBy':
            for i in arrOrderBy:
                diccionarioContent[i] = contenido,llamadaApi(str(CONTENTAPIPERF),instanceId,alias,idioma,contenido,entorno,contentType,cantidad,i)
                diccionarioCatalog[i] = llamadaApi(str(CATALOGAPIPERF),instanceId,alias,idioma,contenido,entorno,contentType,cantidad,i)
        else:
            diccionarioContent = llamadaApi(str(CONTENTAPIPERF),instanceId,alias,idioma,contenido,entorno,contentType,cantidad)
            diccionarioCatalog = llamadaApi(str(CATALOGAPIPERF),instanceId,alias,idioma,contenido,entorno,contentType,cantidad)
    elif entorno == 'functional':
        if contenido == 'orderBy':
            for i in arrOrderBy:
                diccionarioContent[i] = llamadaApi(str(CONTENTAPIFUNC),instanceId,alias,idioma,contenido,entorno,contentType,cantidad,i)
                diccionarioCatalog[i] = llamadaApi(str(CATALOGAPIFUNC),instanceId,alias,idioma,contenido,entorno,contentType,cantidad,i)
        else:
            diccionarioContent = llamadaApi(str(CONTENTAPIFUNC),instanceId,alias,idioma,contenido,entorno,contentType,cantidad)
            diccionarioCatalog = llamadaApi(str(CATALOGAPIFUNC),instanceId,alias,idioma,contenido,entorno,contentType,cantidad)

        diccionarioContent["liveChannel"] = contentType
        diccionarioCatalog["liveChannel"] = contentType
        return diccionarioContent,diccionarioCatalog
    
def diferenciasPids(dict1, dict2):
    def extract_pids(data):
        return {item["Pid"] for item in data.get("Content", [])}
    
    pids1 = extract_pids(dict1)
    pids2 = extract_pids(dict2)
    
    unique_pids_dict1 = pids1 - pids2
    unique_pids_dict2 = pids2 - pids1

    result = {
        "dict1": list(unique_pids_dict1),
        "dict2": list(unique_pids_dict2)
    }
    
    result_json = json.dumps(result)
    
    return result_json

### PRUEBAS ###

# Todas las pruebas juntas con multihilos

def obtener_elementos_aleatorios(lista, cantidad):
    return random.sample(lista, cantidad)

def prueba_schedules():
    for valor in valores:
        intentos = 0 

        while intentos < 40:  
            diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "schedules", valor)
            #diferencias = comparaJson(diccionario1, diccionario2, ["GenrePids", "ImageOrder", "['Images']['VideoFrame'][0]['Pid']"])
            diferencias = comparaJson(diccionario1, diccionario2)

            if len(diferencias) == 0:
                break  
            
            for i,y in zip(diccionario1["Content"],diccionario2["Content"]):
                print ("Pid: "+str(i["Pid"])+" AvailableFrom: "+str(i["AvailableFrom"]) + " - AvailableUntil "+ str(i["AvailableUntil"]))
                print ("Pid: "+str(y["Pid"])+" AvailableFrom: "+str(y["AvailableFrom"]) + " - AvailableUntil "+ str(y["AvailableUntil"]))

            now = datetime.now()
            startTime = datetime.now()-timedelta(hours=2)
            endTime = datetime.now()+timedelta(hours=2)
            starttime_epoch = int(time.mktime(startTime.timetuple()))
            finishtime_epoch = int((time.mktime(endTime.timetuple())))
            print ("NUEVO INTENTO")
            print(f"Intento {intentos + 1}: {len(diferencias)} diferencias encontradas.")
            print(diccionario1["liveChannel"] + " - " + diccionario2["liveChannel"] + " - " + valor)
            print (str(starttime_epoch) +" - "+str(finishtime_epoch))

            intentos += 1 
            time.sleep(1)
        crearJsonContent(diferencias, "schedules", valor, 
                         "https://catalog-api.labs.gvp.telefonica.com/25/default/pt-br/schedules?ca_devicetypes=502%7Cnull&offset=0&fields=ageratingpid,channelname&includeAttributes=ca_programtypeid,ca_cpvrDisable,ca_descriptors,ca_blackout_target,ca_blackout_areas&includeImages=videoframe&includerelations=genre&limit=100&orderBy=PID&liveChannelPids=LIVECHANNEL&startTime=STARTTIME&endTime=ENDTIME", 
                         "https://contentapi-preprod.labs.gvp.telefonica.com/25/default/pt-br/schedules?ca_devicetypes=502%7Cnull&offset=0&fields=ageratingpid,channelname&includeAttributes=ca_programtypeid,ca_cpvrDisable,ca_descriptors,ca_blackout_target,ca_blackout_areas&includeImages=videoframe&includerelations=genre&limit=100&orderBy=PID&liveChannelPids=LIVECHANNEL&startTime=STARTTIME&endTime=ENDTIME")


def prueba_pageSchedules():
    for valor in valores:
        intentos = 0 

        while intentos < 40:  
            diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "pagedschedules", valor)
            #diferencias = comparaJson(diccionario1, diccionario2,["Images","GenrePids"])
            diferencias = comparaJson(diccionario1, diccionario2)
            
            if len(diferencias) == 0:
                break  

            now = datetime.now()
            startTime = datetime.now()-timedelta(hours=2)
            endTime = datetime.now()+timedelta(hours=2)
            starttime_epoch = int(time.mktime(startTime.timetuple()))
            finishtime_epoch = int((time.mktime(endTime.timetuple())))
            print ("NUEVO INTENTO")
            print(f"Intento {intentos + 1}: {len(diferencias)} diferencias encontradas.")
            print(diccionario1["liveChannel"] + " - " + diccionario2["liveChannel"] + " - " + valor)
            print (str(starttime_epoch) +" - "+str(finishtime_epoch))

            intentos += 1 

            if intentos < 3:
                print ("falla")
            time.sleep(2)
        crearJsonContent(diferencias, "pageSchedules", valor, 
                         "", 
                         "")

def prueba_orderBy():
    diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "orderBy", "LCH", 3)
    #diferencias = comparaJson(diccionario1, diccionario2,["Images"])
    diferencias = comparaJson(diccionario1, diccionario2)
    guardarDiferencias(diferencias,"https://catalog-api.labs.gvp.telefonica.com/25/default/pt-br/content/","https://contentapi-preprod.labs.gvp.telefonica.com/25/default/pt-br/content/")

def prueba_content(pid):
    diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "content", pid)
    diferencias = comparaJson(diccionario1, diccionario2)
    print (diferencias)
    crearJsonContent(diferencias, "content", pid,"https://catalog-api.labs.gvp.telefonica.com/25/default/pt-br/content/","https://contentapi-preprod.labs.gvp.telefonica.com/25/default/pt-br/content/")

def process_content(y):
    try:
        diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "content", y)
        diferencias = comparaJson(diccionario1, diccionario2)
        crearJsonContent(diferencias, "contentAll", y,
                         "https://catalog-api.labs.gvp.telefonica.com/25/default/pt-br/content/",
                         "https://contentapi-preprod.labs.gvp.telefonica.com/25/default/pt-br/content/")
    except Exception as e:
        print(f"Error procesando el contenido {y}: {e}")

def prueba_contentAll():
    try:
        for arr in arrContentType:
            diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "contentsAll", arr)
            diferencias = comparaJson(diccionario1,diccionario2)
            print ("PRUEBA")
            print (diferencias)
            crearJson(diferencias,"prueba.json")
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_content, y) for i1, i2 in diccionario1.items() for y in i2]

                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()  
                    except Exception as e:
                        print(f"Error en una de las tareas: {e}")
            
        print("Procesamiento completado.")
    except Exception as e:
        print(f"Error en el procesamiento principal: {e}")

# Función que procesa el contenido de manera segura
def process_content_schedule(y):
    try:
        diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "schedules", y)
        #diferencias = comparaJson(diccionario1, diccionario2, ["GenrePids", "ImageOrder","['Images']['VideoFrame'][0]['Pid']"])
        diferencias = comparaJson(diccionario1, diccionario2)
        crearJsonContent(diferencias, "schedules", y, "", "")
    except Exception as e:
        print(f"Error procesando el contenido {y}: {e}")

# Función principal que asegura que las tareas se ejecutan secuencialmente en orden
def prueba_schedules_hilos():
    try:
        for y in valores:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(process_content_schedule, y)
                try:
                    # Esperamos a que el resultado del hilo se complete antes de continuar con el siguiente
                    future.result()  
                except Exception as e:
                    print(f"Error en una de las tareas: {e}")
            
        
        print("Procesamiento completado.")
    except Exception as e:
        print(f"Error en el procesamiento principal: {e}")

def prueba_relatedContents(releatedPID):
    diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "relatedcontents", releatedPID)
    #diferencias = comparaJson(diccionario1, diccionario2,["CatalogDate","'Images']['Background'][0]['Pid']"])
    diferencias = comparaJson(diccionario1, diccionario2)
    crearJson(diferencias, "relatedContents.json")

def process_content_related(y):
    try:
        diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "relatedcontents", y)
        #diferencias = comparaJson(diccionario1, diccionario2,["CatalogDate","'Images']['Background'][0]['Pid']"])
        diferencias = comparaJson(diccionario1, diccionario2)
        crearJsonContent(diferencias, "ReleatedContent", y,
                         "https://catalog-api.labs.gvp.telefonica.com/25/default/pt-br/relatedcontents?sourcePids=RELEATEDCONTENT&relationTypes=AnyLevelChildren&contentTypes=CHA&limit=100&offset=0&includeImages=Cover%2cVideoFrame%2cBanner%2cIcon%2cBackground%2cLogo%2cRegistrationBanner%2cPaymentMethodImages%2cLandscapeCover%2cLandscapeArt%2cPortraitArt&orderBy=Pid&includeAttributes=CA_RequiresPin%2cCA_AnyLevelChildren%2cCA_DeviceTypes&CA_DeviceTypes=801",
                         "https://contentapi-preprod.labs.gvp.telefonica.com/25/default/pt-br/relatedcontents?sourcePids=RELEATEDCONTENT&relationTypes=AnyLevelChildren&contentTypes=CHA&limit=100&offset=0&includeImages=Cover%2cVideoFrame%2cBanner%2cIcon%2cBackground%2cLogo%2cRegistrationBanner%2cPaymentMethodImages%2cLandscapeCover%2cLandscapeArt%2cPortraitArt&orderBy=Pid&includeAttributes=CA_RequiresPin%2cCA_AnyLevelChildren%2cCA_DeviceTypes&CA_DeviceTypes=801")
    except Exception as e:
        print(f"Error procesando el contenido {y}: {e}")

def prueba_relatedContents_hilos():
    try:
        for arr in arrContentType:
            diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "contentsAll", arr)
            diferencias = comparaJson(diccionario1,diccionario2)
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_content_related, y) for i1, i2 in diccionario1.items() for y in i2]

                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()  
                    except Exception as e:
                        print(f"Error en una de las tareas: {e}")
            
        print("Procesamiento completado.")
    except Exception as e:
        print(f"Error en el procesamiento principal: {e}")

def prueba_enlace(enlace):
    diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "enlace", enlace)
    diferencias = comparaJson(diccionario1, diccionario2)
    crearJson(diferencias, "enlace.json")

def prueba_children(childrenPID):
    diccionario1, diccionario2 = prueba("functional", "25", "default", "pt-br", "children", childrenPID)
    diferencias = comparaJson(diccionario1, diccionario2)
    crearJson(diferencias, "children.json")

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [
        #executor.submit(prueba_schedules_hilos),
        #executor.submit(prueba_schedules),
        #executor.submit(prueba_pageSchedules),
        executor.submit(prueba_orderBy),
        #executor.submit(prueba_content,"AVA1"),
        #executor.submit(prueba_contentAll),
        #executor.submit(prueba_relatedContents,"CHA356"),
        #executor.submit(prueba_relatedContents_hilos),
        #executor.submit(prueba_enlace,"/content/AVA1"),
        #executor.submit(prueba_children,"CHA1040")
    ]
    for future in concurrent.futures.as_completed(futures):
        future.result()

now = datetime.now()
print ("Fecha actual" +str(now))
prueba_schedules()
now = datetime.now()
print ("Fecha Final" +str(now))

prueba_pageSchedules()