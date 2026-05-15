import pandas as pd
import requests
import asyncio
import aiohttp
import os
import sys
import re
import time
import zipfile
import json
import shutil
import pdfplumber
from bs4 import BeautifulSoup
from ddgs import DDGS
from google import genai
from rapidfuzz import fuzz  # <--- NOWA BIBLIOTEKA DO DOPASOWANIA NAZW

is_ci = os.getenv('CI') == 'true'

# Inicjalizacja klienta Gemini
GEMINI_API_KEY = os.getenv(key="GEMINI_API_KEY")
gemini_client = None

if GEMINI_API_KEY:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    print("Brak klucza GEMINI_API_KEY. LLM nie zadziała.")

class SsmPdfVerifier:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.lei_set = set()

    def load_leis(self) -> None:
        if not os.path.exists(path=self.pdf_path):
            print(f"⚠️ Plik PDF nie istnieje: {self.pdf_path}. Pomijam weryfikację PDF.")
            return
        print(f"Analiza pliku PDF: {self.pdf_path}...")
        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        found_leis = re.findall(pattern=r'\b[A-Z0-9]{20}\b', string=text)
                        self.lei_set.update(found_leis)
            print(f"Wczytano {len(self.lei_set)} unikalnych kodów LEI z pliku PDF.")
        except Exception as e:
            print(f"Błąd podczas odczytu PDF: {e}")

    def is_lei_in_pdf(self, lei: str) -> bool:
        if not lei: return False
        return lei.strip().upper() in self.lei_set

class EbaPsdVerifier:
    """Weryfikuje licencje PSD/EMD z pliku JSON w archiwum ZIP."""
    def __init__(self, zip_path: str):
        self.zip_path = zip_path
        self.psd_data = []

    def load_data(self) -> None:
        if not os.path.exists(self.zip_path):
            print(f"⚠️ Plik ZIP nie istnieje: {self.zip_path}. Pomijam weryfikację PSD.")
            return

        print(f"Wypakowywanie i analiza danych PSD z: {self.zip_path}...")
        tmp_dir = "tmp_eba"
        try:
            if os.path.exists(tmp_dir): shutil.rmtree(tmp_dir)
            os.makedirs(tmp_dir)

            with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmp_dir)

            json_files = [f for f in os.listdir(tmp_dir) if f.endswith('.json')]
            if not json_files:
                print("Nie znaleziono pliku JSON w archiwum.")
                return

            with open(os.path.join(tmp_dir, json_files[0]), 'r', encoding='utf-8') as f:
                raw_content = json.load(f)
                
                # --- POPRAWKA DLA STRUKTURY LISTY LIST [[...], [...]] ---
                entities_list = []
                if isinstance(raw_content, list) and len(raw_content) > 1:
                    # Z Twojego przykładu wynika, że indeks [0] to disclaimer, a [1] to dane
                    if isinstance(raw_content[1], list):
                        entities_list = raw_content[1]
                elif isinstance(raw_content, list):
                    entities_list = raw_content
                elif isinstance(raw_content, dict):
                    # Fallback dla innych wersji pliku
                    for val in raw_content.values():
                        if isinstance(val, list):
                            entities_list = val
                            break
                
                if not entities_list:
                    print("Nie udało się zlokalizować listy rekordów w pliku JSON.")
                    return

                # Przetwarzanie rekordów
                for entry in entities_list:
                    if not isinstance(entry, dict):
                        continue
                        
                    # Spłaszczanie Properties
                    props_list = entry.get('Properties', [])
                    if not isinstance(props_list, list):
                        continue
                        
                    props = {}
                    for p in props_list:
                        if isinstance(p, dict) and p:
                            # Wyciągamy klucz i wartość z pierwszego elementu słownika
                            k = list(p.keys())[0]
                            v = list(p.values())[0]
                            props[k] = v
                    
                    # Dodajemy do bazy tylko jeśli mamy nazwę
                    name = props.get('ENT_NAM')
                    if name:
                        self.psd_data.append({
                            'name': str(name).upper(),
                            'address': str(props.get('ENT_ADD', '')).upper(),
                            'type': entry.get('EntityType', ''),
                            'auth': props.get('ENT_AUT', [])
                        })
            print(f"Wczytano {len(self.psd_data)} rekordów PSD/EMD.")
        except Exception as e:
            print(f"Błąd podczas przetwarzania danych PSD: {e}")
            print(f"Typ błędu: {type(e).__name__}")
        finally:
            if os.path.exists(tmp_dir): shutil.rmtree(tmp_dir)

    def find_match(self, company_name: str, address: str) -> str:
        """Próbuje dopasować nazwę firmy do bazy PSD za pomocą Fuzzy Matching."""
        name_to_search = str(company_name).upper()
        addr_to_search = str(address).upper()
        
        best_score = 0
        best_match = None

        for record in self.psd_data:
            # Token Set Ratio ignoruje kolejność słów i nadmiarowe człony
            score = fuzz.token_set_ratio(name_to_search, record['name'])
            
            if score > 85:
                # Jeśli nazwa pasuje, sprawdzamy adres lub ekstremalnie wysoki score nazwy
                if not addr_to_search or fuzz.partial_ratio(addr_to_search, record['address']) > 70 or score > 95:
                    best_score = score
                    best_match = record
                    break 

        if best_match:
            entity_type = best_match['type']
            auth_dates = best_match['auth']
            # Jeśli w ENT_AUT są dwie daty -> licencja wycofana
            if isinstance(auth_dates, list) and len(auth_dates) >= 2:
                return f"{entity_type} (Wycofana {auth_dates[1]})"
            return entity_type

        return ""

class BankingLicenseVerifier:
    def __init__(self, pdf_verifier: SsmPdfVerifier = None):
        self.ddgs = DDGS()
        self.pdf_verifier = pdf_verifier

    def check_banking_license(self, company_name: str, lei: str, website: str, address: str) -> str:
        if self.pdf_verifier and self.pdf_verifier.is_lei_in_pdf(lei):
            return "TAK [SSM/ECB]"

        query = f'"{company_name}" banking license EU authorisation'
        try:
            results = list(self.ddgs.text(query=query, region='pl-pl', max_results=3))
            search_context = "\n".join([f"Source: {r.get('href')}\nText: {r.get('body')}" for r in results])
        except Exception as e:
            return f"Search Error: {e}"

        prompt = f"""
        Jesteś ekspertem ds. regulacji bankowych w UE i specjalistą Compliance.
        Twoim zadaniem jest weryfikacja statusu licencji bankowej podmiotu.
        DANE PODMIOTU: Nazwa: {company_name}, LEI: {lei}, Website: {website}, Siedziba: {address}
        KONTEKST: {search_context}
        FORMAT: "TAK [Organ]", "NIE" lub "BRAK DANYCH". Odpowiedz wyłącznie w tym formacie.
        """ 
        
        models_to_try = ['gemma-4-26b-a4b-it', 'gemma-4-31b-it', 'gemini-3.1-flash-lite']
        for model_name in models_to_try:
            try:
                response = gemini_client.models.generate_content(model=model_name, contents=prompt, config={"temperature": 0.1})   
                return " ".join(response.text.strip().replace('\n', ' ').split())
            except: continue
        return "BRAK DANYCH"

class EsmaCsvExtractor:
    def __init__(self, url: str):
        self.url = url
    def fetch_and_clean_csv(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.url, encoding='utf-8').fillna('')
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: " ".join(str(x).split()).strip())
            return df
        except Exception as e:
            raise ValueError(f"Błąd CSV ESMA: {e}")

class EsmaApiEnricher:
    async def fetch_entity_classification(self, session: aiohttp.ClientSession, lei: str) -> str:
        if not lei: return ""
        url = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select"
        params = {'q': f'ae_lei:"{lei}"', 'fq': 'type_s:parent', 'rows': '10', 'wt': 'json'}
        try:
            async with session.get(url=url, params=params, headers={'User-Agent': 'Mozilla/5.0'}) as response:
                if response.status == 200:
                    data = await response.json()
                    docs = data.get('response', {}).get('docs',[])
                    labels = set()
                    for doc in docs:
                        val = doc.get('ae_entityTypeLabel')
                        if val: labels.update(val if isinstance(val, list) else [val])
                    return " | ".join(sorted(labels))
        except: pass
        return ""
    async def fetch_all_classifications(self, leis: list[str]) -> dict:
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_entity_classification(session, lei) for lei in leis]
            print(f"Wysyłanie żądań do API ESMA dla {len(leis)} podmiotów...")
            final_responses = await asyncio.gather(*tasks)
            return dict(zip(leis, final_responses))

def process_esma_data(df: pd.DataFrame, entity_types: dict) -> pd.DataFrame:
    df['Działalność w Polsce?'] = df['ac_serviceCode_cou'].apply(lambda x: 'PRAWDA' if 'PL' in str(x).split('|') else 'FAŁSZ')
    uslugi = {"Usługi - a. custody": "providing custody", "Usługi - b. trading platform": "operating a trading platform", "Usługi - c. exchange CA for funds": "exchange of crypto-assets for funds", "Usługi - d. exchange CA for CA": "exchange of crypto-assets for other crypto-assets", "Usługi - e. order execution for clients": "execution of orders", "Usługi - f. placing CA": "placing of crypto-assets", "Usługi - g. reception and transmission orders for clients": "reception and transmission", "Usługi - h. advice on CA": "providing advice", "Usługi - i. portfolio mgmt": "providing portfolio management", "Usługi - j. transfer of CA for clients": "providing transfer services"}
    for col_name, search_phrase in uslugi.items():
        df[col_name] = df['ac_serviceCode'].str.lower().fillna('').apply(lambda x: 1 if search_phrase.lower() in x else 0)
    df['ESMA - Typ Podmiotu'] = df['ae_lei'].map(entity_types).fillna('')
    klasyfikacja = {"FI": ["investment firm"], "Fiinfra": ["organised trading facility", "multilateral trading facility", "regulated market"], "AssetMgmt": ["aifm"]}
    df['Bank'] = df['Banking License Status'].str.contains('TAK', case=False, na=False).astype(int)
    for col_name, search_phrases in klasyfikacja.items():
        df[col_name] = df['ESMA - Typ Podmiotu'].str.lower().apply(lambda x: 1 if any(phrase in str(x) for phrase in search_phrases) else 0)
    return df

async def run_esma_pipeline() -> None:
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pdf_path = os.path.join(base_dir, "data", "raw", "ssm.pdf")
    zip_path = os.path.join(base_dir, "data", "raw", "eba_psd.zip")
    output_path = os.path.join(base_dir, "data", "processed", "esma_casps_enriched.csv")

    csv_url = "https://www.esma.europa.eu/sites/default/files/2024-12/CASPS.csv"
    df_esma = EsmaCsvExtractor(url=csv_url).fetch_and_clean_csv()
    
    pdf_verifier = SsmPdfVerifier(pdf_path=pdf_path)
    pdf_verifier.load_leis()
    
    psd_verifier = EbaPsdVerifier(zip_path=zip_path)
    psd_verifier.load_data()

    enricher = EsmaApiEnricher()
    classifications = await enricher.fetch_all_classifications(df_esma['ae_lei'].unique().tolist())

    df_final = df_esma.copy() 
    df_final['Banking License Status'] = ""
    df_final['PSD status'] = ""
    
    verifier = BankingLicenseVerifier(pdf_verifier=pdf_verifier)
    NON_EURO_ZONE = {'CZ', 'PL', 'HU', 'SE', 'DK'}
    
    total = len(df_final)
    for index, row in df_final.iterrows():
        company_name = str(row['ae_lei_name'])
        lei = str(row['ae_lei'])
        country_code = str(row.get('ae_lei_cou_code', '')).strip().upper()
        
        print(f"[{index+1}/{total}] Przetwarzanie: {company_name[:50]}...", end=" ", flush=True)
        
        # 1. Weryfikacja Bankowa
        should_use_llm = (country_code in NON_EURO_ZONE) or (not country_code)
        if not should_use_llm:
            status = "TAK [SSM/ECB]" if pdf_verifier.is_lei_in_pdf(lei) else "NIE"
        else:
            status = verifier.check_banking_license(company_name, lei, row['ae_website'], row['ae_address'])
        
        df_final.at[index, 'Banking License Status'] = status
        
        # 2. Weryfikacja PSD/EMD (Tylko jeśli nie jest bankiem)
        if "TAK" not in status.upper():
            psd_status = psd_verifier.find_match(company_name, row['ae_address'])
            df_final.at[index, 'PSD status'] = psd_status
        
        print(f"Bank: {status} | PSD: {df_final.at[index, 'PSD status']}")
        if should_use_llm and "TAK" not in status.upper(): time.sleep(4)

    df_final = process_esma_data(df=df_final, entity_types=classifications)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\nSukces! Plik zapisano w: {output_path}")

if __name__ == "__main__":
    asyncio.run(run_esma_pipeline())
