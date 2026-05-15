import pandas as pd
import requests
import asyncio
import aiohttp
from tqdm.asyncio import tqdm as async_tqdm
import os
import re
import time
from bs4 import BeautifulSoup
from ddgs import DDGS
from tqdm import tqdm
from google import genai  # Nowe, wspierane SDK Google


# Inicjalizacja klienta Gemini
GEMINI_API_KEY = os.getenv(key="GEMINI_API_KEY")
gemini_client = None

if GEMINI_API_KEY:
    gemini_client = genai.Client(
        api_key=GEMINI_API_KEY
    )
    for m in gemini_client.models.list():
        print(f"Model: {m.name}")
else:
    print("Brak klucza GEMINI_API_KEY. LLM nie zadziała.")
    
class BankingLicenseVerifier:
    def __init__(self):
        self.ddgs = DDGS()

    def check_banking_license(self, company_name: str, lei: str, website: str, address: str) -> str:
        query = f'Does company "{company_name}" (LEI: {lei}) have an EU banking license?'
    
        # 1. Pobranie wyników wyszukiwania (to robimy zawsze)
        try:
            results = list(self.ddgs.text(query=query, region='pl-pl', max_results=3))
            search_context = "\n".join([f"Source: {r.get('href')}\nText: {r.get('body')}" for r in results])
        except Exception as e:
            return f"Search Error: {e}"
            
        prompt = f"""
        ROLE: Financial OSINT Analyst.
        TASK: Determine if {company_name} (LEI: {lei}) has an EU banking license based on the context.
        CONTEXT: {search_context}
        OUTPUT: Provide "TAK [Authority Name]", "NIE", or "BRAK DANYCH". No other text.
        """
    
        # 2. Próba z użyciem "Retry" i Fallbacku
        models_to_try = ['gemma-4-26b-a4b-it', 'gemma-4-31b-it', 'gemini-3.1-flash-lite']
        last_error_msg = "Unknown error"
        for model_name in models_to_try:
            try:
                response = gemini_client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config={"temperature": 0.1} # Niska temperatura = mniej halucynacji
                )
                return response.text.strip()
            except Exception as e:
                # Zapisujemy błąd w zmiennej zewnętrznej względem bloku try
                err_str = str(e)
                last_error_msg = err_str
                print(f"Model {model_name} failed: {err_str[:50]}...")
                
                # Jeśli błąd to 500 lub 503, czekamy dłużej
                if "500" in err_str or "503" in err_str or "INTERNAL" in err_str:
                    time.sleep(5)
                continue
            
        return f"Final Error: {last_error_msg}"

class EsmaCsvExtractor:
    """Pobiera i wstępnie czyści dane CASP z pliku CSV od ESMA."""
    
    def __init__(self, url: str):
        self.url = url
        
    def fetch_and_clean_csv(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.url, encoding='utf-8')
            df = df.fillna('')
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: " ".join(str(x).split()).strip())
            return df
        except Exception as e:
            raise ValueError(f"Błąd podczas pobierania lub przetwarzania CSV z ESMA: {e}")

class EsmaApiEnricher:
    """Wzbogaca dane o typ podmiotu na podstawie API ESMA."""
    
    async def fetch_entity_classification(self, session: aiohttp.ClientSession, lei: str) -> str:
        """
        Pobiera wszystkie unikalne etykiety klasyfikacji dla danego LEI,
        przeszukując wszystkie zwrócone dokumenty (rekordy) w API.
        """
        if not lei: return ""
        
        url = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select"
        
        # q=ae_lei:"{lei}" - szukamy w tym polu
        # fq=type_s:parent - tylko główne wpisy
        params = {
            'q': f'ae_lei:"{lei}"',
            'fq': 'type_s:parent',
            'rows': '10', # Zwiększamy do 10, bo podmiot może mieć więcej niż 1 licencję
            'wt': 'json'
        }
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        try:
            async with session.get(url=url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    docs = data.get('response', {}).get('docs',[])
                    
                    if docs:
                        # Zbieramy wszystkie etykiety ze wszystkich znalezionych dokumentów
                        labels = set()
                        for doc in docs:
                            val = doc.get('ae_entityTypeLabel')
                            if val:
                                if isinstance(val, list):
                                    labels.update(val)
                                else:
                                    labels.add(str(val))
                        
                        # Łączymy w jeden długi string, który potem przefiltrujemy
                        return " | ".join(sorted(labels))
                return ""
        except Exception as e:
            print(f"Błąd API ESMA dla LEI {lei}: {e}")
            return ""
            
    async def fetch_all_classifications(self, leis: list[str]) -> dict:
        """Pobiera klasyfikacje dla całej listy kodów LEI asynchronicznie."""
        async with aiohttp.ClientSession() as session:
            tasks =[self.fetch_entity_classification(session, lei) for lei in leis]
            
            # Poprawne wywołanie: async_tqdm.gather
            responses = await async_tqdm.gather(*tasks, desc="Weryfikacja statusów w API ESMA")
            
            return dict(zip(leis, responses))
            
def process_esma_data(df: pd.DataFrame, entity_types: dict) -> pd.DataFrame:
    """Przetwarza DataFrame, dodając nowe kolumny i flagi."""
    
    # 1. Flaga "Działalność w Polsce?"
    df['Działalność w Polsce?'] = df['ac_serviceCode_cou'].apply(
        lambda x: 'PRAWDA' if 'PL' in str(x).split('|') else 'FAŁSZ'
    )
    
    # 2. Usługi (z poprawionymi frazami)
    uslugi = {
        "Usługi - a. custody": "providing custody",
        "Usługi - b. trading platform": "operating a trading platform",
        "Usługi - c. exchange CA for funds": "exchange of crypto-assets for funds",
        "Usługi - d. exchange CA for CA": "exchange of crypto-assets for other crypto-assets",
        "Usługi - e. order execution for clients": "execution of orders",
        "Usługi - f. placing CA": "placing of crypto-assets",
        "Usługi - g. reception and transmission orders for clients": "reception and transmission",
        "Usługi - h. advice on CA": "providing advice",
        "Usługi - i. portfolio mgmt": "providing portfolio management",
        "Usługi - j. transfer of CA for clients": "providing transfer services"
    }
    
    for col_name, search_phrase in uslugi.items():
        df[col_name] = df['ac_serviceCode'].str.lower().fillna('').apply(
            lambda x: 1 if search_phrase.lower() in x else 0
        )
        
    # 3. Mapowanie typu podmiotu z API
    df['ESMA - Typ Podmiotu'] = df['ae_lei'].map(entity_types).fillna('')
    
    # 4. Klasyfikacja
    # Mapujemy pozostałe kolumny na LISTY fraz
    klasyfikacja = {
        "FI": ["investment firm"],
        "Fiinfra": ["organised trading facility", "multilateral trading facility", "regulated market"],
        "AssetMgmt": ["aifm"]
    }

    # Specjalny warunek dla kolumny Bank - sprawdza kolumnę "Banking License Status"
    # Używamy .str.contains('TAK', case=False) aby znaleźć "TAK" niezależnie od wielkości liter
    df['Bank'] = df['Banking License Status'].str.contains('TAK', case=False, na=False).astype(int)

    # Pozostałe klasyfikacje z kolumny "ESMA - Typ Podmiotu"
    for col_name, search_phrases in klasyfikacja.items():
        df[col_name] = df['ESMA - Typ Podmiotu'].str.lower().apply(
            lambda x: 1 if any(phrase in str(x) for phrase in search_phrases) else 0
        )
        
    return df

async def run_esma_pipeline() -> None:
    """Główna funkcja uruchamiająca cały pipeline."""
    
    # KROK 1: Ekstrakcja
    csv_url = "https://www.esma.europa.eu/sites/default/files/2024-12/CASPS.csv"
    extractor = EsmaCsvExtractor(url=csv_url)
    df_esma = extractor.fetch_and_clean_csv()
    print(f"Pobrano {len(df_esma)} podmiotów z rejestru ESMA.")
    
    # KROK 2: Wzbogacanie (API ESMA)
    enricher = EsmaApiEnricher()
    leis_to_check = df_esma['ae_lei'].unique().tolist()
    classifications = await enricher.fetch_all_classifications(leis=leis_to_check)

    # --- TUTAJ POPRAWKA ---
    # Tworzymy df_final na podstawie pobranego df_esma, aby móc do niego dopisywać dane
    df_final = df_esma.copy() 
    
    print("Weryfikacja licencji bankowych (to może zająć chwilę)...")
    verifier = BankingLicenseVerifier()
    
    # Dodajemy kolumnę na wyniki - teraz df_final już istnieje!
    df_final['Banking License Status'] = ""
    
    # Testowo na 20 pierwszych dla oszczędności czasu
    for index, row in tqdm(df_final.head(20).iterrows(), total=20, desc="Weryfikacja Banków"):
        status = verifier.check_banking_license(
            company_name=row['ae_lei_name'],
            lei=row['ae_lei'],
            website=row['ae_website'],
            address=row['ae_address']
        )
        df_final.at[index, 'Banking License Status'] = status
        time.sleep(4) # Rate limiting dla DDG
    
    # KROK 3: Przetwarzanie (Klasyfikacja)
    # Teraz przekazujemy df_final (który ma już kolumnę Banking License Status)
    df_final = process_esma_data(df=df_final, entity_types=classifications)
    
    # Zapis
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_path = os.path.join(base_dir, "data", "processed", "esma_casps_enriched.csv")
    
    # Upewnij się, że folder istnieje
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Sukces! Plik zapisano w: {output_path}")

if __name__ == "__main__":
    asyncio.run(run_esma_pipeline())
