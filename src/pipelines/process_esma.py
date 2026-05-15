import pandas as pd
import requests
import asyncio
import aiohttp
from tqdm.asyncio import tqdm
import os

class EsmaCsvExtractor:
    """Pobiera i wstępnie czyści dane CASP z pliku CSV od ESMA."""
    
    def __init__(self, url: str):
        self.url = url
        
    def fetch_and_clean_csv(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                filepath_or_buffer=self.url,
                encoding='utf-8'
            )
            # Wstępne czyszczenie - zamiana NaN na puste stringi i usuwanie białych znaków
            df = df.fillna('')
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: " ".join(str(x).split()).strip())
            return df
        except Exception as e:
            raise ValueError(f"Błąd podczas pobierania lub przetwarzania CSV z ESMA: {e}")
            
class EsmaApiEnricher:
    """Wzbogaca dane o typ podmiotu na podstawie API ESMA."""
    
    BASE_URL = "https://registers.esma.europa.eu/solr/esma_registers_entities_rev/select"
    
    async def fetch_entity_type(self, session: aiohttp.ClientSession, lei: str) -> str:
        """Asynchronicznie pobiera dane dla pojedynczego kodu LEI."""
        if not lei:
            return ""
        
        # ==========================================================
        # POPRAWKA: Zmiana z 'entity_code' na standardowe 'LEI'
        # ==========================================================
        params = {
            'q': f'LEI:"{lei}"',
            'wt': 'json'
        }
        
        try:
            async with session.get(url=self.BASE_URL, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    docs = data.get('response', {}).get('docs', [])
                    if docs:
                        # Typ podmiotu jest w polu 'entity_type'
                        return docs[0].get('entity_type', 'Unknown')
                return "Not Found"
        except Exception:
            return "API Error"
            
    async def fetch_all_entity_types(self, leis: list[str]) -> dict:
        """Pobiera typy dla całej listy kodów LEI asynchronicznie."""
        results = {}
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_entity_type(session=session, lei=lei) for lei in leis]
            
            api_responses = await tqdm.gather(
                *tasks, 
                desc="Odpytywanie API ESMA"
            )
            
            for lei, entity_type in zip(leis, api_responses):
                results[lei] = entity_type
                
        return results

def process_esma_data(
    df: pd.DataFrame, 
    entity_types: dict
) -> pd.DataFrame:
    """Przetwarza DataFrame, dodając nowe kolumny i flagi."""
    
    # 1. Flaga "Działalność w Polsce?"
    df['Działalność w Polsce?'] = df['ac_serviceCode_cou'].apply(
        lambda x: 'PRAWDA' if 'PL' in str(x).split('|') else 'FAŁSZ'
    )
    
    # 2. Kolumny 0/1 dla usług (od a. do j.)
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
        # POPRAWKA 1: Dodajemy .lower() aby wyszukiwanie było niewrażliwe na wielkość liter
        df[col_name] = df['ac_serviceCode'].str.lower().apply(
            lambda x: 1 if search_phrase in str(x) else 0
        )
        
    # 3. Mapowanie i flagowanie typów podmiotów z API
    # POPRAWKA 2: Zostawiamy surową kolumnę i nadajemy jej czytelną nazwę
    df['ESMA - Typ Podmiotu'] = df['ae_lei'].map(entity_types)
    
    klasyfikacja = {
        "Bank": "Credit institution",
        "FI": "Investment Firm",
        "Fiinfra": "Financial infrastructure entity",
        "AssetMgmt": "Asset management entity"
    }
    
    for col_name, search_phrase in klasyfikacja.items():
        df[col_name] = df['ESMA - Typ Podmiotu'].apply(
            lambda x: 1 if search_phrase in str(x) else 0
        )
        
    return df

async def run_esma_pipeline() -> None:
    """Główna funkcja uruchamiająca cały pipeline dla danych ESMA."""
    
    # --- KROK 1: Ekstrakcja i czyszczenie pliku CSV ---
    print("Pobieranie pliku CSV z rejestrem CASP od ESMA...")
    csv_url = "https://www.esma.europa.eu/sites/default/files/2024-12/CASPS.csv"
    extractor = EsmaCsvExtractor(url=csv_url)
    df_esma = extractor.fetch_and_clean_csv()
    
    print(f"Pobrano {len(df_esma)} podmiotów z rejestru ESMA.")
    
    # --- KROK 2: Wzbogacanie danych przez API ---
    print("Wzbogacanie danych o typ podmiotu z API ESMA (to może potrwać)...")
    enricher = EsmaApiEnricher()
    leis_to_check = df_esma['ae_lei'].unique().tolist()
    entity_types_map = await enricher.fetch_all_entity_types(leis=leis_to_check)
    
    # --- KROK 3: Finalne przetwarzanie i zapis do pliku ---
    print("Przetwarzanie danych i generowanie flag...")
    df_final = process_esma_data(df=df_esma, entity_types=entity_types_map)
    
    # Zapisywanie wyniku
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_path = os.path.join(base_dir, "data", "processed", "esma_casps_enriched.csv")
    
    df_final.to_csv(
        path_or_buf=output_path,
        index=False,
        encoding='utf-8'
    )
    
    print(f"Sukces! Przetworzone dane ESMA zapisano w pliku: {output_path}")

if __name__ == "__main__":
    # Uruchamiamy pętlę zdarzeń asyncio
    asyncio.run(run_esma_pipeline())
