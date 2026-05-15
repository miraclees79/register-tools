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
        df[col_name] = df['ac_comments'].str.lower().fillna('').apply(
            lambda x: 1 if search_phrase.lower() in x else 0
        )
        
    # 3. Mapowanie typu podmiotu z API
    df['ESMA - Typ Podmiotu'] = df['ae_lei'].map(entity_types).fillna('')
    
    # 4. Klasyfikacja - szukamy w nowym polu 'ae_entityTypeLabel' (które teraz jest w kolumnie ESMA - Typ Podmiotu)
    # Mapujemy wartości z ESMA na Twoje kolumny
    klasyfikacja = {
        "Bank": "credit institution",
        "FI": "investment firm",
        "Fiinfra": "financial infrastructure",
        "AssetMgmt": "asset management"
    }
    
    for col_name, search_phrase in klasyfikacja.items():
        df[col_name] = df['ESMA - Typ Podmiotu'].str.lower().apply(
            lambda x: 1 if search_phrase in x else 0
        )
        
    return df

async def run_esma_pipeline() -> None:
    """Główna funkcja uruchamiająca cały pipeline."""
    
    # KROK 1: Ekstrakcja
    csv_url = "https://www.esma.europa.eu/sites/default/files/2024-12/CASPS.csv"
    extractor = EsmaCsvExtractor(url=csv_url)
    df_esma = extractor.fetch_and_clean_csv()
    print(f"Pobrano {len(df_esma)} podmiotów z rejestru ESMA.")
    
    # KROK 2: Wzbogacanie
    enricher = EsmaApiEnricher()
    leis_to_check = df_esma['ae_lei'].unique().tolist()
    
    # Zmieniona nazwa zmiennej na 'classifications' dla jasności
    classifications = await enricher.fetch_all_classifications(leis=leis_to_check)
    
    # KROK 3: Przetwarzanie i zapis
    df_final = process_esma_data(df=df_esma, entity_types=classifications)
    
    # Zapis
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    output_path = os.path.join(base_dir, "data", "processed", "esma_casps_enriched.csv")
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Sukces! Plik zapisano w: {output_path}")

if __name__ == "__main__":
    asyncio.run(run_esma_pipeline())
