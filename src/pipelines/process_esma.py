import pandas as pd
import requests
import asyncio
import aiohttp
import os
import sys
import re
import time
import pdfplumber  # <--- NOWA BIBLIOTEKA
from bs4 import BeautifulSoup
from ddgs import DDGS
from google import genai

is_ci = os.getenv('CI') == 'true'

# Inicjalizacja klienta Gemini
GEMINI_API_KEY = os.getenv(key="GEMINI_API_KEY")
gemini_client = None

if GEMINI_API_KEY:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    print("Brak klucza GEMINI_API_KEY. LLM nie zadziała.")

class SsmPdfVerifier:
    """Wyciąga kody LEI z pliku PDF SSM (Lista EBC)."""
    
    def __init__(
        self, 
        pdf_path: str
    ):
        self.pdf_path = pdf_path
        self.lei_set = set()

    def load_leis(self) -> None:
        """Parsuje PDF i zbiera wszystkie kody LEI bezpośrednio z warstwy tekstowej."""
        if not os.path.exists(path=self.pdf_path):
            print(f"⚠️ Plik PDF nie istnieje: {self.pdf_path}. Pomijam weryfikację PDF.")
            return

        print(f"Analiza pliku PDF: {self.pdf_path}...")
        try:
            # Poprawny sposób otwarcia pliku w pdfplumber (ścieżka jako pozycyjny argument)
            with pdfplumber.open(self.pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    
                    if text:
                        # Wyszukujemy wszystkie 20-znakowe ciągi alfanumeryczne
                        found_leis = re.findall(
                            pattern=r'\b[A-Z0-9]{20}\b', 
                            string=text
                        )
                        self.lei_set.update(found_leis)
                        
            print(f"Wczytano {len(self.lei_set)} unikalnych kodów LEI z pliku PDF.")
            
        except Exception as e:
            print(f"Błąd podczas odczytu PDF: {e}")

    def is_lei_in_pdf(
        self, 
        lei: str
    ) -> bool:
        """Weryfikuje, czy podany kod LEI znajduje się w pobranym zbiorze."""
        if not lei:
            return False
            
        return lei.strip().upper() in self.lei_set

class BankingLicenseVerifier:
    def __init__(self, pdf_verifier: SsmPdfVerifier = None):
        self.ddgs = DDGS()
        self.pdf_verifier = pdf_verifier

    def check_banking_license(self, company_name: str, lei: str, website: str, address: str) -> str:
        # --- KROK 1: Weryfikacja w PDF (Szybka ścieżka) ---
        if self.pdf_verifier and self.pdf_verifier.is_lei_in_pdf(lei):
            return "TAK [SSM/ECB]"

        # --- KROK 2: Jeśli nie ma w PDF, używamy wyszukiwarki i LLM ---
        query = f'"{company_name}" banking license EU authorisation'
        try:
            results = list(self.ddgs.text(query=query, region='pl-pl', max_results=3))
            search_context = "\n".join([f"Source: {r.get('href')}\nText: {r.get('body')}" for r in results])
        except Exception as e:
            return f"Search Error: {e}"

        prompt = f"""
        Jesteś ekspertem ds. regulacji bankowych w UE i specjalistą Compliance.
        Twoim zadaniem jest weryfikacja statusu licencji bankowej podmiotu.

        DANE PODMIOTU:
        - Nazwa: {company_name}
        - LEI: {lei}
        - Website: {website}
        - Siedziba: {address}

        KONTEKST Z WYSZUKIWARKI:
        {search_context}

        ZADANIE:
        1. Przeanalizuj dostarczony KONTEKST pod kątem informacji o posiadanej licencji bankowej w UE.
        2. Jeśli w kontekście nie ma jednoznacznej odpowiedzi, sprawdź czy podmiot jest powszechnie znanym bankiem z aktywną licencją.
        3. Jeśli nazwa zawiera "Bank/Banco", ale kontekst wskazuje na działalność doradczą, odpowiedz "NIE".

        FORMAT ODPOWIEDZI:
        - Jeśli podmiot posiada licencję: "TAK [Nazwa Organu Nadzorczego, np. KNF, ECB, BaFin]"
        - Jeśli podmiot na pewno nie posiada licencji: "NIE"
        - Jeśli nie można jednoznacznie stwierdzić: "BRAK DANYCH"

        Odpowiedz wyłącznie w powyższym formacie.
        """ 
        
        models_to_try = ['gemma-4-26b-a4b-it', 'gemma-4-31b-it', 'gemini-3.1-flash-lite']
        last_error_msg = "Unknown error"
    
        for i, model_name in enumerate(models_to_try):
            try:
                response = gemini_client.models.generate_content(
                    model=model_name, contents=prompt, config={"temperature": 0.1}
                )   
                clean_text = response.text.strip().replace('\n', ' ').replace('\r', ' ')
                return " ".join(clean_text.split())
            except Exception as e:
                err_str = str(e)
                last_error_msg = err_str
                if i < len(models_to_try) - 1:
                    print(f"⚠️ Model {model_name} nie powiódł się. Przełączam na {models_to_try[i+1]}...")
                if "500" in err_str or "503" in err_str or "INTERNAL" in err_str:
                    time.sleep(3)
                continue
            
        return f"Final Error: {last_error_msg}"

class EsmaCsvExtractor:
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
            raise ValueError(f"Błąd podczas pobierania CSV z ESMA: {e}")

class EsmaApiEnricher:
    async def fetch_entity_classification(self, session: aiohttp.ClientSession, lei: str) -> str:
        if not lei: return ""
        url = "https://registers.esma.europa.eu/solr/esma_registers_upreg/select"
        params = {'q': f'ae_lei:"{lei}"', 'fq': 'type_s:parent', 'rows': '10', 'wt': 'json'}
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            async with session.get(url=url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    docs = data.get('response', {}).get('docs',[])
                    if docs:
                        labels = set()
                        for doc in docs:
                            val = doc.get('ae_entityTypeLabel')
                            if val:
                                if isinstance(val, list): labels.update(val)
                                else: labels.add(str(val))
                        return " | ".join(sorted(labels))
                return ""
        except Exception as e:
            print(f"Błąd API ESMA dla LEI {lei}: {e}")
            return ""
            
    async def fetch_all_classifications(self, leis: list[str]) -> dict:
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_entity_classification(session, lei) for lei in leis]
            print(f"Wysyłanie żądań do API ESMA dla {len(leis)} podmiotów...")
            final_responses = await asyncio.gather(*tasks)
            print("Zakończono pobieranie z API ESMA.")
            return dict(zip(leis, final_responses))

def process_esma_data(df: pd.DataFrame, entity_types: dict) -> pd.DataFrame:
    df['Działalność w Polsce?'] = df['ac_serviceCode_cou'].apply(
        lambda x: 'PRAWDA' if 'PL' in str(x).split('|') else 'FAŁSZ'
    )
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
    df['ESMA - Typ Podmiotu'] = df['ae_lei'].map(entity_types).fillna('')
    klasyfikacja = {
        "FI": ["investment firm"],
        "Fiinfra": ["organised trading facility", "multilateral trading facility", "regulated market"],
        "AssetMgmt": ["aifm"]
    }
    df['Bank'] = df['Banking License Status'].str.contains('TAK', case=False, na=False).astype(int)
    for col_name, search_phrases in klasyfikacja.items():
        df[col_name] = df['ESMA - Typ Podmiotu'].str.lower().apply(
            lambda x: 1 if any(phrase in str(x) for phrase in search_phrases) else 0
        )
    return df

async def run_esma_pipeline() -> None:
    # --- Konfiguracja ścieżek ---
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pdf_path = os.path.join(base_dir, "data", "raw", "ssm.pdf")
    output_path = os.path.join(base_dir, "data", "processed", "esma_casps_enriched.csv")

    # KROK 1: Ekstrakcja CSV
    csv_url = "https://www.esma.europa.eu/sites/default/files/2024-12/CASPS.csv"
    extractor = EsmaCsvExtractor(url=csv_url)
    df_esma = extractor.fetch_and_clean_csv()
    print(f"Pobrano {len(df_esma)} podmiotów z rejestru ESMA.")
    
    # KROK 2: Wczytanie PDF SSM (Szybka weryfikacja)
    pdf_verifier = SsmPdfVerifier(pdf_path=pdf_path)
    pdf_verifier.load_leis()
    
    # KROK 3: Wzbogacanie API ESMA
    enricher = EsmaApiEnricher()
    leis_to_check = df_esma['ae_lei'].unique().tolist()
    classifications = await enricher.fetch_all_classifications(leis=leis_to_check)

    df_final = df_esma.copy() 
    
    print("Weryfikacja licencji bankowych...")
    # Przekazujemy pdf_verifier do weryfikatora
    verifier = BankingLicenseVerifier(pdf_verifier=pdf_verifier)
    df_final['Banking License Status'] = ""
    
    test_sample = df_final.head(20)
    total = len(test_sample)
    
    for index, row in test_sample.iterrows():
        company_name = str(row['ae_lei_name'])
        print(f"[{index+1}/{total}] Sprawdzanie: {company_name[:50]}...", end=" ", flush=True)
        
        try:
            status = verifier.check_banking_license(
                company_name=company_name,
                lei=row['ae_lei'],
                website=row['ae_website'],
                address=row['ae_address']
            )
            print(f"Wynik: {status}")
            df_final.at[index, 'Banking License Status'] = status
        except Exception as e:
            print(f"KRYTYCZNY błąd: {e}")
            df_final.at[index, 'Banking License Status'] = "CRITICAL_FAILURE"
            
        time.sleep(4) 
    
    # KROK 4: Przetwarzanie końcowe
    df_final = process_esma_data(df=df_final, entity_types=classifications)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\nSukces! Plik zapisano w: {output_path}")

if __name__ == "__main__":
    asyncio.run(run_esma_pipeline())