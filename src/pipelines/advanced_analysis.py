import os
import time
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS
import google.generativeai as genai

# Inicjalizacja modelu Gemini
# Ustaw zmienną środowiskową: export GEMINI_API_KEY="twój-klucz"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    # Gemini 1.5 Flash jest idealny: szybki, tani/darmowy i ma ogromne okno kontekstowe
    model = genai.GenerativeModel('gemini-1.5-flash')
else:
    print("Brak klucza GEMINI_API_KEY. LLM nie zadziała.")

class WebAnalyzer:
    def __init__(self):
        self.ddgs = DDGS()

    def find_website(self, company_name: str) -> str:
        """Wyszukuje pierwszy pasujący link dla firmy (preferowane polskie wyniki)"""
        query = f'"{company_name}" krypto OR kryptowaluty OR crypto exchange'
        try:
            results = list(self.ddgs.text(query, region='pl-pl', max_results=1))
            if results:
                return results[0].get('href', '')
        except Exception as e:
            print(f"Błąd wyszukiwania dla {company_name}: {e}")
        return ""

    def scrape_website_text(self, url: str) -> str:
        """Pobiera i czyści tekst z podanej strony WWW"""
        if not url: return ""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            # Krótki timeout, bo nie chcemy zawiesić pipeline'u
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Usuwamy skrypty i style
                for script in soup(["script", "style", "nav", "footer"]):
                    script.extract()
                text = soup.get_text(separator=' ', strip=True)
                return text[:5000] # Ograniczamy do 5000 znaków (wystarczy do oceny)
            else:
                return f"HTTP {response.status_code}"
        except Exception as e:
            return f"Błąd pobierania: {str(e)}"

    def synthesize_with_llm(self, company_name: str, website_text: str) -> str:
        """Wysyła zebrany tekst do Google Gemini w celu syntezy OSINT"""
        if not GEMINI_API_KEY: return "Brak klucza API."
        if not website_text or "Błąd" in website_text or "HTTP" in website_text:
            return "Brak zawartości strony do analizy."

        prompt = f"""
        Jesteś analitykiem finansowym (OSINT) badającym rynek kryptowalut.
        Poniżej znajduje się tekst ze strony internetowej podmiotu zarejestrowanego jako VASP (Virtual Asset Service Provider).
        Nazwa podmiotu: {company_name}
        
        Tekst ze strony:
        {website_text}
        
        Twoje zadanie to odpowiedzieć zwięźle (max 3-4 zdania):
        1. Jaki jest główny profil działalności tej firmy (kantor online, kantor stacjonarny, giełda, bramka płatnicza, firma consultingowa)?
        2. Czy kierują swoje usługi do klientów detalicznych (B2C) czy instytucjonalnych (B2B)?
        Jeśli tekst nie zawiera jednoznacznych informacji o profilu krypto, napisz: "Strona nie zawiera wyraźnych informacji o usługach kryptowalutowych."
        """
        
        try:
            response = model.generate_content(prompt)
            return response.text.replace('\n', ' ').strip()
        except Exception as e:
            return f"Błąd LLM: {str(e)}"


def analyze_address_clusters(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Wykrywa wirtualne biura / klastry rejestracyjne na podstawie 
    wszystkich adresów (aktualnych i historycznych).
    """
    # 1. Zabezpieczenie przed brakiem danych (NaN) i ujednolicenie
    aktualny_adres = df['krs_adres_aktualny'].fillna(
        value=''
    )
    historyczne_adresy = df['krs_adresy_historyczne'].fillna(
        value=''
    ).replace(
        to_replace="Brak zmian adresu", 
        value=""
    )
    
    # 2. Połączenie wszystkich adresów w jeden ciąg oddzielony separatorem
    all_addresses_raw = aktualny_adres + " -> " + historyczne_adresy
    
    # 3. Podział na listę adresów dla każdego podmiotu
    df['adresy_lista'] = all_addresses_raw.str.split(
        pat=" -> "
    )
    
    # 4. Funkcja wewnętrzna do oczyszczania i normalizacji listy adresów
    def clean_addresses(
        addr_list: list
    ) -> list:
        cleaned_set = set()
        for addr in addr_list:
            addr_str = str(addr).lower()
            addr_str = re.sub(
                pattern=r'ul\.', 
                repl='', 
                string=addr_str
            )
            addr_str = re.sub(
                pattern=r'[^a-z0-9ąćęłńóśźż/ ]', 
                repl='', 
                string=addr_str
            )
            addr_str = re.sub(
                pattern=r'\s+', 
                repl=' ', 
                string=addr_str
            ).strip()
            
            if addr_str:
                cleaned_set.add(addr_str)
        return list(cleaned_set)
        
    df['znormalizowane_adresy'] = df['adresy_lista'].apply(
        func=clean_addresses
    )
    
    # 5. Zliczenie wystąpień dla wszystkich znormalizowanych adresów w rejestrze
    exploded_addresses = df['znormalizowane_adresy'].explode()
    address_counts = exploded_addresses.value_counts()
    
    # 6. Funkcje wyciągające wyniki (szukamy największego klastra z historii firmy)
    def get_max_cluster(
        addr_list: list
    ) -> int:
        if not addr_list:
            return 0
        counts = [address_counts.get(key=a, default=0) for a in addr_list]
        return max(counts)
        
    def get_cluster_details(
        addr_list: list
    ) -> str:
        if not addr_list:
            return ""
        # Zapiszmy jakie klastry firma "odwiedziła" w formacie np. "zwykla ulica 1 [1] | mroczny adres 99 [45]"
        details = [f"{a} [{address_counts.get(key=a, default=0)}]" for a in addr_list]
        return " | ".join(details)
        
    df['najwiekszy_klaster_adresowy'] = df['znormalizowane_adresy'].apply(
        func=get_max_cluster
    )
    df['szczegoly_adresow_historycznych'] = df['znormalizowane_adresy'].apply(
        func=get_cluster_details
    )
    
    # 7. Flaga ryzyka przypisywana na podstawie najbardziej zatłoczonego adresu w historii podmiotu
    def assign_risk(
        cluster_size: int
    ) -> str:
        if cluster_size >= 3:
            return 'Wysokie'
        elif cluster_size <= 1:
            return 'Niskie'
        else:
            return 'Średnie'

    df['wirtualne_biuro_ryzyko'] = df['najwiekszy_klaster_adresowy'].apply(
        func=assign_risk
    )
    
    # 8. Usuwanie kolumn tymczasowych ułatwiających obliczenia
    df.drop(
        columns=['adresy_lista', 'znormalizowane_adresy'], 
        inplace=True
    )
    
    return df

def run_advanced_pipeline():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    input_path = os.path.join(base_dir, "data", "processed", "enriched_crypto_register.csv")
    output_path = os.path.join(base_dir, "data", "processed", "osint_crypto_register.csv")

    print("Wczytywanie bazy...")
    df = pd.read_csv(input_path)

    # KROK 1: Analiza adresów (szybkie operacje w Pandas)
    print("Analiza klastrów adresowych...")
    df = analyze_address_clusters(df)
    
    # Wypiszmy najpopularniejsze adresy:
    top_addresses = df[df['wielkosc_klastra_adresowego'] > 1]['krs_adres'].unique()
    print(f"Znaleziono {len(top_addresses)} klastrów adresowych (biura obsługujące wiele VASPów).")

    # KROK 2: Analiza WWW z użyciem AI
    print("Uruchamianie wyszukiwania i analizy LLM (to może potrwać)...")
    analyzer = WebAnalyzer()
    
    # Tworzymy puste kolumny
    df['website_url'] = ""
    df['ai_summary'] = ""

    # Aby nie zużyć limitów (i czasu), analizujemy tylko pierwsze 10 podmiotów (usuń [:10] dla pełnego przebiegu)
    for index, row in df.head(10).iterrows():
        company_name = str(row['Imię i Nazwisko / Nazwa firmy'])
        print(f"\n[{index+1}] Badanie: {company_name}")
        
        # Omijamy osoby fizyczne (brak KRS zazwyczaj oznacza działalność CEIDG, tam trudniej o precyzyjne WWW, ale można włączyć)
        if pd.notna(row['Numer KRS']):
            url = analyzer.find_website(company_name)
            print(f" Znaleziono URL: {url}")
            df.at[index, 'website_url'] = url
            
            if url:
                text = analyzer.scrape_website_text(url)
                if len(text) > 50: # Pomyślnie pobrano jakiś tekst
                    summary = analyzer.synthesize_with_llm(company_name, text)
                    df.at[index, 'ai_summary'] = summary
                    print(f" AI: {summary[:100]}...")
                else:
                    df.at[index, 'ai_summary'] = "Nie udało się pobrać treści strony (blokada / brak tekstu)."
            
            # Rate limiting API LLM i wyszukiwarki
            time.sleep(4)
        else:
            df.at[index, 'ai_summary'] = "Pominięto (brak KRS)."

    print("\nZapisywanie osint_crypto_register.csv...")
    df.drop(columns=['normalized_address'], inplace=True) # usuwamy kolumnę techniczną
    df.to_csv(output_path, index=False, encoding='utf-8')
    print("Zakończono!")

if __name__ == "__main__":
    run_advanced_pipeline()