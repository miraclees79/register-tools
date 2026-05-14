import os
import time
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS 
from tqdm import tqdm
from google import genai  # Nowe, wspierane SDK Google

# Inicjalizacja klienta Gemini
GEMINI_API_KEY = os.getenv(key="GEMINI_API_KEY")
gemini_client = None

if GEMINI_API_KEY:
    gemini_client = genai.Client(
        api_key=GEMINI_API_KEY
    )
else:
    print("Brak klucza GEMINI_API_KEY. LLM nie zadziała.")

class WebAnalyzer:
    def __init__(self):
        self.ddgs = DDGS()

    def find_website(
        self, 
        company_name: str
    ) -> str:
        query = f'"{company_name}" krypto OR kryptowaluty OR crypto exchange'
        try:
            results = list(
                self.ddgs.text(
                    keywords=query, 
                    region='pl-pl', 
                    max_results=1
                )
            )
            if results:
                # dict.get musi być wywołane pozycyjnie
                return results[0].get('href', '')
        except Exception as e:
            print(f"Błąd wyszukiwania dla {company_name}: {e}")
        return ""

    def scrape_website_text(
        self, 
        url: str
    ) -> str:
        if not url: 
            return ""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
            }
            response = requests.get(
                url=url, 
                headers=headers, 
                timeout=5
            )
            if response.status_code == 200:
                soup = BeautifulSoup(
                    markup=response.text, 
                    features='html.parser'
                )
                for script in soup(name=["script", "style", "nav", "footer"]):
                    script.extract()
                text = soup.get_text(
                    separator=' ', 
                    strip=True
                )
                return text[:5000]
            else:
                return f"HTTP {response.status_code}"
        except Exception as e:
            return f"Błąd pobierania: {str(e)}"

    def synthesize_with_llm(
        self, 
        company_name: str, 
        website_text: str
    ) -> str:
        if not gemini_client: 
            return "Brak klucza API."
        if not website_text or "Błąd" in website_text or "HTTP" in website_text:
            return "Brak zawartości strony do analizy."

        prompt = f"""
        Jesteś analitykiem finansowym (OSINT) badającym rynek kryptowalut.
        Poniżej znajduje się tekst ze strony internetowej podmiotu zarejestrowanego jako VASP.
        Nazwa podmiotu: {company_name}
        
        Tekst ze strony:
        {website_text}
        
        Twoje zadanie to odpowiedzieć zwięźle (max 3-4 zdania):
        1. Jaki jest główny profil działalności tej firmy?
        2. Czy kierują swoje usługi do klientów detalicznych (B2C) czy instytucjonalnych (B2B)?
        Jeśli tekst nie zawiera jednoznacznych informacji, napisz: "Strona nie zawiera wyraźnych informacji."
        """
        
        try:
            # Nowy sposób wywoływania modelu w zaktualizowanej bibliotece
            response = gemini_client.models.generate_content(
                model='gemini-1.5-flash',
                contents=prompt
            )
            if response.text:
                return response.text.replace(
                    old='\n', 
                    new=' '
                ).strip()
            return "Brak odpowiedzi modelu."
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


def analyze_shareholder_clusters(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Wykrywa klastry powiązań kapitałowych na podstawie obecnych 
    i historycznych udziałowców (firmy oraz osoby fizyczne maskowane PESELem).
    """
    # 1. Zabezpieczenie przed brakami danych (NaN)
    aktualni_udzialowcy = df['udzialowcy'].fillna(
        value=''
    )
    historyczni_udzialowcy = df['historyczni_udzialowcy'].fillna(
        value=''
    )
    
    # 2. Połączenie wszystkich powiązań w jeden wspólny ciąg
    wszyscy_udzialowcy_raw = aktualni_udzialowcy + " | " + historyczni_udzialowcy
    
    # 3. Rozbicie ciągu znaków na listę w oparciu o separator
    df['udzialowcy_lista'] = wszyscy_udzialowcy_raw.str.split(
        pat=" | "
    )
    
    # 4. Funkcja do czyszczenia danych (usunięcie informacji o kwotach i datach)
    def clean_shareholders(
        sh_list: list
    ) -> list:
        cleaned_set = set()
        for sh in sh_list:
            sh_str = str(sh).strip()
            if not sh_str or sh_str == "|":
                continue
                
            # Usuwamy wszystko co jest w nawiasach kwadratowych: "[600 UDZIAŁÓW...]" oraz "[od 18.11...]"
            # Dzięki temu zostaje samo: "SCALE COMPUTING SOLUTIONS LTD" lub "W******* G******** (PESEL: 5**********)"
            sh_clean = re.sub(
                pattern=r'\[.*?\]', 
                repl='', 
                string=sh_str
            ).strip()
            
            if sh_clean:
                cleaned_set.add(sh_clean)
                
        return list(cleaned_set)
        
    df['znormalizowani_udzialowcy'] = df['udzialowcy_lista'].apply(
        func=clean_shareholders
    )
    
    # 5. Globalne zliczenie wystąpień dla każdego "czystego" udziałowca w rejestrze
    exploded_sh = df['znormalizowani_udzialowcy'].explode()
    sh_counts = exploded_sh.value_counts()
    
    # 6. Wyciągnięcie najwyższej liczby powiązań dla danej spółki
    def get_max_sh_cluster(
        sh_list: list
    ) -> int:
        if not sh_list:
            return 0
        counts = [sh_counts.get(key=s, default=0) for s in sh_list]
        return max(counts)
        
    # 7. Zbudowanie czytelnego podsumowania (kto konkretnie jest seryjnym udziałowcem)
    def get_sh_cluster_details(
        sh_list: list
    ) -> str:
        if not sh_list:
            return ""
        details = []
        for s in sh_list:
            poziom_wystapien = sh_counts.get(key=s, default=0)
            # Zapisujemy tylko tych, którzy przewijają się przez więcej niż 1 firmę
            if poziom_wystapien > 1:
                details.append(f"{s} [występuje w {poziom_wystapien} podmiotach]")
                
        if not details:
            return "Brak powiązań z innymi podmiotami"
            
        return " | ".join(details)
        
    df['max_powiazania_udzialowca'] = df['znormalizowani_udzialowcy'].apply(
        func=get_max_sh_cluster
    )
    df['powiazani_udzialowcy_szczegoly'] = df['znormalizowani_udzialowcy'].apply(
        func=get_sh_cluster_details
    )
    
    # 8. Nadanie flagi ryzyka (skalarnej)
    def assign_sh_risk(
        cluster_size: int
    ) -> str:
        if cluster_size >= 3:
            return 'Wysokie' # Ten sam udziałowiec w 3+ firmach krypto to silny sygnał
        elif cluster_size == 2:
            return 'Średnie'
        else:
            return 'Niskie'

    df['ryzyko_powiazan_kapitalowych'] = df['max_powiazania_udzialowca'].apply(
        func=assign_sh_risk
    )
    
    # 9. Sprzątanie
    df.drop(
        columns=['udzialowcy_lista', 'znormalizowani_udzialowcy'], 
        inplace=True
    )
    
    return df


def run_advanced_pipeline() -> None:
    base_dir = os.path.dirname(
        p=os.path.dirname(
            p=os.path.dirname(
                p=__file__
            )
        )
    )
    input_path = os.path.join(
        base_dir, 
        "data", 
        "processed", 
        "enriched_crypto_register.csv"
    )
    output_path = os.path.join(
        base_dir, 
        "data", 
        "processed", 
        "osint_crypto_register.csv"
    )

    print("Wczytywanie bazy...")
    df = pd.read_csv(
        filepath_or_buffer=input_path
    )

    print("Analiza klastrów adresowych...")
    df = analyze_address_clusters(
        df=df
    )
    
    # NAPRAWIONY BŁĄD KeyError (używamy teraz nowych nazw kolumn dla adresów historycznych)
    df_filtered_addresses = df[df['najwiekszy_klaster_adresowy'] > 1]
    top_addresses = df_filtered_addresses['krs_adres_aktualny'].unique()
    print(f"Znaleziono {len(top_addresses)} klastrów adresowych (biura obsługujące wiele VASPów).")
    
    # KROK 1.5: Analiza powiązań udziałowców
    print("Analiza powiązań udziałowców (kapitałowych)...")
    df = analyze_shareholder_clusters(df=df)
    
    # KROK 2: Analiza WWW z użyciem AI
    print("Uruchamianie wyszukiwania i analizy LLM (to może potrwać)...")
    analyzer = WebAnalyzer()
    
    df['website_url'] = ""
    df['ai_summary'] = ""

    # Używamy tqdm dla wyświetlania paska postępu
    for index, row in tqdm(iterable=df.iterrows(), total=len(df), desc="Analiza AI"):
        company_name = str(row['Imię i Nazwisko / Nazwa firmy'])
        
        if pd.notna(row['Numer KRS']):
            url = analyzer.find_website(
                company_name=company_name
            )
            df.at[index, 'website_url'] = url
            
            if url:
                text = analyzer.scrape_website_text(
                    url=url
                )
                if len(text) > 50:
                    summary = analyzer.synthesize_with_llm(
                        company_name=company_name, 
                        website_text=text
                    )
                    df.at[index, 'ai_summary'] = summary
                else:
                    df.at[index, 'ai_summary'] = "Nie udało się pobrać treści strony."
            
            time.sleep(4)
        else:
            df.at[index, 'ai_summary'] = "Pominięto (brak KRS)."

    print("\nZapisywanie osint_crypto_register.csv...")
    df.to_csv(
        path_or_buf=output_path, 
        index=False, 
        encoding='utf-8'
    )
    print("Zakończono pomyślnie!")

if __name__ == "__main__":
    run_advanced_pipeline()