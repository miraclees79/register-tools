import os
import time
import pandas as pd
import requests
import re
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
else:
    print("Brak klucza GEMINI_API_KEY. LLM nie zadziała.")

class WebAnalyzer:
    def __init__(self):
        self.ddgs = DDGS()

    def find_websites(
        self, 
        company_name: str,
        company_address: str,
        shareholder_name: str | None = None
    ) -> list:
        # Zestaw form prawnych do usunięcia, aby szukać "czystej" nazwy marki
        formy_prawne = [
            "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ", "SP. Z O.O.", "SP. Z O. O.",
            "SPÓŁKA AKCYJNA", "S.A.", "PROSTA SPÓŁKA AKCYJNA", "P.S.A.",
            "SPÓŁKA KOMANDYTOWA", "SP. K."
        ]

        # 1. Czyszczenie nazwy głównej firmy
        clean_company = company_name.upper()
        for forma in formy_prawne:
            clean_company = clean_company.replace(forma, "")
        # Usunięcie podwójnych spacji
        clean_company = " ".join(clean_company.split())
        
        # Budowa zapytania bazowego (np. "DIWISS" krypto)
        search_query = f'"{clean_company}" adres: {company_address} krypto'
        
        # 2. Czyszczenie nazwy udziałowca (jeśli jest i nie jest zanonimizowany PESELem)
        if shareholder_name and "PESEL" not in shareholder_name:
            clean_shareholder = shareholder_name.upper()
            # Dla udziałowców dorzucamy jeszcze zagraniczne sufiksy
            for forma in formy_prawne + ["LTD", "LIMITED", "LLC"]:
                clean_shareholder = clean_shareholder.replace(forma, "")
            clean_shareholder = " ".join(clean_shareholder.split())
            
            if clean_shareholder:
                search_query += f' OR "{clean_shareholder}"'

        valid_links = []
        # Rozszerzona czarna lista domen (dodano tablicafirm)
        excluded_domains = [
            "krs", "aleo.com", "rejestr.io", "owg.pl", 
            "infoveriti", "biznes.gov.pl", "ceidg", 
            "krs-online", "gowork", "panoramafirm",
            "tablicafirm"
        ]
        
        try:
            results = list(
                self.ddgs.text(
                    query=search_query,
                    region='pl-pl', 
                    max_results=10
                )
            )
            for res in results:
                href = res.get('href', '').lower()
                
                # Jeśli w linku znajduje się zakazana domena - pomijamy
                if any(ex in href for ex in excluded_domains):
                    continue
                    
                if href:
                    valid_links.append(res.get('href', ''))
                
                # Zatrzymujemy po zebraniu 3 dobrych linków
                if len(valid_links) == 3:
                    break
                    
        except Exception as e:
            print(f"Błąd wyszukiwania dla {clean_company}: {e}")
            
        return valid_links

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
        company_address: str, 
        website_text: str
    ) -> str:
        if not gemini_client: 
            return "Brak klucza API."
        if not website_text or "Błąd" in website_text or "HTTP" in website_text:
            return "Brak zawartości strony do analizy."

        prompt = f"""
        Jesteś analitykiem finansowym (OSINT) badającym rynek kryptowalut.
        Poniżej znajduje się tekst z maksymalnie 3 najlepszych stron WWW powiązanych z podmiotem (VASP).
        Nazwa podmiotu: {company_name}
        Adres siedziby: {company_address}
        
        Tekst ze stron:
        {website_text}
        
        Twoje zadanie to odpowiedzieć zwięźle (max 3-4 zdania):
        1. Jaki jest główny profil działalności tej firmy?
        2. Czy kierują swoje usługi do klientów detalicznych (B2C) czy instytucjonalnych (B2B)?
        Jeśli tekst to tylko szczątkowe informacje rejestrowe, napisz: "Brak wyraźnych informacji o profilu usług."
        """
        
        try:
            response = gemini_client.models.generate_content(
                model='gemma-4-31b-it',
                contents=prompt
            )
            if response.text:
                # Usunięto argumenty nazwane old= i new=
                return response.text.replace('\n', ' ').strip()
            return "Brak odpowiedzi modelu."
        except Exception as e:
            return f"Błąd LLM: {str(e)}"


def analyze_address_clusters(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Wykrywa wirtualne biura / klastry rejestracyjne na podstawie 
    wszystkich adresów (aktualnych i historycznych), dodając ID klastra.
    """
    # 1. Przygotowanie danych adresowych
    aktualny_adres = df['krs_adres_aktualny'].fillna('')
    historyczne_adresy = df['krs_adresy_historyczne'].fillna('').replace("Brak zmian adresu", "")
    all_addresses_raw = aktualny_adres + " -> " + historyczne_adresy
    
    df['adresy_lista'] = all_addresses_raw.str.split(" -> ")
    
    # 2. Normalizacja adresów
    def clean_addresses(addr_list: list) -> list:
        cleaned_set = set()
        for addr in addr_list:
            addr_str = str(addr).lower()
            addr_str = re.sub(r'ul\.', '', addr_str)
            addr_str = re.sub(r'[^a-z0-9ąćęłńóśźż/ ]', '', addr_str)
            addr_str = re.sub(r'\s+', ' ', addr_str).strip()
            if addr_str:
                cleaned_set.add(addr_str)
        return list(cleaned_set)
        
    df['znormalizowane_adresy'] = df['adresy_lista'].apply(clean_addresses)
    
    # 3. Globalne zliczenie wystąpień dla każdego adresu
    exploded_addresses = df['znormalizowane_adresy'].explode().dropna()
    address_counts = exploded_addresses.value_counts()
    
    # 4. Wyciągnięcie kluczowych metryk (rozmiar i ID klastra)
    def get_max_cluster_size(addr_list: list) -> int:
        if not addr_list: return 0
        return max([address_counts.get(a, 0) for a in addr_list])
        
    def get_main_address_cluster_id(addr_list: list) -> str:
        if not addr_list: return ""
        # Jako ID klastra zwracamy ten adres z historii firmy, który jest najbardziej "zatłoczony"
        return max(addr_list, key=lambda addr: address_counts.get(addr, 0))

    df['najwiekszy_klaster_adresowy'] = df['znormalizowane_adresy'].apply(get_max_cluster_size)
    df['klaster_adresowy_id'] = df['znormalizowane_adresy'].apply(get_main_address_cluster_id)

    # 5. Przypisanie flagi ryzyka
    def assign_risk(cluster_size: int) -> str:
        if cluster_size >= 3: return 'Wysokie'
        if cluster_size == 2: return 'Średnie'
        return 'Niskie'

    df['wirtualne_biuro_ryzyko'] = df['najwiekszy_klaster_adresowy'].apply(assign_risk)
    
    # 6. Sprzątanie - kolumny ID zostają w pliku wynikowym!
    df.drop(columns=['adresy_lista', 'znormalizowane_adresy'], inplace=True)
    
    return df


def analyze_shareholder_clusters(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Wykrywa klastry powiązań kapitałowych na podstawie obecnych 
    i historycznych udziałowców, dodając ID klastra (odporne na ucięte nawiasy).
    """
    # 1. Przygotowanie danych o udziałowcach
    aktualni_udzialowcy = df['udzialowcy'].fillna('')
    historyczni_udzialowcy = df['historyczni_udzialowcy'].fillna('')
    
    # Łączymy, dodając separator tylko gdy oba pola coś zawierają
    wszyscy_udzialowcy_raw = aktualni_udzialowcy + " | " + historyczni_udzialowcy
    df['udzialowcy_lista'] = wszyscy_udzialowcy_raw.str.split(" | ")
    
    # 2. Normalizacja (super-bezpieczne odcinanie od pierwszego nawiasu)
    def clean_shareholders(sh_list: list) -> list:
        cleaned_set = set()
        for sh in sh_list:
            sh_str = str(sh).strip()
            if sh_str and sh_str != "|":
                # Używamy split('[') i bierzemy tylko pierwszy element [0] (to co przed nawiasem)
                # Dzięki temu nawet jeśli brakuje zamykającego ']', reszta tekstu znika.
                sh_clean = sh_str.split('[')[0].strip()
                if sh_clean:
                    cleaned_set.add(sh_clean)
        return list(cleaned_set)
        
    df['znormalizowani_udzialowcy'] = df['udzialowcy_lista'].apply(clean_shareholders)
    
    # 3. Globalne zliczenie wystąpień dla każdego "czystego" udziałowca
    exploded_sh = df['znormalizowani_udzialowcy'].explode().dropna()
    sh_counts = exploded_sh.value_counts()
    
    # 4. Wyciągnięcie kluczowych metryk (rozmiar i ID klastra)
    def get_max_sh_cluster_size(sh_list: list) -> int:
        if not sh_list: return 0
        return max([sh_counts.get(s, 0) for s in sh_list])
        
    def get_main_sh_cluster_id(sh_list: list) -> str:
        if not sh_list: return ""
        # Jako ID klastra zwracamy tego udziałowca z historii firmy, który jest najbardziej "seryjny"
        return max(sh_list, key=lambda sh: sh_counts.get(sh, 0))
    
    df['max_powiazania_udzialowca'] = df['znormalizowani_udzialowcy'].apply(get_max_sh_cluster_size)
    df['klaster_udzialowca_id'] = df['znormalizowani_udzialowcy'].apply(get_main_sh_cluster_id)

    # 5. Przypisanie flagi ryzyka
    def assign_sh_risk(cluster_size: int) -> str:
        if cluster_size >= 3: return 'Wysokie'
        if cluster_size == 2: return 'Średnie'
        return 'Niskie'

    df['ryzyko_powiazan_kapitalowych'] = df['max_powiazania_udzialowca'].apply(assign_sh_risk)
    
    # 6. Sprzątanie
    df.drop(columns=['udzialowcy_lista', 'znormalizowani_udzialowcy'], inplace=True)
    
    return df

def run_advanced_pipeline() -> None:
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
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
    
    df_filtered_addresses = df[df['najwiekszy_klaster_adresowy'] > 1]
    top_addresses = df_filtered_addresses['krs_adres_aktualny'].unique()
    print(f"Znaleziono {len(top_addresses)} klastrów adresowych.")

    print("Analiza powiązań udziałowców (kapitałowych)...")
    df = analyze_shareholder_clusters(
        df=df
    )

    print("Uruchamianie wyszukiwania i analizy LLM (to może potrwać)...")
    analyzer = WebAnalyzer()
    
    df['website_url'] = ""
    df['ai_summary'] = ""

    # Używamy head(20) w celu puszczenia testu na 20 pierwszych podmiotach
    for index, row in tqdm(iterable=df.head(20).iterrows(), total=20, desc="Analiza AI"):
        company_name = str(row['Imię i Nazwisko / Nazwa firmy'])
        company_address = str(row['krs_adres_aktualny'])
        # ==========================================
        # FILTROWANIE PODMIOTÓW AKTYWNYCH
        # ==========================================
        zawieszenie_ias = str(row.get('Informacja o zawieszeniu działalności', '')).strip().lower()
        zakonczenie_ias = str(row.get('Informacja o zakończeniu działalności', '')).strip().lower()
        krs_status = str(row.get('krs_status', '')).strip()
        likwidacja = str(row.get('likwidacja', '')).strip()

        # Adnotacje "---" lub puste oznaczają brak zawieszenia/wykreślenia w rejestrze IAS
        is_active_ias = (zawieszenie_ias in ['---', '', 'nan']) and (zakonczenie_ias in ['---', '', 'nan'])
        # Podmiot aktywny w KRS lub w ogóle go tam nie ma (np. zagraniczny / CEIDG)
        is_active_krs = (krs_status == 'Aktywny') or (krs_status == 'Brak KRS')
        not_liquidated = (likwidacja != 'Tak')

        if not (is_active_ias and is_active_krs and not_liquidated):
            df.at[index, 'ai_summary'] = "Pominięto (podmiot nieaktywny, wykreślony lub zawieszony)."
            continue
        # ==========================================

        main_shareholder = str(row.get('klaster_udzialowca_id', '')) 
        
        # Szukamy do 3 przefiltrowanych linków
        urls = analyzer.find_websites(
            company_name=company_name,
            shareholder_name=main_shareholder,
            company_address=company_address
        )
        
        # Zapisujemy znalezione linki oddzielone znakiem |
        df.at[index, 'website_url'] = " | ".join(urls) if urls else ""
        
        if urls:
            combined_text = ""
            for url in urls:
                text = analyzer.scrape_website_text(
                    url=url
                )
                # Odrzucamy błędy HTTP i dodajemy tekst z tej konkretnej strony
                if not text.startswith("Błąd") and not text.startswith("HTTP"):
                    combined_text += f"\n--- {url} ---\n{text}"
            
            # Jeśli udało się zebrać jakikolwiek poprawny tekst
            if len(combined_text) > 50:
                summary = analyzer.synthesize_with_llm(
                    company_name=company_name, 
                    company_address=company_address,
                    # Przekazujemy scalony tekst obcięty do 8000 znaków dla bezpieczeństwa
                    website_text=combined_text[:8000] 
                )
                df.at[index, 'ai_summary'] = summary
            else:
                df.at[index, 'ai_summary'] = "Nie udało się pobrać treści z żadnej ze znalezionych stron."
        else:
            df.at[index, 'ai_summary'] = "Nie znaleziono odpowiednich stron (po odrzuceniu śmieciowych agregatorów KRS)."
        
        time.sleep(4)

    print("\nZapisywanie osint_crypto_register.csv...")
    df.to_csv(
        path_or_buf=output_path, 
        index=False, 
        encoding='utf-8'
    )
    print("Zakończono pomyślnie test na 20 podmiotach!")

if __name__ == "__main__":
    run_advanced_pipeline()
