import os
import time
import pandas as pd
import requests
import re
import sys
from bs4 import BeautifulSoup
from ddgs import DDGS
from google import genai  # Nowe, wspierane SDK Google

# Inicjalizacja klienta Gemini
GEMINI_API_KEY = os.getenv(key="GEMINI_API_KEY")
gemini_client = None

is_ci = os.getenv('CI') == 'true'

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
        clean_company = " ".join(clean_company.split())
        
        # Budowa zapytania bazowego
        search_query = f'"{clean_company}" adres: {company_address} krypto'
        
        # 2. Czyszczenie nazwy udziałowca
        if shareholder_name and "PESEL" not in shareholder_name:
            clean_shareholder = shareholder_name.upper()
            for forma in formy_prawne + ["LTD", "LIMITED", "LLC"]:
                clean_shareholder = clean_shareholder.replace(forma, "")
            clean_shareholder = " ".join(clean_shareholder.split())
            
            if clean_shareholder:
                search_query += f' OR "{clean_shareholder}"'

        valid_links = []
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
                if any(ex in href for ex in excluded_domains):
                    continue
                if href:
                    valid_links.append(res.get('href', ''))
                if len(valid_links) == 3:
                    break
        except Exception as e:
            print(f"Błąd wyszukiwania dla {clean_company}: {e}")
            
        return valid_links

    def scrape_website_text(self, url: str) -> str:
        if not url: 
            return ""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            response = requests.get(url=url, headers=headers, timeout=5)
            if response.status_code == 200:
                soup = BeautifulSoup(markup=response.text, features='html.parser')
                for script in soup(name=["script", "style", "nav", "footer"]):
                    script.extract()
                text = soup.get_text(separator=' ', strip=True)
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
        Jesteś ekspertem OSINT i analitykiem AML/KYC specjalizującym się w sektorze kryptowalut (VASP).
        Twoim celem jest analiza treści stron internetowych w celu określenia realnego profilu działalności firmy.

        PODMIOT DO ANALIZY:
        Nazwa: {company_name}
        Adres: {company_address}

        TREŚĆ ZE STRON:
        {website_text}

        ZADANIE:
        Na podstawie wyłącznie dostarczonego tekstu, odpowiedz na poniższe pytania. Jeśli informacji nie ma w tekście, napisz "Brak danych".

        1. Profil działalności: (Czym firma zajmuje się w praktyce? Np. giełda, portfel, doradztwo, mining).
        2. Model klienta: (B2C, B2B czy oba? Podaj krótki dowód z tekstu).
        3. Rynek docelowy: (Polska, Globalnie czy konkretne kraje? Na podstawie języka i treści).

        ZASADY:
        - Bądź maksymalnie zwięzły (łącznie max 4 zdania).
        - Nie używaj zwrotów typu "Na podstawie dostarczonego tekstu..." lub "Wydaje się, że...".
        - Jeśli tekst zawiera tylko dane rejestrowe (NIP, KRS, adres), odpowiedz: "Brak wyraźnych informacji o profilu usług."
        """     
        
        models_to_try = ['gemma-4-26b-a4b-it', 'gemma-4-31b-it', 'gemini-3.1-flash-lite']
        last_error_msg = "Unknown error"
    
        for i, model_name in enumerate(models_to_try):
            try:
                response = gemini_client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config={"temperature": 0.1}
                )   
                # Czyszczenie odpowiedzi do jednej linii dla CSV/Excel
                clean_text = response.text.strip()
                clean_text = clean_text.replace('\n', ' ').replace('\r', ' ')
                clean_text = " ".join(clean_text.split())
                return clean_text
                
            except Exception as e:
                err_str = str(e)
                last_error_msg = err_str
                if i < len(models_to_try) - 1:
                    next_model = models_to_try[i+1]
                    print(f"⚠️ Model {model_name} nie powiódł się. Przełączam na {next_model} dla {company_name[:20]}...")
                if "500" in err_str or "503" in err_str or "INTERNAL" in err_str:
                    time.sleep(3)
                continue
            
        return f"Final Error: {last_error_msg}"


def analyze_address_clusters(df: pd.DataFrame) -> pd.DataFrame:
    aktualny_adres = df['krs_adres_aktualny'].fillna('')
    historyczne_adresy = df['krs_adresy_historyczne'].fillna('').replace("Brak zmian adresu", "")
    all_addresses_raw = aktualny_adres + " -> " + historyczne_adresy
    df['adresy_lista'] = all_addresses_raw.str.split(" -> ")
    
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
    exploded_addresses = df['znormalizowane_adresy'].explode().dropna()
    address_counts = exploded_addresses.value_counts()
    
    def get_max_cluster_size(addr_list: list) -> int:
        if not addr_list: return 0
        return max([address_counts.get(a, 0) for a in addr_list])
        
    def get_main_address_cluster_id(addr_list: list) -> str:
        if not addr_list: return ""
        return max(addr_list, key=lambda addr: address_counts.get(addr, 0))

    df['najwiekszy_klaster_adresowy'] = df['znormalizowane_adresy'].apply(get_max_cluster_size)
    df['klaster_adresowy_id'] = df['znormalizowane_adresy'].apply(get_main_address_cluster_id)

    def assign_risk(cluster_size: int) -> str:
        if cluster_size >= 3: return 'Wysokie'
        if cluster_size == 2: return 'Średnie'
        return 'Niskie'

    df['wirtualne_biuro_ryzyko'] = df['najwiekszy_klaster_adresowy'].apply(assign_risk)
    df.drop(columns=['adresy_lista', 'znormalizowane_adresy'], inplace=True)
    return df



def analyze_shareholder_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wykrywa klastry powiązań kapitałowych na podstawie:
    1. Całej historii (aktualni i historyczni udziałowcy).
    2. Tylko aktualnych udziałowców.
    """
    
    # =========================================================================
    # CZĘŚĆ 1: KLASTROWANIE NA PODSTAWIE CAŁEJ HISTORII (Tak jak do tej pory)
    # =========================================================================
    aktualni_udzialowcy = df['udzialowcy'].fillna('')
    historyczni_udzialowcy = df['historyczni_udzialowcy'].fillna('')
    wszyscy_udzialowcy_raw = aktualni_udzialowcy + " | " + historyczni_udzialowcy
    
    df['udzialowcy_lista_historia'] = wszyscy_udzialowcy_raw.str.split(" | ", regex=False)
    
    def clean_shareholders(sh_list: list) -> list:
        cleaned_set = set()
        for sh in sh_list:
            sh_str = str(sh).strip()
            if sh_str and sh_str != "|":
                sub_shareholders = re.split(r'\s\|\s|,\s', sh_str)
                for sub_sh in sub_shareholders:
                    sub_sh = sub_sh.strip()
                    if not sub_sh: continue
                    sh_clean = sub_sh.split('[')[0].strip()
                    if "PESEL" not in sh_clean.upper():
                        formy_prawne = ["SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ", "SP. Z O.O.", "SP. Z O. O.", "SPÓŁKA AKCYJNA", "S.A.", "PROSTA SPÓŁKA AKCYJNA", "P.S.A.", "SPÓŁKA KOMANDYTOWA", "SP. K.", "LTD", "LIMITED", "LLC"]
                        for forma in formy_prawne:
                            pattern = r'\b' + re.escape(forma) + r'\b'
                            sh_clean = re.sub(pattern, "", sh_clean, flags=re.IGNORECASE)
                    sh_clean = " ".join(sh_clean.split()).strip()
                    if len(sh_clean) > 3:
                        cleaned_set.add(sh_clean)
        return list(cleaned_set)
        
    df['znormalizowani_udzialowcy_historia'] = df['udzialowcy_lista_historia'].apply(clean_shareholders)
    exploded_sh_historia = df['znormalizowani_udzialowcy_historia'].explode().dropna()
    sh_counts_historia = exploded_sh_historia.value_counts()
    
    df['max_powiazania_udzialowca_historia'] = df['znormalizowani_udzialowcy_historia'].apply(lambda l: max([sh_counts_historia.get(s, 0) for s in l]) if l else 0)
    df['klaster_udzialowca_id_historia'] = df['znormalizowani_udzialowcy_historia'].apply(lambda l: max(l, key=lambda s: sh_counts_historia.get(s, 0)) if l else "")
    
    df['ryzyko_powiazan_kapitalowych_historia'] = df['max_powiazania_udzialowca_historia'].apply(lambda x: 'Wysokie' if x >= 3 else ('Średnie' if x == 2 else 'Niskie'))
    
    # =========================================================================
    # CZĘŚĆ 2: NOWA LOGIKA - KLASTROWANIE TYLKO NA OBECNYCH UDZIAŁOWCACH
    # =========================================================================
    df['udzialowcy_lista_aktualni'] = df['udzialowcy'].fillna('').str.split(" | ", regex=False)
    
    # Używamy tej samej funkcji czyszczącej, co wcześniej
    df['znormalizowani_udzialowcy_aktualni'] = df['udzialowcy_lista_aktualni'].apply(clean_shareholders)
    exploded_sh_aktualni = df['znormalizowani_udzialowcy_aktualni'].explode().dropna()
    sh_counts_aktualni = exploded_sh_aktualni.value_counts()
    
    # Tworzymy nowe kolumny dla klastrów aktualnych
    df['max_powiazania_udzialowca_aktualni'] = df['znormalizowani_udzialowcy_aktualni'].apply(lambda l: max([sh_counts_aktualni.get(s, 0) for s in l]) if l else 0)
    df['klaster_udzialowca_id_aktualni'] = df['znormalizowani_udzialowcy_aktualni'].apply(lambda l: max(l, key=lambda s: sh_counts_aktualni.get(s, 0)) if l else "")
    
    # =========================================================================
    # SPRZĄTANIE (Usuwamy wszystkie kolumny tymczasowe)
    # =========================================================================
    df.drop(
        columns=[
            'udzialowcy_lista_historia', 
            'znormalizowani_udzialowcy_historia',
            'udzialowcy_lista_aktualni',
            'znormalizowani_udzialowcy_aktualni'
        ], 
        inplace=True
    )
    
    return df


def analyze_board_member_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wykrywa klastry powiązań osobowych na podstawie obecnych 
    członków zarządu/osób decyzyjnych.
    """
    
    # KROK 1: Przygotowanie danych wejściowych
    # Pracujemy tylko na kolumnie 'osoby_decyzyjne' (aktualni)
    aktualni_czlonkowie = df['osoby_decyzyjne'].fillna('')
    
    # KROK 2: Podział na listę osób
    df['zarzad_lista'] = aktualni_czlonkowie.str.split(" | ", regex=False)

    # KROK 3: Funkcja czyszcząca
    def clean_board_members(bm_list: list) -> list:
        cleaned_set = set()
        for bm in bm_list:
            bm_str = str(bm).strip()
            if not bm_str or bm_str == "|":
                continue

            # Rozdzielamy na wypadek wielu osób w jednym stringu (bezpiecznik)
            sub_members = re.split(r'\s\|\s|,\s', bm_str)
            for sub_bm in sub_members:
                sub_bm = sub_bm.strip()
                if not sub_bm: continue

                # Odcinamy metadane (wszystko od pierwszej daty w nawiasie '[')
                bm_clean = sub_bm.split('[')[0].strip()
                
                # Usuwamy funkcję w zarządzie, np. "(PREZES ZARZĄDU)"
                # To pozwala na połączenie tej samej osoby, nawet jeśli zmieniła funkcję
                bm_clean = re.sub(r'\(.*?\)', '', bm_clean).strip()
                
                # Finalne czyszczenie podwójnych spacji
                bm_clean = " ".join(bm_clean.split()).strip()

                if len(bm_clean) > 3:
                    cleaned_set.add(bm_clean)
        return list(cleaned_set)
        
    df['znormalizowani_czlonkowie_zarzadu'] = df['zarzad_lista'].apply(clean_board_members)
    
    # KROK 4: Globalne zliczenie wystąpień
    exploded_bm = df['znormalizowani_czlonkowie_zarzadu'].explode().dropna()
    bm_counts = exploded_bm.value_counts()
    
    # KROK 5: Wyciągnięcie metryk i ID klastra
    df['max_powiazania_zarzadu'] = df['znormalizowani_czlonkowie_zarzadu'].apply(lambda l: max([bm_counts.get(s, 0) for s in l]) if l else 0)
    df['klaster_zarzadu_id'] = df['znormalizowani_czlonkowie_zarzadu'].apply(lambda l: max(l, key=lambda s: bm_counts.get(s, 0)) if l else "")
    
    # KROK 6: Sprzątanie
    df.drop(columns=['zarzad_lista', 'znormalizowani_czlonkowie_zarzadu'], inplace=True)
    
    return df

def run_advanced_pipeline() -> None:
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    input_path = os.path.join(base_dir, "data", "processed", "enriched_crypto_register.csv")
    output_path = os.path.join(base_dir, "data", "processed", "osint_crypto_register.csv")

    print("Wczytywanie bazy...")
    df = pd.read_csv(filepath_or_buffer=input_path)

    print("Analiza klastrów adresowych...")
    df = analyze_address_clusters(df=df)
    
    df_filtered_addresses = df[df['najwiekszy_klaster_adresowy'] > 1]
    print(f"Znaleziono {len(df_filtered_addresses['krs_adres_aktualny'].unique())} klastrów adresowych.")

    print("Analiza powiązań udziałowców (kapitałowych)...")
    df = analyze_shareholder_clusters(df=df)
    
    print("Analiza powiązań osób decyzyjnych (zarząd)...")
    df = analyze_board_member_clusters(df=df)

    print("Uruchamianie wyszukiwania i analizy LLM (to może potrwać)...")
    analyzer = WebAnalyzer()
    
    df['website_url'] = ""
    df['ai_summary'] = ""

    test_sample = df    # df.head(10) testowo
    total = len(test_sample)

    for index, row in test_sample.iterrows():
        company_name = str(row['Imię i Nazwisko / Nazwa firmy'])
        company_address = str(row['krs_adres_aktualny'])
        
        # Logowanie postępu bez tqdm (czyste linie w GH Actions)
        print(f"[{index+1}/{total}] Analiza: {company_name[:50]}...", end=" ", flush=True)

        # FILTROWANIE PODMIOTÓW AKTYWNYCH
        zawieszenie_ias = str(row.get('Informacja o zawieszeniu działalności', '')).strip().lower()
        zakonczenie_ias = str(row.get('Informacja o zakończeniu działalności', '')).strip().lower()
        krs_status = str(row.get('krs_status', '')).strip()
        likwidacja = str(row.get('likwidacja', '')).strip()

        is_active_ias = (zawieszenie_ias in ['---', '', 'nan']) and (zakonczenie_ias in ['---', '', 'nan'])
        is_active_krs = (krs_status == 'Aktywny') or (krs_status == 'Brak KRS')
        not_liquidated = (likwidacja != 'Tak')

        if not (is_active_ias and is_active_krs and not_liquidated):
            df.at[index, 'ai_summary'] = "Pominięto (podmiot nieaktywny, wykreślony lub zawieszony)."
            print("Pominięto (nieaktywny).")
            continue

        main_shareholder = str(row.get('klaster_udzialowca_id', '')) 
        urls = analyzer.find_websites(
            company_name=company_name,
            shareholder_name=main_shareholder,
            company_address=company_address
        )
        
        df.at[index, 'website_url'] = " | ".join(urls) if urls else ""
        
        if urls:
            combined_text = ""
            for url in urls:
                text = analyzer.scrape_website_text(url=url)
                if not text.startswith("Błąd") and not text.startswith("HTTP"):
                    combined_text += f"\n--- {url} ---\n{text}"
            
            if len(combined_text) > 50:
                summary = analyzer.synthesize_with_llm(
                    company_name=company_name, 
                    company_address=company_address,
                    website_text=combined_text[:8000] 
                )
                df.at[index, 'ai_summary'] = summary
                print("Sukces.")
            else:
                df.at[index, 'ai_summary'] = "Nie udało się pobrać treści z żadnej ze znalezionych stron."
                print("Błąd pobierania treści.")
        else:
            df.at[index, 'ai_summary'] = "Nie znaleziono odpowiednich stron (po odrzuceniu śmieciowych agregatorów KRS)."
            print("Brak stron.")
        
        time.sleep(4)

    print("\nZapisywanie osint_crypto_register.csv...")
    df.to_csv(path_or_buf=output_path, index=False, encoding='utf-8')
    print(f"Zakończono pomyślnie przebieg na {total} podmiotach!")

if __name__ == "__main__":
    run_advanced_pipeline()
