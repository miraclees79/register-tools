import pdfplumber
import pandas as pd
import re

class IASPdfExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        
        # Zdefiniowany nagłówek na podstawie dostarczonego wzoru
        self.expected_headers =[
            "Numer w rejestrze",
            "Data wpisu",
            "Imię i Nazwisko / Nazwa firmy",
            "Numer KRS",
            "NIP",
            "Rodzaj świadczonych usług",
            "Informacja o zawieszeniu działalności",
            "Informacja o zakończeniu działalności"
        ]

    def extract_table(self) -> pd.DataFrame:
        all_rows =[]
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                # pdfplumber zwraca tabelę jako listę wierszy (gdzie każdy wiersz to lista komórek)
                table = page.extract_table()
                if table:
                    for row in table:
                        # Sprawdzamy, czy to wiersz nagłówkowy (powtarzający się na każdej stronie)
                        # row[0] to "Numer w rejestrze"
                        first_cell = str(row[0]).strip() if row[0] else ""
                        if "Numer w rejestrze" in first_cell:
                            continue # Pomijamy wiersz, bo to nagłówek
                        
                        cleaned_row =[]
                        for i, cell in enumerate(row):
                            if cell is None:
                                cleaned_row.append(None)
                            elif i == 5:
                                # Kolumna 5: "Rodzaj świadczonych usług"
                                # pdfplumber zwraca załamania wierszy z PDF jako "\n".
                                # Zamieniamy "\n" na spację, aby mieć ciągły tekst bez łamania wierszy 
                                # w pliku CSV, ale zachowujemy wszystkie inne spacje.
                                cell_text = str(cell).replace('\n', ' ')
                                # Usuwamy podwójne spacje, które mogły powstać na styku
                                cell_text = re.sub(r'\s+', ' ', cell_text).strip()
                                cleaned_row.append(cell_text)
                            else:
                                # Pozostałe kolumny - standardowe czyszczenie
                                cleaned_row.append(str(cell).replace('\n', ' ').strip())
                                
                        # Dodajemy tylko wiersze, które nie są całkowicie puste
                        if any(cleaned_row):
                            all_rows.append(cleaned_row)
        
        if not all_rows:
            raise ValueError("Nie znaleziono tabeli w pliku PDF.")

        # Tworzenie DataFrame
        df = pd.DataFrame(all_rows)
        
        # Ochrona przed sytuacją, gdyby pdfplumber skleił lub rozdzielił kolumny inaczej
        if len(df.columns) == len(self.expected_headers):
            df.columns = self.expected_headers
        else:
            print(f"OSTRZEŻENIE: Znaleziono {len(df.columns)} kolumn w PDF, a oczekiwano {len(self.expected_headers)}.")
            # Zabezpieczenie: przypisz tyle nagłówków ile się da
            assigned_headers = self.expected_headers[:len(df.columns)]
            # Jeśli kolumn jest więcej, nazwij je Col_X
            if len(df.columns) > len(self.expected_headers):
                assigned_headers.extend([f"Dodatkowa_{i}" for i in range(len(df.columns) - len(self.expected_headers))])
            df.columns = assigned_headers

        return df

    def clean_krs_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Wyszukuje i czyści numery KRS z kolumny "Numer KRS".
        Pozwala to odfiltrować np. osoby fizyczne z pustym KRS-em.
        """
        krs_col_name = 'Numer KRS'
        
        if krs_col_name not in df.columns:
            # Zabezpieczenie w razie nieprzewidzianej zmiany układu tabeli
            df['clean_krs'] = None
            return df

        def extract_krs(text):
            if pd.isna(text) or not text: 
                return None
            # Szukamy dokładnie 10 cyfr (nawet jeśli ktoś wpisał spacje w środku KRS-u w PDF)
            match = re.search(r'\b\d{10}\b', str(text).replace(' ', ''))
            return match.group(0) if match else None

        df['clean_krs'] = df[krs_col_name].apply(extract_krs)
        return df