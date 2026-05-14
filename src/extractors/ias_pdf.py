import pdfplumber
import pandas as pd
import re

class IASPdfExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        
        self.expected_headers = [
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
        all_rows = []
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table:
                    continue
                    
                for row in table:
                    # 1. Wstępne czyszczenie komórek, by pozbyć się "None"
                    cleaned_row = []
                    for i, cell in enumerate(row):
                        if cell is None:
                            cleaned_row.append("")
                        elif i == 5:
                            # Kolumna usług: usuwamy entery, zostawiamy pojedyncze spacje
                            cell_text = str(cell).replace('\n', ' ')
                            cell_text = re.sub(r'\s+', ' ', cell_text).strip()
                            cleaned_row.append(cell_text)
                        else:
                            cleaned_row.append(str(cell).replace('\n', ' ').strip())
                            
                    # Pomijamy wiersze, które po czyszczeniu są całkowicie puste
                    if not any(cleaned_row):
                        continue
                        
                    # 2. Agresywne usuwanie nagłówków (odporne na dodatkowe spacje i białe znaki w PDF)
                    # Usuwamy wszystkie spacje i porównujemy wielkimi literami
                    first_cell_norm = re.sub(r'\s+', '', cleaned_row[0].upper())
                    second_cell_norm = re.sub(r'\s+', '', cleaned_row[1].upper())
                    
                    if "NUMERWREJESTRZE" in first_cell_norm or "DATAWPISU" in second_cell_norm:
                        continue
                        
                    # 3. Detekcja wierszy rozbitych na dwie strony (Continuation Row)
                    # Jeśli pierwsze 5 kolumn jest pustych (np. nie ma NIP, KRS, Nazwy), 
                    # a w innych kolumnach jest tekst, to na pewno dokończenie wiersza wyżej.
                    is_continuation = all(cell == "" for cell in cleaned_row[:5])
                    
                    if is_continuation and all_rows:
                        # Doklejamy tekst do poprzedniego wiersza (do odpowiednich kolumn)
                        for idx in range(5, len(cleaned_row)):
                            if cleaned_row[idx]:
                                # Łączymy dotychczasowy tekst z nowym, przedzielając spacją
                                all_rows[-1][idx] = (all_rows[-1][idx] + " " + cleaned_row[idx]).strip()
                    else:
                        # To jest standardowy, nowy wiersz
                        all_rows.append(cleaned_row)
                        
        if not all_rows:
            raise ValueError("Nie znaleziono tabeli w pliku PDF.")

        # 4. Budowa gotowej ramki danych
        df = pd.DataFrame(all_rows)
        
        # Ochrona w przypadku zmiany struktury tabeli
        if len(df.columns) == len(self.expected_headers):
            df.columns = self.expected_headers
        else:
            assigned_headers = self.expected_headers[:len(df.columns)]
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
            df['clean_krs'] = None
            return df

        def extract_krs(text):
            if pd.isna(text) or not text: 
                return None
            match = re.search(r'\b\d{10}\b', str(text).replace(' ', ''))
            return match.group(0) if match else None

        df['clean_krs'] = df[krs_col_name].apply(extract_krs)
        return df
